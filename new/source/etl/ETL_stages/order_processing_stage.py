import logging
import queue
import sqlite3
import threading
import math
from pathlib import Path
from typing import Dict, List, Optional, Union, Tuple, Any # Added Tuple, Any
from concurrent.futures import as_completed, Future

from new.source.utils.dataframe import DataFrame
from new.source.framework.using_threads import ThreadWrapper
from new.source.framework.hybrid_pool import HybridPool

# Relative imports
try:
    from ..etl_types import (
        ExtractedDataQueueItem, # Input: (task_id, df, payload) - This is what Dispatcher sends
        ProcessedDataQueueItem, # Output: (task_id, {data_dict}, payload)
        ExtractionTaskPayload
    )
    from ..config import DEFAULT_CHUNK_SIZE_ROWS_ORDERS
    from ..orders_ingest import (
        transform_new_orders_worker,
        get_db_connection,
        get_last_pk
    )
    ORDERS_LOGIC_AVAILABLE = True
except ImportError as e:
    logging.error(f"OrderProcessingStage: Failed to import components: {e}. Order functionality unavailable.")
    ORDERS_LOGIC_AVAILABLE = False
    # Define placeholders
    def transform_new_orders_worker(*_args, **_kwargs): raise NotImplementedError("orders_ingest.py not found")
    def get_db_connection(*_args, **_kwargs): raise NotImplementedError("orders_ingest.py not found")
    def get_last_pk(*_args, **_kwargs): raise NotImplementedError("orders_ingest.py not found")

logger = logging.getLogger(__name__)

# Sentinel type
Sentinel = type(None)

# Define Input/Output queue item types more precisely for clarity
# Dispatcher sends (task_id, (df, payload))
InputQueueItem = Tuple[str, Tuple[DataFrame, ExtractionTaskPayload]]
# We output (task_id, (data_dict, payload))
OutputQueueItem = Tuple[str, Tuple[Dict[str, DataFrame], ExtractionTaskPayload]]


# The 'value' type ThreadWrapper handles is Tuple[DataFrame, ExtractionTaskPayload]
class OrderProcessingStage(ThreadWrapper[str, Tuple[DataFrame, ExtractionTaskPayload]]):
    """
    Processes raw order DataFrames, transforms them into 'pedidos' and 'itens',
    handles ID generation and chunking, and supports sentinel shutdown.
    """
    def __init__(
        self,
        in_queue: queue.Queue[Union[InputQueueItem, None]], # Updated type
        out_queue: queue.Queue[Union[OutputQueueItem,None]], # Updated type
        hybrid_pool: HybridPool,
        ecommerce_db_path: Path,
        name: str = "OrderProcessingStage",
    ):
        super().__init__(in_queue, out_queue, name=name)
        self.hybrid_pool = hybrid_pool
        self.ecommerce_db_path = ecommerce_db_path
        self.default_chunk_size_rows = DEFAULT_CHUNK_SIZE_ROWS_ORDERS
        self._id_lock = threading.Lock()
        self._ecommerce_conn_for_ids: Optional[sqlite3.Connection] = None
        self._running = True # Ensure _running is set

        if not ORDERS_LOGIC_AVAILABLE:
            logger.error(f"{self.name} instantiated, but order processing logic is unavailable.")

    def _get_reusable_ecommerce_connection(self) -> sqlite3.Connection:
        """Gets or creates a reusable SQLite connection for ID lookups."""
        if self._ecommerce_conn_for_ids is None or self._is_connection_closed(self._ecommerce_conn_for_ids):
            if not ORDERS_LOGIC_AVAILABLE:
                raise ConnectionError("Cannot get DB connection: Order logic not available.")
            try:
                self._ecommerce_conn_for_ids = get_db_connection(self.ecommerce_db_path)
            except Exception as e:
                logger.error(f"{self.name}: Failed to get e-commerce DB connection for IDs: {e}")
                raise
        return self._ecommerce_conn_for_ids

    def _is_connection_closed(self, conn: sqlite3.Connection) -> bool:
        """Checks if a SQLite connection is closed or unusable."""
        try:
            conn.execute("SELECT 1").fetchone()
            return False
        except (sqlite3.ProgrammingError, sqlite3.OperationalError):
            return True
        except Exception:
            return True

    def handle_item(self, task_id: str, item_value: Tuple[DataFrame, ExtractionTaskPayload]) -> Optional[ProcessedDataQueueItem]:
        """
        Processes a single order extraction item.
        `item_value` is `(df_raw_orders, task_payload)`.
        Returns `(task_id, data_dict, payload)`.
        """
        df_raw_orders, task_payload = item_value # Unpack the value part

        # Always create a default empty result in case of early exit
        empty_result = {'pedidos': DataFrame(), 'itens': DataFrame()}

        if not ORDERS_LOGIC_AVAILABLE:
            logger.error(f"{self.name}: Order logic unavailable, skipping '{task_id}'.")
            return task_id, {'pedidos': DataFrame(columns=["error"]), 'itens': DataFrame()}, task_payload

        # Ensure this stage only processes its designated tasks
        if task_payload.get("processing_type") != "ecommerce_new_orders":
            logger.warning(f"{self.name}: Received task '{task_id}' not for order processing. Skipping.")
            return None # Return None so nothing is put on the out_queue

        if df_raw_orders.empty:
            logger.info(f"{self.name}: Raw orders DF for '{task_id}' is empty.")
            return task_id, empty_result, task_payload

        logger.info(f"{self.name}: Processing {df_raw_orders.shape[0]} raw orders for '{task_id}'.")

        try:
            # --- ID Generation ---
            with self._id_lock:
                conn_for_ids = self._get_reusable_ecommerce_connection()
                start_pedido_id = get_last_pk(conn_for_ids, "pedidos") + 1
                start_item_id = get_last_pk(conn_for_ids, "itens_pedido") + 1
            logger.info(f"{self.name}: IDs for '{task_id}': Pedido={start_pedido_id}, Item={start_item_id}")

            # --- Chunking and Processing ---
            num_chunks = math.ceil(df_raw_orders.shape[0] / self.default_chunk_size_rows)
            futures: List[Future] = []
            pedido_offset = 0
            item_offset = 0

            for i in range(num_chunks):
                # Ensure DataFrame has .slice() method
                chunk_df = df_raw_orders.slice(i * self.default_chunk_size_rows, (i + 1) * self.default_chunk_size_rows)
                if chunk_df.empty:
                    continue

                chunk_pedido_id = start_pedido_id + pedido_offset
                chunk_item_id = start_item_id + item_offset

                future = self.hybrid_pool.submit(
                    transform_new_orders_worker,
                    chunk_df, self.ecommerce_db_path,
                    chunk_pedido_id, chunk_item_id
                )
                futures.append(future)
                # This assumes transform_new_orders_worker *knows* how many IDs it will consume
                # A safer approach might be to return consumed counts or process serially for IDs.
                # For now, we stick to the 1-to-1 assumption for offset.
                pedido_offset += chunk_df.shape[0]
                item_offset += chunk_df.shape[0]

            # --- Collecting Results ---
            all_pedidos_dfs: List[DataFrame] = []
            all_itens_dfs: List[DataFrame] = []
            for future in as_completed(futures):
                try:
                    df_p, df_i = future.result() # Expects (df_pedidos, df_itens)
                    if df_p is not None and not df_p.empty: all_pedidos_dfs.append(df_p)
                    if df_i is not None and not df_i.empty: all_itens_dfs.append(df_i)
                except Exception as e:
                    logger.error(f"{self.name}: Error in worker for '{task_id}': {e}", exc_info=True)

            final_pedidos_df = DataFrame.concat_dfs(all_pedidos_dfs) if all_pedidos_dfs else DataFrame()
            final_itens_df = DataFrame.concat_dfs(all_itens_dfs) if all_itens_dfs else DataFrame()

            logger.info(f"{self.name}: Processed '{task_id}'. Pedidos: {final_pedidos_df.shape}, Itens: {final_itens_df.shape}")
            return task_id, {'pedidos': final_pedidos_df, 'itens': final_itens_df}, task_payload

        except Exception as e:
            logger.error(f"{self.name}: Critical error processing orders for '{task_id}': {e}", exc_info=True)
            return task_id, {'pedidos': DataFrame(columns=["critical_error"]), 'itens': DataFrame()}, task_payload

    def run(self):
        """
        Overrides ThreadWrapper.run to implement sentinel handling.
        """
        logger.info(f"{self.name}: Starting...")
        while self._running:
            try:
                item = self.in_queue.get(timeout=0.5)

                if item is None: # Sentinel received
                    logger.info(f"{self.name}: Sentinel received. Stopping.")
                    self._running = False
                    # self.in_queue.task_done()
                    continue

                # Standard processing: item is (task_id, (df, payload))
                task_id, item_value = item
                processed_item = self.handle_item(task_id, item_value)

                if processed_item:
                    # ThreadWrapper expects (key, value)
                    # We want to send (task_id, (data_dict, payload)) to LoadStage
                    out_key, out_dict, out_payload = processed_item
                    self.out_queue.put((out_key, (out_dict, out_payload)))

                # self.in_queue.task_done()

            except queue.Empty:
                if not self._running:
                    break
                continue
            except Exception as e:
                logger.error(f"{self.name}: Error processing item: {e}", exc_info=True)
                # self.in_queue.task_done()

        # --- Signal Downstream ---
        logger.info(f"{self.name}: Stopped processing. Sending sentinel downstream.")
        self.out_queue.put(None)
        logger.info(f"{self.name}: Finished.")

    def __del__(self):
        """Ensure connection is closed when the object is destroyed."""
        if self._ecommerce_conn_for_ids:
            try:
                self._ecommerce_conn_for_ids.close()
                logger.info(f"{self.name}: E-commerce DB connection closed.")
            except Exception as e:
                 logger.error(f"{self.name}: Error closing e-commerce DB connection: {e}")