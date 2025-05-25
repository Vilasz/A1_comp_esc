import logging
import queue
import sqlite3
import threading
import time
from pathlib import Path
from typing import Union, Any, Dict, Optional, Tuple # Added Tuple

from new.source.utils.dataframe import DataFrame
from new.source.etl.ETL_utils import RepoData
from new.source.framework.using_threads import ThreadWrapper

# Relative imports
try:
    from ..etl_types import (
        DataFrameQueueItem, ProcessedDataQueueItem, 
        ExtractionTaskPayload, LoadStageInputItem
    )
    from ..config import PATH_CONFIG
except ImportError as e:
    logging.error(f"LoadStage: Failed to import ETL components: {e}")
    raise

logger = logging.getLogger(__name__)

# Sentinel type for queues
Sentinel = type(None)

class LoadStage(ThreadWrapper[str, Any]):
    """
    Loads DataFrames into the target database. Handles inputs from
    TransformStage/DispatcherStage (DataFrame) and OrderProcessingStage
    (Dict[str, DataFrame]). Implements sentinel handling for shutdown.
    """
    def __init__(
        self,
        in_queue: queue.Queue[Union[LoadStageInputItem, None]], # Accepts data or Sentinel
        db_path_or_conn: Union[str, Path, sqlite3.Connection],
        target_table_prefix: str = "",
        if_exists_mode: str = "append",
        num_inputs: int = 1, # <<< How many upstream stages feed this loader
        name: str = "LoaderStage",
    ):
        super().__init__(in_queue, out_queue=None, name=name)
        self.repo = RepoData()
        self.db_path_or_conn = db_path_or_conn
        self.target_table_prefix = target_table_prefix
        self.if_exists_mode = if_exists_mode
        self._times_lock = threading.Lock()
        self._times_file = Path(PATH_CONFIG.get("TIMES_FILE_PATH", "etl_times.txt"))
        
        # --- For Sentinel Handling ---
        if num_inputs <= 0:
            logger.warning(f"{self.name}: num_inputs set to {num_inputs}, defaulting to 1.")
            self.num_inputs = 1
        else:
            self.num_inputs = num_inputs
        self.sentinels_received = 0
        # --- Ensure ThreadWrapper uses _running ---
        self._running = True # Make sure this is initialized for the run loop

    def _log_time(self, table_name: str, operation_start_ns: int) -> None:
        """Logs the duration of a load operation."""
        duration_ns = time.time_ns() - operation_start_ns
        try:
            with self._times_lock, self._times_file.open("a", encoding="utf-8") as fh:
                fh.write(f"{self.name} {table_name} duration_ms:{duration_ns / 1_000_000:.2f}\n")
        except Exception as e:
            logger.error(f"{self.name}: Failed to write to times_file '{self._times_file}': {e}")

    def handle_item(self, task_id: str, item_value: Any) -> None:
        """
        Processes an item from the input queue and loads its DataFrame(s).
        `item_value` is the 'value' part of the (key, value) tuple from ThreadWrapper.
        """
        dfs_to_load: Dict[str, DataFrame] = {} # Holds {table_suffix: DataFrame}

        # Case 1: Input is (task_id, DataFrame)
        # The 'value' part is just the DataFrame.
        if isinstance(item_value, DataFrame):
            logger.debug(f"{self.name}: Received DataFrame for task '{task_id}'.")
            dfs_to_load[task_id] = item_value

        # Case 2: Input is ProcessedDataQueueItem -> (task_id, (data_dict, payload))
        # The 'value' part is the tuple (data_dict, payload).
        elif (isinstance(item_value, tuple) and len(item_value) == 2 and 
              isinstance(item_value[0], dict) and isinstance(item_value[1], dict)):
            
            data_dict: Dict[str, DataFrame] = item_value[0]
            # payload: ExtractionTaskPayload = item_value[1] # We don't use payload here
            logger.debug(f"{self.name}: Received Dict[DataFrame] for task '{task_id}'. Keys: {list(data_dict.keys())}")

            for key, df in data_dict.items():
                if isinstance(df, DataFrame):
                    table_suffix = f"{task_id}_{key}" # e.g., new_orders_pedidos
                    dfs_to_load[table_suffix] = df
                else:
                    logger.warning(f"{self.name}: Item in data_dict for task '{task_id}' is not a DataFrame (key: {key}). Skipping.")
        
        # Case 3 (Robustness): Input might be ExtractedDataQueueItem -> (task_id, (df, payload))
        # The 'value' part is the tuple (df, payload).
        elif (isinstance(item_value, tuple) and len(item_value) == 2 and 
              isinstance(item_value[0], DataFrame) and isinstance(item_value[1], dict)):
              
            df: DataFrame = item_value[0]
            # payload: ExtractionTaskPayload = item_value[1] # Not used here
            logger.warning(f"{self.name}: Received (DataFrame, Payload) directly for task '{task_id}'. Loading the DataFrame only. (This might indicate a routing issue).")
            dfs_to_load[task_id] = df

        # Error Case: Unknown format
        else:
            logger.error(f"{self.name}: Received unknown item format for task '{task_id}'. Type: {type(item_value)}. Skipping.")
            return

        # --- Perform the Loading ---
        if not dfs_to_load:
            logger.info(f"{self.name}: No DataFrames to load for task '{task_id}'.")
            return

        for table_suffix, df_to_load in dfs_to_load.items():
            if df_to_load.empty:
                logger.info(f"{self.name}: DataFrame for '{table_suffix}' (task '{task_id}') is empty. Skipping.")
                continue

            table_name = f"{self.target_table_prefix}{table_suffix}"
            logger.info(f"{self.name}: Loading {df_to_load.shape} data for '{table_suffix}' into '{table_name}'")
            
            start_ns = time.time_ns()
            try:
                self.repo.set_strategy_for_loading(
                    kind=RepoData.Kind.SQL,
                    db_conn_or_path=self.db_path_or_conn,
                    table_name=table_name,
                    if_exists=self.if_exists_mode
                )
                self.repo.load_data(df_to_load)
                logger.info(f"{self.name}: Successfully loaded '{table_suffix}' into '{table_name}'")
            except Exception as e:
                logger.error(f"{self.name}: Failed to load '{table_suffix}' into '{table_name}': {e}", exc_info=True)
            finally:
                self._log_time(table_name, start_ns)
        
        return None # No output

    def run(self):
        """
        Overrides ThreadWrapper.run to implement sentinel handling for graceful shutdown.
        """
        logger.info(f"{self.name}: Starting (expecting {self.num_inputs} sentinel(s))...")
        while self._running:
            try:
                # Use JoinableQueue (if available) or handle task_done manually.
                # Assuming `queue.Queue` for this example.
                item = self.in_queue.get(timeout=0.5)

                if item is None: # Sentinel received
                    self.sentinels_received += 1
                    logger.info(f"{self.name}: Sentinel {self.sentinels_received}/{self.num_inputs} received.")
                    # self.in_queue.task_done() # Call if using JoinableQueue
                    if self.sentinels_received >= self.num_inputs:
                        logger.info(f"{self.name}: All sentinels received. Draining queue & stopping.")
                        self._running = False # Signal to stop *after* draining
                    continue # Check for more items or exit if _running is false

                # Standard processing
                task_id, item_value = item
                self.handle_item(task_id, item_value)
                # self.in_queue.task_done() # Call if using JoinableQueue

            except queue.Empty:
                if not self._running:
                    # If we've been told to stop (all sentinels received) and queue is empty, break.
                    break
                # Otherwise, it's a normal timeout, just continue waiting.
                continue
            except Exception as e:
                logger.error(f"{self.name}: Error processing item: {e}", exc_info=True)
                # Decide how to handle errors - here we log and continue.
                # Ensure task_done is called if using JoinableQueue, even on error.
                # if isinstance(self.in_queue, queue.JoinableQueue):
                #     self.in_queue.task_done()

        logger.info(f"{self.name}: Stopped.")

    # You might need a stop() method if ThreadWrapper doesn't have one
    # or if you need to set _running = False externally (though sentinel is preferred)
    # def stop(self):
    #     logger.info(f"{self.name}: Stop signal received.")
    #     self._running = False