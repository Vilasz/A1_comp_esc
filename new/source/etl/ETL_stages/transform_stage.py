import logging
import queue
from typing import Any, Dict, List, Optional, Callable, Union, Tuple
from concurrent.futures import as_completed, Future

from new.source.utils.dataframe import DataFrame
from new.source.framework.using_threads import ThreadWrapper
from new.source.framework.hybrid_pool import HybridPool

# Relative imports
try:
    from ..etl_types import DataFrameQueueItem
    from ..etl_workers import transform_dataframe_chunk_worker
    from ..config import DEFAULT_CHUNK_SIZE_ROWS_TRANSFORM
except ImportError as e:
    logging.error(f"TransformStage: Failed to import ETL components: {e}")
    raise

logger = logging.getLogger(__name__)

# Sentinel type for queues
Sentinel = type(None)

class TransformStage(ThreadWrapper[str, DataFrame]):
    """
    Applies user-defined transformations to DataFrames, supports chunking
    for large DataFrames, and handles sentinel for shutdown.
    """
    def __init__(
        self,
        in_queue: queue.Queue[Union[DataFrameQueueItem, None]], # Updated type hint
        out_queue: queue.Queue[Union[DataFrameQueueItem, None]], # Updated type hint
        hybrid_pool: HybridPool,
        user_transform_function: Optional[Callable[[DataFrame, Any], DataFrame]] = None,
        user_transform_params: Optional[Dict[str, Any]] = None,
        name: str = "TransformerStage",
    ):
        super().__init__(in_queue, out_queue, name=name)
        self.hybrid_pool = hybrid_pool
        self.user_transform_function = user_transform_function
        self.user_transform_params = user_transform_params or {}
        self.default_chunk_size_rows = DEFAULT_CHUNK_SIZE_ROWS_TRANSFORM
        self._running = True # Ensure _running is set

    def handle_item(self, task_id: str, df_input: DataFrame) -> Optional[DataFrameQueueItem]:
        """
        Applies transformation logic. Assumes input is DataFrame, as
        guaranteed by the DispatcherStage.
        """
        if self.user_transform_function is None:
            logger.debug(f"{self.name}: No transform function. Passing '{task_id}' through.")
            return task_id, df_input

        if df_input.empty:
            logger.info(f"{self.name}: Input DF for '{task_id}' is empty. Passing through.")
            return task_id, df_input
            
        logger.info(f"{self.name}: Transforming '{task_id}' (shape {df_input.shape})")

        try:
            # Check if DataFrame is large enough to warrant chunking
            if df_input.shape[0] <= self.default_chunk_size_rows * 1.5:
                transformed_df = transform_dataframe_chunk_worker(
                    df_input, self.user_transform_function, self.user_transform_params
                )
                if transformed_df is None:
                    logger.error(f"{self.name}: Transformation returned None for task '{task_id}'")
                    return None # Propagate failure (or return original?)
                logger.info(f"{self.name}: Transformed '{task_id}', new shape {transformed_df.shape}")
                return task_id, transformed_df
            else:
                # --- Chunking Logic ---
                logger.info(f"{self.name}: Chunking '{task_id}' for parallel transform.")
                df_chunks: List[DataFrame] = []
                # Ensure DataFrame has a .slice(start, end) method or implement chunking
                try:
                    for i in range(0, df_input.shape[0], self.default_chunk_size_rows):
                        chunk_df = df_input.slice(i, min(i + self.default_chunk_size_rows, df_input.shape[0]))
                        if not chunk_df.empty:
                            df_chunks.append(chunk_df)
                except AttributeError:
                     logger.error(f"{self.name}: DataFrame object missing '.slice()' method. Cannot chunk. Processing as whole.")
                     # Fallback to whole processing if slice isn't available
                     transformed_df = transform_dataframe_chunk_worker(df_input, self.user_transform_function, self.user_transform_params)
                     return task_id, transformed_df if transformed_df else df_input

                if not df_chunks:
                     logger.warning(f"{self.name}: Chunking resulted in no data for '{task_id}'. Processing as whole.")
                     transformed_df = transform_dataframe_chunk_worker(df_input, self.user_transform_function, self.user_transform_params)
                     return task_id, transformed_df if transformed_df else df_input

                futures: List[Future[Optional[DataFrame]]] = [
                    self.hybrid_pool.submit(
                        transform_dataframe_chunk_worker, df_chunk,
                        self.user_transform_function, self.user_transform_params
                    ) for df_chunk in df_chunks
                ]

                transformed_dfs: List[DataFrame] = [fut.result() for fut in as_completed(futures) if fut.result() is not None]

                if not transformed_dfs:
                    logger.error(f"{self.name}: All transform chunks failed for '{task_id}'")
                    return None

                final_df = DataFrame.concat_dfs(transformed_dfs)
                logger.info(f"{self.name}: Reconstructed transformed DF for '{task_id}', shape {final_df.shape}")
                return task_id, final_df

        except Exception as e:
            logger.error(f"{self.name}: Error transforming '{task_id}': {e}", exc_info=True)
            return None # Or return original df: (task_id, df_input)

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
                    # self.in_queue.task_done() # If using JoinableQueue
                    continue # Exit loop on next check

                # Standard processing: item is (task_id, df)
                task_id, df_input = item
                processed_item = self.handle_item(task_id, df_input)

                if processed_item:
                    self.out_queue.put(processed_item)

                # self.in_queue.task_done() # If using JoinableQueue

            except queue.Empty:
                if not self._running:
                    break # Exit loop
                continue # Keep waiting
            except Exception as e:
                logger.error(f"{self.name}: Error processing item: {e}", exc_info=True)
                # self.in_queue.task_done() # If using JoinableQueue

        # --- Signal Downstream ---
        logger.info(f"{self.name}: Stopped processing. Sending sentinel downstream.")
        self.out_queue.put(None)
        logger.info(f"{self.name}: Finished.")