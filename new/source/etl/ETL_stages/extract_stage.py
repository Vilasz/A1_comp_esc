import logging
import queue
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple
from concurrent.futures import as_completed, Future

from new.source.utils.dataframe import DataFrame
from new.source.etl.ETL_utils import RepoData, MapMutex
from new.source.framework.using_threads import ThreadWrapper
from new.source.framework.hybrid_pool import HybridPool

# Relative imports
try:
    from ..etl_types import (
        ExtractionTaskItem, DataFrameQueueItem, ExtractionTaskPayload,
        ExtractedDataQueueItem # This is Tuple[str, DataFrame, ExtractionTaskPayload]
    )
    from ..etl_workers import parse_file_chunk_worker
    from ..config import (
        PATH_CONFIG, DEFAULT_CHUNK_SIZE_LINES_EXTRACT,
        DEFAULT_PG_EVENT_BATCH_SIZE, ETL_POSTGRES_DB_CONFIG
    )
except ImportError as e:
    logging.error(f"ExtractStage: Failed to import ETL components: {e}")
    raise

logger = logging.getLogger(__name__)

# Sentinel type for queues
Sentinel = type(None)

# Define the type for the output queue item - ThreadWrapper adds one layer.
# So, if handle_item returns (id, df, payload), queue items are (id, (df, payload))
OutputQueueItem = Tuple[str, Tuple[DataFrame, ExtractionTaskPayload]]

class ExtractStage(ThreadWrapper[str, ExtractionTaskPayload]):
    """
    Extracts data from various sources (files, DBs), handles chunking,
    and manages incremental loads like DataCat. Outputs data with its
    original payload for downstream dispatching.
    """
    def __init__(
        self,
        in_queue: queue.Queue[Union[ExtractionTaskItem, None]],
        # The out_queue will receive items like (task_id, (df, payload))
        out_queue: queue.Queue[Union[OutputQueueItem, None]],
        map_mutex: MapMutex,
        hybrid_pool: HybridPool,
        name: str = "ExtractorStage",
    ):
        super().__init__(in_queue, out_queue, name=name)
        self.map_mutex = map_mutex
        self.hybrid_pool = hybrid_pool
        self.repo = RepoData()
        self.pg_db_config = ETL_POSTGRES_DB_CONFIG # Keep for potential future use
        self.default_pg_event_batch_size = DEFAULT_PG_EVENT_BATCH_SIZE # Keep
        self.path_config = PATH_CONFIG
        self.default_chunk_size_lines = DEFAULT_CHUNK_SIZE_LINES_EXTRACT
        self._running = True # Ensure _running is set

    def _resolve_path_from_task(self, path_str: str) -> Path:
        """Resolves a relative path using PATH_CONFIG or CWD."""
        p = Path(path_str)
        if p.is_absolute(): return p
        for root_key, root_path in self.path_config.items():
            if isinstance(root_path, (str, Path)):
                root_path_obj = Path(root_path)
                potential_path = root_path_obj / path_str
                # Check if it exists as file or (for datacat) as dir
                if potential_path.exists():
                    return potential_path.resolve()
        logger.warning(f"Could not resolve path '{path_str}' against known roots. Trying as relative.")
        return Path(path_str).resolve()

    def _split_file_into_line_chunk_definitions(self, file_path: Path, strategy_params: Dict) -> List[Dict[str, Any]]:
        """Splits a text file into line-based chunk definitions."""
        chunk_defs = []
        header_line = None
        delimiter = strategy_params.get("delimiter", ",")
        has_header = strategy_params.get('csv_has_header', True) or strategy_params.get('txt_has_header', False)

        try:
            with file_path.open('r', encoding=strategy_params.get('encoding', 'utf-8')) as f:
                if has_header:
                    header_line = f.readline().strip()

                current_chunk_lines = []
                for line in f:
                    current_chunk_lines.append(line.strip())
                    if len(current_chunk_lines) >= self.default_chunk_size_lines:
                        chunk_defs.append({
                            'lines_to_parse': current_chunk_lines,
                            'header_for_chunk': header_line.split(delimiter) if header_line else None
                        })
                        current_chunk_lines = []
                if current_chunk_lines:
                    chunk_defs.append({
                        'lines_to_parse': current_chunk_lines,
                        'header_for_chunk': header_line.split(delimiter) if header_line else None
                    })
        except Exception as e:
            logger.error(f"Error splitting file {file_path} into chunks: {e}", exc_info=True)
        return chunk_defs

    def _handle_datacat(self, task_id: str, task_payload: ExtractionTaskPayload, resolved_path: Path) -> DataFrame:
        """Handles incremental DataCat file processing with reliable marker updates."""
        processed_files_key = f"datacat_last_file_{resolved_path.name}"
        dfs_from_new_files: List[DataFrame] = []
        futures_map: Dict[Future[Optional[DataFrame]], str] = {} # Map future to filename

        with self.map_mutex.key_lock(processed_files_key):
            last_processed_marker = str(self.map_mutex.get(processed_files_key, ""))
            logger.info(f"{self.name} (DataCat): Current marker for {resolved_path.name} is '{last_processed_marker}'.")

            try:
                sorted_entries = sorted([entry for entry in resolved_path.iterdir() if entry.is_file()])
            except FileNotFoundError:
                 logger.error(f"{self.name} (DataCat): Directory not found: {resolved_path}")
                 return DataFrame(columns=task_payload.get("expected_columns", []))

            files_to_submit = []
            for entry_path in sorted_entries:
                if entry_path.name > last_processed_marker:
                    files_to_submit.append(entry_path)

            if not files_to_submit:
                logger.info(f"{self.name} (DataCat): No new files found > '{last_processed_marker}'.")
                return DataFrame(columns=task_payload.get("expected_columns", []))

            logger.info(f"{self.name} (DataCat): Found {len(files_to_submit)} new files. Submitting...")

            datacat_file_repo_params = {
                "txt_delimiter": task_payload.get("txt_delimiter", "|"),
                "txt_has_header": task_payload.get("txt_has_header", False),
                "encoding": task_payload.get("encoding", "utf-8"),
                "delimiter": task_payload.get("txt_delimiter", "|"), # for worker
            }

            for entry_path in files_to_submit:
                future = self.hybrid_pool.submit(
                    parse_file_chunk_worker,
                    RepoData.Kind.TXT,
                    entry_path,
                    {"is_full_file": True},
                    datacat_file_repo_params
                )
                futures_map[future] = entry_path.name

            successfully_processed_names = []
            for fut in as_completed(futures_map):
                entry_name = futures_map[fut]
                try:
                    df_result = fut.result()
                    if df_result is not None:
                        dfs_from_new_files.append(df_result)
                        successfully_processed_names.append(entry_name)
                        logger.debug(f"{self.name} (DataCat): Successfully processed {entry_name}.")
                    else:
                        logger.warning(f"{self.name} (DataCat): Worker returned None for {entry_name}.")
                except Exception as e:
                    logger.error(f"{self.name} (DataCat): Error processing file {entry_name}: {e}", exc_info=True)

            # --- Reliable Marker Update ---
            if successfully_processed_names:
                new_marker = max(successfully_processed_names)
                if new_marker > last_processed_marker:
                    self.map_mutex.set(processed_files_key, new_marker)
                    logger.info(f"{self.name} (DataCat): Updated marker to '{new_marker}'.")
                else:
                     logger.info(f"{self.name} (DataCat): No successful files newer than marker '{last_processed_marker}'. Marker unchanged.")
            else:
                logger.warning(f"{self.name} (DataCat): No files processed successfully. Marker unchanged.")

        # --- Concatenate Results ---
        if not dfs_from_new_files:
            return DataFrame(columns=task_payload.get("expected_columns", []))
        else:
            final_df = DataFrame.concat_dfs(dfs_from_new_files)
            logger.info(f"{self.name} (DataCat): Combined {len(dfs_from_new_files)} files, shape {final_df.shape}.")
            return final_df


    def handle_item(self, task_id: str, task_payload: ExtractionTaskPayload) -> Optional[ExtractedDataQueueItem]:
        """
        Processes a single extraction task and returns a tuple
        (task_id, DataFrame, task_payload) for the DispatcherStage.
        """
        data_kind = task_payload.get("kind")
        file_path_str = task_payload.get("path")

        if not data_kind or not file_path_str:
            logger.error(f"{self.name}: Task '{task_id}' missing 'kind' or 'path'. Skipping.")
            return None

        resolved_path = self._resolve_path_from_task(file_path_str)

        # Handle path validation
        if data_kind == "datacat_loader":
            if not resolved_path.is_dir():
                logger.error(f"{self.name}: DataCat path '{resolved_path}' for task '{task_id}' is not a directory. Skipping.")
                return None
        elif not resolved_path.exists():
            logger.error(f"{self.name}: File not found for task '{task_id}': {resolved_path}. Skipping.")
            return None

        logger.info(f"{self.name}: Processing '{task_id}' for path: {resolved_path}")
        final_df: Optional[DataFrame] = None

        # --- DataCat Logic ---
        if data_kind == "datacat_loader":
            final_df = self._handle_datacat(task_id, task_payload, resolved_path)

        # --- General File/Other Logic ---
        else:
            chunkable = task_payload.get("chunkable", False)
            repo_params = {
                "csv_delimiter": task_payload.get("csv_delimiter", ","),
                "csv_has_header": task_payload.get("csv_has_header", True),
                "txt_delimiter": task_payload.get("txt_delimiter", "|"),
                "txt_has_header": task_payload.get("txt_has_header", False),
                "json_records_path": task_payload.get("json_records_path"),
                "encoding": task_payload.get("encoding", "utf-8")
            }
            repo_params["delimiter"] = repo_params.get("csv_delimiter") if data_kind == RepoData.Kind.CSV else repo_params.get("txt_delimiter")

            futures: List[Future[Optional[DataFrame]]] = []

            # Chunking Logic
            if chunkable and (data_kind in [RepoData.Kind.CSV, RepoData.Kind.TXT]):
                chunk_defs = self._split_file_into_line_chunk_definitions(resolved_path, repo_params)
                if chunk_defs:
                    logger.info(f"{self.name}: Submitting {len(chunk_defs)} chunks for '{task_id}'.")
                    for i, chunk_def in enumerate(chunk_defs):
                        worker_params = dict(repo_params)
                        # Only first chunk *might* have header based on original spec
                        has_orig_header = worker_params.get('csv_has_header') or worker_params.get('txt_has_header')
                        worker_params["header_for_chunk"] = chunk_def.get("header_for_chunk")
                        fut = self.hybrid_pool.submit(
                            parse_file_chunk_worker, data_kind, resolved_path, chunk_def, worker_params
                        )
                        futures.append(fut)
                else: # Chunking failed or no chunks, fall back to whole file
                    logger.warning(f"{self.name}: Could not create chunks for {resolved_path}. Processing as whole.")
                    futures.append(self.hybrid_pool.submit(
                        parse_file_chunk_worker, data_kind, resolved_path, {"is_full_file": True}, repo_params
                    ))
            # Whole File Logic
            else:
                logger.info(f"{self.name}: Processing whole file {resolved_path} for task '{task_id}'.")
                futures.append(self.hybrid_pool.submit(
                    parse_file_chunk_worker, data_kind, resolved_path, {"is_full_file": True}, repo_params
                ))

            # Collect results
            partial_dfs: List[DataFrame] = [fut.result() for fut in as_completed(futures) if fut.result() is not None]

            if not partial_dfs:
                logger.warning(f"{self.name}: No data returned from workers for task '{task_id}'.")
                final_df = DataFrame(columns=task_payload.get("expected_columns", []))
            else:
                final_df = DataFrame.concat_dfs(partial_dfs)
                logger.info(f"{self.name}: Extracted data for '{task_id}', shape {final_df.shape}.")

        # Ensure we always have a DataFrame (even if empty)
        if final_df is None:
            final_df = DataFrame(columns=task_payload.get("expected_columns", []))

        # --- Always return the consistent (task_id, df, payload) format ---
        return task_id, final_df, task_payload


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

                # Standard processing: item is (task_id, task_payload)
                task_id, task_payload = item
                processed_item = self.handle_item(task_id, task_payload)

                if processed_item:
                    # ThreadWrapper expects (key, value)
                    # We want to send (task_id, (df, payload))
                    out_key, out_df, out_payload = processed_item
                    self.out_queue.put((out_key, (out_df, out_payload)))

                # self.in_queue.task_done() # If using JoinableQueue

            except queue.Empty:
                # If not running and queue is empty, loop will terminate.
                # If running, just continue waiting.
                continue
            except Exception as e:
                logger.error(f"{self.name}: Error processing item: {e}", exc_info=True)
                # Ensure task_done is called if using JoinableQueue, even on error.
                # self.in_queue.task_done()

        # --- Signal Downstream ---
        logger.info(f"{self.name}: Stopped processing. Sending sentinel downstream.")
        self.out_queue.put(None)
        logger.info(f"{self.name}: Finished.")