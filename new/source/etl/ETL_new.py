# new/source/etl/ETL.py
from __future__ import annotations

import logging
import queue  # Standard library queue
import sqlite3
import threading
import time # For potential delays or monitoring
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, Callable
from concurrent.futures import as_completed, Future
import argparse

import math # Para math.ceil ao dividir em chunks
import threading # Para o lock de IDs

# >>> MANTENHA O NOME CORRETO DO SEU ARQUIVO DE LÓGICA DE PEDIDOS <<<
try:
    from new.source.etl.orders_ingest import (
        transform_new_orders_worker,
        update_stock_worker,
        get_db_connection, # get_last_pk é usado internamente por OrderProcessingStage
        get_last_pk
    )
    ORDERS_LOGIC_AVAILABLE = True
except ImportError as e:
    logging.error(f"Falha ao importar de 'orders_processing_logic.py': {e}. A funcionalidade de pedidos não estará disponível.")
    ORDERS_LOGIC_AVAILABLE = False
    # Placeholders para evitar erros de NameError se a importação falhar
    def transform_new_orders_worker(*_args, **_kwargs): raise NotImplementedError("orders_processing_logic.py não encontrado")
    def update_stock_worker(*_args, **_kwargs): raise NotImplementedError("orders_processing_logic.py não encontrado")
    def get_db_connection(*_args, **_kwargs): raise NotImplementedError("orders_processing_logic.py não encontrado")
    def get_last_pk(*_args, **_kwargs): raise NotImplementedError("orders_processing_logic.py não encontrado")



from new.source.etl.data_generators import generate_csv as etl_generate_csv
from new.source.etl.data_generators import generate_json as etl_generate_json # Assuming these are in new.etl.data_generators

ETL_POSTGRES_DB_CONFIG = {
    "host": "localhost",
    "database": "my_etl_db",
    "user": "my_etl_user",
    "password": "your_secure_password" # CHANGEME
}


import os
import json

# --- Project-specific imports ---
from new.source.utils.dataframe import DataFrame
from new.source.etl.ETL_utils import RepoData, MapMutex, PostgresLoaderEtl # BaseStrategy, Extractors, Loader from here if needed directly
from new.source.framework.using_threads import ThreadWrapper
from new.source.framework.hybrid_pool import HybridPool, CPU_LIMIT # Assuming CPU_LIMIT is defined in hybrid_pool

LOG_FORMAT = "%(asctime)s [%(levelname)8s] [%(threadName)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


# --- Type Definitions for Queue Items ---
# Task for Extractor: (unique_task_id, {kind: 'csv', path: '...', 'csv_delimiter': ','})
ExtractionTaskPayload = Dict[str, Any]
ExtractionTaskItem = Tuple[str, ExtractionTaskPayload]

# Define type for order processing stage input: (task_id, df_raw_orders, task_payload)
ExtractedDataQueueItem = Tuple[str, DataFrame, ExtractionTaskPayload]
# Define type for order processing stage output: (task_id, {'pedidos': DataFrame, 'itens': DataFrame}, task_payload)
ProcessedDataQueueItem = Tuple[str, Dict[str, DataFrame], ExtractionTaskPayload]

# Item for Transformer/Loader: (unique_task_id_or_source_key, DataFrame)
DataFrameQueueItem = Tuple[str, DataFrame]

# --- Global Configuration (Consider moving to a config file/class) ---
# Define base paths for different data sources if they are predictable
# This should be configured based on your project structure.
# Example: if ETL.py is in project_root/new/source/etl/
# and simulator data is in project_root/simulator/data/
# try:
#     # This assumes ETL.py is two levels down from the project root where 'simulator' resides.
#     # Adjust as necessary.
#     _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
#     PATH_CONFIG = {
#         "SIMULATOR_DATA_ROOT": _PROJECT_ROOT / "simulator" / "data",
#         "DEFAULT_OUTPUT_DB_PATH": _PROJECT_ROOT / "output_data" / "etl_output.db", # Example output
#         # Add other base paths as needed
#     }
#     PATH_CONFIG["DEFAULT_OUTPUT_DB_PATH"].parent.mkdir(parents=True, exist_ok=True) # Ensure output dir exists
# except Exception as e:
#     logger.error(f"Could not determine project paths based on __file__: {e}. Paths may need manual configuration.")
#     PATH_CONFIG = {"SIMULATOR_DATA_ROOT": Path("."), "DEFAULT_OUTPUT_DB_PATH": Path("etl_output.db")}


try:
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    PATH_CONFIG = {
        "SIMULATOR_DATA_ROOT": _PROJECT_ROOT / "simulator_data", # For any remaining file examples
        "DEFAULT_SQLITE_OUTPUT_DB_PATH": _PROJECT_ROOT / "output_data" / "etl_sqlite_output.db",
    }
    if "DEFAULT_SQLITE_OUTPUT_DB_PATH" in PATH_CONFIG:
         PATH_CONFIG["DEFAULT_SQLITE_OUTPUT_DB_PATH"].parent.mkdir(parents=True, exist_ok=True)
except Exception as e:
    logger.warning(f"Could not determine project paths based on __file__: {e}. Paths may need manual configuration.")
    PATH_CONFIG = {}


# --- CPU-Bound Worker Functions (to be run in HybridPool) ---
def parse_file_chunk_worker(
    strategy_kind: str,
    file_path: Path,
    # Parameters specific to chunking, e.g., byte offset, line count, or pre-read lines
    chunk_definition: Dict[str, Any], # e.g., {'lines': ['line1', 'line2']} or {'offset': N, 'bytes': M}
    repo_extraction_params: Dict[str, Any] # e.g., delimiter, header_in_chunk, records_path
) -> Optional[DataFrame]:
    """
    Worker function to parse a defined chunk of a file.
    This needs to be carefully designed based on how file types can be chunked.
    """
    try:
        repo = RepoData() # Each worker gets its own instance
        logger.debug(f"Worker: Parsing chunk for {file_path} using {strategy_kind}. Chunk def: {str(chunk_definition)[:100]}")

        # This is a highly simplified placeholder. Real chunk parsing is complex:
        # - For CSV/TXT: Might involve reading specific lines passed in chunk_definition['lines']
        #   or seeking to an offset and reading N bytes/lines. Header handling for chunks is tricky.
        # - For JSON: Generally hard to chunk unless it's line-delimited JSON.
        #   This might just process the whole file if chunk_definition implies it.

        if "lines_to_parse" in chunk_definition and (strategy_kind == RepoData.Kind.CSV or strategy_kind == RepoData.Kind.TXT) :
            # Assume lines are passed directly for parsing
            lines = chunk_definition["lines_to_parse"]
            header = repo_extraction_params.get("header_for_chunk") # Header might be passed if known
            
            # Minimalistic CSV/TXT parsing from lines (adapt from dataframe.py's read_csv internals)
            if not lines: return DataFrame(columns=header or [])
            
            # If header is not provided with chunk, first line might be data or partial header
            # This logic needs to be robust depending on chunking strategy
            if header:
                df_chunk = DataFrame(columns=header)
                delim = repo_extraction_params.get("delimiter", ",")
                for line_str in lines:
                    values = [val.strip() for val in line_str.split(delim)]
                    if len(values) == len(header):
                        df_chunk.append(dict(zip(header, values)))
                    else:
                        logger.warning(f"Worker: Mismatched columns for line in chunk: {line_str[:50]}")
            else: # No header, infer or use default col names
                # This is too simplistic for robust chunk parsing without known schema
                logger.warning("Worker: Chunk parsing without header is highly unreliable. Returning raw.")
                return DataFrame(data=[line.split(repo_extraction_params.get("delimiter", ",")) for line in lines]) # Basic split
            return df_chunk

        # Fallback to full file processing if no specific chunking logic matches
        repo.set_strategy_for_extraction(
            kind=strategy_kind,
            path=file_path,
            **repo_extraction_params # Pass all relevant params
        )
        return repo.extract_data()

    except Exception as e:
        logger.error(f"Worker: Error parsing chunk for {file_path}: {e}", exc_info=True)
        return None

def transform_dataframe_chunk_worker(
    df_chunk: DataFrame,
    transform_function: Callable[[DataFrame], DataFrame], # User-defined transform
    transform_params: Optional[Dict[str, Any]] = None
) -> Optional[DataFrame]:
    """Worker function to apply transformations to a DataFrame chunk."""
    try:
        logger.debug(f"Worker: Transforming DataFrame chunk, shape {df_chunk.shape}")
        if transform_params:
            return transform_function(df_chunk, **transform_params)
        return transform_function(df_chunk)
    except Exception as e:
        logger.error(f"Worker: Error transforming DataFrame chunk: {e}", exc_info=True)
        return None

# --- ETL Pipeline Threads ---
class ExtractStage(ThreadWrapper[str, ExtractionTaskPayload]):
    def __init__(
        self,
        in_queue: queue.Queue[ExtractionTaskItem],
        out_queue: queue.Queue[DataFrameQueueItem],
        map_mutex: MapMutex, # For tasks like _handle_datacat state
        hybrid_pool: HybridPool,
        path_config: Dict[str, Path],
        pg_db_config: Optional[Dict] = None, # For PostgresEventExtractor
        default_pg_event_batch_size: int = 100,
        default_chunk_size_lines: int = 100000, # For splitting large files
        name: str = "ExtractorStage",
    ):
        super().__init__(in_queue, out_queue, name=name)
        self.map_mutex = map_mutex
        self.hybrid_pool = hybrid_pool
        self.repo = RepoData() # Each stage instance gets its own RepoData
        self.pg_db_config = pg_db_config
        self.default_pg_event_batch_size = default_pg_event_batch_size
        self.path_config = path_config
        self.default_chunk_size_lines = default_chunk_size_lines

    def _resolve_path_from_task(self, path_str: str) -> Path:
        p = Path(path_str)
        if p.is_absolute(): return p
        # Example: task path "contaverde/products.txt" + SIMULATOR_DATA_ROOT
        # This assumes task paths are relative to one of the roots in PATH_CONFIG
        for root_key, root_path in self.path_config.items():
            if (root_path / path_str).exists():
                return (root_path / path_str).resolve()
        logger.warning(f"Could not resolve path '{path_str}' against known roots. Trying as relative to CWD.")
        return Path(path_str).resolve() # Fallback

    def _split_file_into_line_chunk_definitions(self, file_path: Path, strategy_params: Dict) -> List[Dict[str,Any]]:
        """
        Splits a large text file (CSV/TXT) into definitions for line-based chunks.
        Returns a list of dicts, each like {'lines': list_of_lines_for_chunk}.
        This is a basic implementation; byte-offset chunking might be more robust for huge files.
        """
        chunk_defs = []
        header_line = None
        try:
            with file_path.open('r', encoding=strategy_params.get('encoding', 'utf-8')) as f:
                if strategy_params.get('csv_has_header', True) or strategy_params.get('txt_has_header', False):
                    header_line = f.readline().strip()

                current_chunk_lines = []
                for line in f:
                    current_chunk_lines.append(line.strip())
                    if len(current_chunk_lines) >= self.default_chunk_size_lines:
                        chunk_defs.append({'lines_to_parse': current_chunk_lines, 'header_for_chunk': header_line.split(strategy_params.get("delimiter",",")) if header_line else None})
                        current_chunk_lines = []
                if current_chunk_lines: # Remaining lines
                    chunk_defs.append({'lines_to_parse': current_chunk_lines, 'header_for_chunk': header_line.split(strategy_params.get("delimiter",",")) if header_line else None})
        except Exception as e:
            logger.error(f"Error splitting file {file_path} into chunks: {e}")
        return chunk_defs


    def handle_item(self, task_id: str, task_payload: ExtractionTaskPayload) -> Optional[DataFrameQueueItem]:
        data_kind = task_payload.get("kind")
        file_path_str = task_payload.get("path")
        chunkable = task_payload.get("chunkable", False) # New task param to indicate if chunking is desired/possible

        if not data_kind or not file_path_str:
            logger.error("Extractor: Task '%s' missing 'kind' or 'path'.", task_id)
            return None

        file_path = self._resolve_path_from_task(file_path_str)
        if not file_path.exists() and data_kind != "datacat_loader": # datacat_loader path is a directory
             logger.error("Extractor: File not found for task '%s': %s", task_id, file_path)
             return None
        
        logger.info("Extractor: Processing '%s' for file: %s", task_id, file_path)

        # --- Specific handler for incremental 'datacat' ---
        if data_kind == "datacat_loader":
            # Ensure file_path is the base directory for datacat
            # Logic from previous _handle_datacat, adapted
            if not file_path.is_dir():
                logger.error("Extractor: DataCat path %s for task '%s' is not a directory.", file_path, task_id)
                return None

            processed_files_key = f"datacat_last_file_{file_path.name}" # Make key specific to this datacat source
            dfs_from_new_files: List[DataFrame] = []
            
            # Using map_mutex to get/set the last processed file marker
            # This lock ensures that reading the marker and updating it is atomic relative to other threads
            # that might be accessing the *same* marker for the *same* datacat directory.
            with self.map_mutex.key_lock(processed_files_key):
                last_processed_file_marker = str(self.map_mutex.get(processed_files_key, ""))
                
                new_marker_to_set = last_processed_file_marker

                datacat_futures: List[Future[Optional[DataFrame]]] = []
                files_to_process_this_run = [] # Keep track for updating marke
                
                # Iterate over sorted files in the directory
                # This assumes files are named in a way that sorting gives chronological order
                sorted_entries = sorted([entry for entry in file_path.iterdir() if entry.is_file()])

                for entry_path in sorted_entries:
                    entry_name = entry_path.name # Using just name for marker comparison
                    if entry_name > last_processed_file_marker:
                        logger.info("Extractor (DataCat): Processing new file %s", entry_path)
                        # Construct repo_params for this specific file type (e.g. TXT)
                        files_to_process_this_run.append(entry_path)
                        logging.info("Extractor (DataCat): File %s is new, processing...", entry_path)
                        datacat_file_repo_params = {
                            "txt_delimiter": task_payload.get("txt_delimiter", "|"), # Default or from task_payload
                            "txt_has_header": task_payload.get("txt_has_header", False),
                            "encoding": task_payload.get("encoding", "utf-8")
                        }
                        # We process each new datacat file as a single unit for simplicity here
                        # Submitting to HybridPool for consistency, though it might be overkill for small files
                        future = self.hybrid_pool.submit(
                            parse_file_chunk_worker,
                            RepoData.Kind.TXT, # Assuming datacat files are TXT
                            entry_path,
                            {"is_full_file": True}, # Indicates to worker to process whole file
                            datacat_file_repo_params
                        )
                        datacat_futures.append(future)
                        new_marker_to_set = max(new_marker_to_set, entry_name)
                        df_single_file = future.result() # Blocking for each file in this example
                        if df_single_file is not None:
                            dfs_from_new_files.append(df_single_file)
                        new_marker_to_set = max(new_marker_to_set, entry_name)

                for fut in as_completed(datacat_futures):
                    df_result = fut.result()
                    if df_result is not None:
                        dfs_from_new_files.append(df_result)
                    else:
                        logger.warning("Extractor (DataCat): One of the futures returned None for task '%s'", task_id)
                
                if new_marker_to_set != last_processed_file_marker:
                    self.map_mutex.set(processed_files_key, new_marker_to_set)
                    logger.info("Extractor (DataCat): Updated last processed file marker to %s", new_marker_to_set)

            if not dfs_from_new_files:
                logger.info("Extractor (DataCat): No new files to process for %s", file_path)
                return task_id, DataFrame(columns=task_payload.get("expected_columns", [])) # Send empty DF with schema if known

            # Concatenate all DataFrames from new files
            if len(dfs_from_new_files) == 1:
                final_df = dfs_from_new_files[0]
            else:
                final_df = dfs_from_new_files[0]
                for next_df in dfs_from_new_files[1:]:
                    final_df = final_df.concat(next_df) # Assumes DataFrame.concat handles schema alignment
            
            logger.info("Extractor (DataCat): Processed %d new files from %s, combined shape %s", len(dfs_from_new_files), file_path, final_df.shape)
            return task_id, final_df


        # --- General file processing logic (can be chunked or full file) ---
        repo_extraction_params = {
            "csv_delimiter": task_payload.get("csv_delimiter"),
            "csv_has_header": task_payload.get("csv_has_header", True),
            "txt_delimiter": task_payload.get("txt_delimiter", "|"),
            "txt_has_header": task_payload.get("txt_has_header", False),
            "json_records_path": task_payload.get("json_records_path"),
            "encoding": task_payload.get("encoding", "utf-8")
        }

        if chunkable and (data_kind == RepoData.Kind.CSV or data_kind == RepoData.Kind.TXT):
            logger.info("Extractor: Chunking file %s for task '%s'", file_path, task_id)
            chunk_definitions = self._split_file_into_line_chunk_definitions(file_path, repo_extraction_params)
            if not chunk_definitions:
                logger.warning("Extractor: Could not create chunks for %s. Processing as whole.", file_path)
                # Fallthrough to whole file processing
            else:
                futures: List[Future[Optional[DataFrame]]] = []
                for i, chunk_def in enumerate(chunk_definitions):
                    # Pass necessary params for the worker to reconstruct the strategy
                    worker_repo_params = dict(repo_extraction_params) # copy
                    # Chunks (except maybe the first if file has header) don't have headers themselves
                    if i > 0 or not (worker_repo_params.get('csv_has_header') or worker_repo_params.get('txt_has_header')):
                        worker_repo_params["csv_has_header_in_chunk"] = False
                        worker_repo_params["txt_has_header_in_chunk"] = False
                    else: # First chunk might imply header presence based on original file spec
                        worker_repo_params["csv_has_header_in_chunk"] = worker_repo_params.get('csv_has_header',True)
                        worker_repo_params["txt_has_header_in_chunk"] = worker_repo_params.get('txt_has_header',False)
                    
                    # The worker needs the delimiter too
                    worker_repo_params["delimiter"] = worker_repo_params.get("csv_delimiter") if data_kind == RepoData.Kind.CSV else worker_repo_params.get("txt_delimiter")

                    fut = self.hybrid_pool.submit(
                        parse_file_chunk_worker,
                        data_kind,
                        file_path, # Worker might not need this if lines are passed directly
                        chunk_def, # e.g. {'lines': list_of_lines}
                        worker_repo_params
                    )
                    futures.append(fut)

                partial_dfs: List[DataFrame] = []
                for fut in as_completed(futures):
                    df_chunk = fut.result()
                    if df_chunk is not None:
                        partial_dfs.append(df_chunk)
                
                if not partial_dfs:
                    logger.warning("Extractor: All chunks failed or returned empty for task '%s' on file %s", task_id, file_path)
                    return task_id, DataFrame(columns=task_payload.get("expected_columns", []))

                # Concatenate partial DataFrames
                final_df = partial_dfs[0]
                for i in range(1, len(partial_dfs)):
                    final_df = final_df.concat(partial_dfs[i])
                
                logger.info("Extractor: Reconstructed DataFrame from %d chunks for task '%s', shape %s", len(partial_dfs), task_id, final_df.shape)
                return task_id, final_df

        # --- Fallback to processing the whole file if not chunkable or chunking failed ---
        logger.info("Extractor: Processing whole file %s for task '%s'", file_path, task_id)
        try:
            # Using a single submission to HybridPool for consistency, even for whole files.
            # This allows HybridPool to manage resources (threads/processes).
            future = self.hybrid_pool.submit(
                parse_file_chunk_worker,
                data_kind,
                file_path,
                {"is_full_file": True}, # Indicates to worker to process whole file
                repo_extraction_params
            )
            df_result = future.result() # Wait for the single future

            if df_result is not None:
                logger.info("Extractor: Successfully processed task '%s' (whole file), DataFrame shape %s", task_id, df_result.shape)
                return task_id, df_result
            else:
                logger.warning("Extractor: Whole file processing returned None for task '%s'", task_id)
                return task_id, DataFrame(columns=task_payload.get("expected_columns", [])) # Empty with schema
        except Exception as e:
            logger.error("Extractor: Error processing whole file for task '%s': %s", task_id, e, exc_info=True)
            return None


class TransformStage(ThreadWrapper[str, DataFrame]): # Input: (task_id, DataFrame)
    def __init__(
        self,
        in_queue: queue.Queue[DataFrameQueueItem],
        out_queue: queue.Queue[DataFrameQueueItem],
        hybrid_pool: HybridPool,
        # Users can provide their own transform function and its params
        user_transform_function: Optional[Callable[[DataFrame, Any], DataFrame]] = None,
        user_transform_params: Optional[Dict[str, Any]] = None,
        default_chunk_size_rows: int = 50000, # For splitting large DataFrames for parallel transform
        name: str = "TransformerStage",
    ):
        super().__init__(in_queue, out_queue, name=name)
        self.hybrid_pool = hybrid_pool
        self.user_transform_function = user_transform_function
        self.user_transform_params = user_transform_params or {}
        self.default_chunk_size_rows = default_chunk_size_rows

    def handle_item(self, task_id: str, df_input: DataFrame) -> Optional[DataFrameQueueItem]:
        if self.user_transform_function is None:
            logger.debug("Transformer: No user transform function provided. Passing data for '%s' through.", task_id)
            return task_id, df_input # Pass through if no transform defined

        if df_input.shape[0] == 0:
            logger.info("Transformer: Input DataFrame for task '%s' is empty. Passing through.", task_id)
            return task_id, df_input
            
        logger.info("Transformer: Applying transformation for task '%s' to DataFrame of shape %s", task_id, df_input.shape)

        try:
            # Simple case: transform the whole DataFrame at once if small enough or no chunking
            # For large DataFrames, split into row-chunks and process in HybridPool
            if df_input.shape[0] <= self.default_chunk_size_rows * 1.5: # Heuristic for "small enough"
                 transformed_df = transform_dataframe_chunk_worker(
                     df_input, self.user_transform_function, self.user_transform_params
                 )
                 if transformed_df is None:
                     logger.error("Transformer: Transformation returned None for task '%s'", task_id)
                     return None # Or return original df_input on error?
                 logger.info("Transformer: Transformed DataFrame for task '%s', new shape %s", task_id, transformed_df.shape)
                 return task_id, transformed_df
            else:
                # Chunk the DataFrame by rows
                logger.info("Transformer: Chunking DataFrame for task '%s' (shape %s) for parallel transformation.", task_id, df_input.shape)
                df_chunks: List[DataFrame] = []
                for i in range(0, df_input.shape[0], self.default_chunk_size_rows):
                    # Slicing the DataFrame (needs a DataFrame.iloc or slice method)
                    # For now, a manual reconstruction of chunk:
                    chunk_rows = [df_input.get_row_dict(j) for j in range(i, min(i + self.default_chunk_size_rows, df_input.shape[0]))]
                    if chunk_rows:
                        df_chunks.append(DataFrame(columns=df_input.columns, data=chunk_rows))
                
                if not df_chunks:
                    logger.warning("Transformer: Failed to create chunks for task '%s'. Processing as whole.", task_id)
                    # Fallback to whole processing (already handled by the if-condition above, this is defensive)
                    transformed_df = transform_dataframe_chunk_worker(df_input, self.user_transform_function, self.user_transform_params)
                    return task_id, transformed_df if transformed_df else df_input


                futures: List[Future[Optional[DataFrame]]] = []
                for df_chunk in df_chunks:
                    fut = self.hybrid_pool.submit(
                        transform_dataframe_chunk_worker,
                        df_chunk,
                        self.user_transform_function,
                        self.user_transform_params
                    )
                    futures.append(fut)

                transformed_dfs: List[DataFrame] = []
                for fut in as_completed(futures):
                    res_df = fut.result()
                    if res_df is not None:
                        transformed_dfs.append(res_df)
                
                if not transformed_dfs:
                    logger.error("Transformer: All transformation chunks failed or returned empty for task '%s'", task_id)
                    return None # Or return original df_input?

                # Concatenate transformed DataFrames
                final_transformed_df = transformed_dfs[0]
                for i in range(1, len(transformed_dfs)):
                    final_transformed_df = final_transformed_df.concat(transformed_dfs[i])
                
                logger.info("Transformer: Reconstructed transformed DataFrame from %d chunks for task '%s', shape %s", len(transformed_dfs), task_id, final_transformed_df.shape)
                return task_id, final_transformed_df

        except Exception as e:
            logger.error("Transformer: Error during transformation for task '%s': %s", task_id, e, exc_info=True)
            return None # Or task_id, df_input to pass original on error
        
class OrderProcessingStage(ThreadWrapper[str, Union[DataFrame, Dict[str, DataFrame]]]): # O tipo de input depende de onde ela pega os dados
    def __init__(
        self,
        in_queue: queue.Queue[ExtractedDataQueueItem], # Recebe (task_id, df_raw_orders, task_payload)
        out_queue: queue.Queue[ProcessedDataQueueItem], # Envia (task_id, {'pedidos': df, 'itens': df}, task_payload)
        hybrid_pool: HybridPool,
        ecommerce_db_path: Path,
        default_chunk_size_rows: int = 10000, # Chunk para processamento paralelo de pedidos
        name: str = "OrderProcessingStage",
    ):
        super().__init__(in_queue, out_queue, name=name)
        self.hybrid_pool = hybrid_pool
        self.ecommerce_db_path = ecommerce_db_path
        self.default_chunk_size_rows = default_chunk_size_rows
        self._id_lock = threading.Lock() # Lock para proteger get_last_pk globalmente
        self._ecommerce_conn_for_ids: Optional[sqlite3.Connection] = None # Conexão reutilizável para IDs

        if not ORDERS_LOGIC_AVAILABLE:
            logger.error(f"{self.name} instanciada, mas a lógica de processamento de pedidos não está disponível devido a erro de importação.")


    def _get_reusable_ecommerce_connection(self) -> sqlite3.Connection:
        # Reutiliza conexão para buscar IDs para evitar abrir/fechar muitas conexões
        # ATENÇÃO: Esta conexão NÃO deve ser passada para os workers do HybridPool se eles forem processos.
        # Cada worker (processo) deve abrir sua própria conexão.
        # Se os workers forem threads, esta conexão pode ser usada COM CUIDADO (SQLite e threads).
        # Para `get_last_pk` executado serialmente sob lock, está OK.
        if self._ecommerce_conn_for_ids is None or self._is_connection_closed(self._ecommerce_conn_for_ids):
            try:
                self._ecommerce_conn_for_ids = get_db_connection(self.ecommerce_db_path)
            except Exception as e:
                logger.error(f"{self.name}: Falha ao obter conexão com DB de e-commerce para IDs: {e}")
                raise
        return self._ecommerce_conn_for_ids

    def _is_connection_closed(self, conn: sqlite3.Connection) -> bool:
        try:
            # Tenta uma operação simples para verificar se a conexão está ativa
            conn.execute("SELECT 1").fetchone()
            return False
        except sqlite3.ProgrammingError: # Connection closed or cursor used on closed connection
            return True
        except Exception: # Outros erros potenciais
            return True


    def handle_item(self, task_id: str, df_raw_orders: DataFrame, task_payload: ExtractionTaskPayload) -> Optional[ProcessedDataQueueItem]:
        if not ORDERS_LOGIC_AVAILABLE:
            logger.error(f"{self.name}: Lógica de pedidos indisponível, pulando tarefa '{task_id}'.")
            # Passar um item de erro ou DataFrame vazio para a próxima stage
            return task_id, DataFrame(columns=["error"]), task_payload


        # Esta stage só processa tarefas marcadas para processamento de pedidos
        if task_payload.get("processing_type") != "ecommerce_new_orders":
            # Se esta stage receber algo que não é para ela, logar e descartar, ou rotear para outra fila (mais complexo)
            logger.debug(f"{self.name}: Tarefa '{task_id}' não é do tipo 'ecommerce_new_orders'. Pulando.")
            return None # Não envia nada para a out_queue desta stage

        if df_raw_orders.empty:
            logger.info(f"{self.name}: DataFrame de pedidos brutos para '{task_id}' está vazio.")
            # Enviar DataFrames vazios para 'pedidos' e 'itens'
            empty_result = {'pedidos': DataFrame(), 'itens': DataFrame()}
            return task_id, empty_result, task_payload

        logger.info(f"{self.name}: Iniciando processamento de {df_raw_orders.shape[0]} pedidos brutos para '{task_id}'.")

        conn_for_ids = None
        try:
            # Obter IDs iniciais globais sob lock
            with self._id_lock:
                conn_for_ids = self._get_reusable_ecommerce_connection()
                start_overall_pedido_id = get_last_pk(conn_for_ids, "pedidos") + 1
                start_overall_item_id = get_last_pk(conn_for_ids, "itens_pedido") + 1
                logger.info(f"{self.name}: IDs iniciais globais para '{task_id}': Pedido={start_overall_pedido_id}, Item={start_overall_item_id}")

            num_chunks = math.ceil(df_raw_orders.shape[0] / self.default_chunk_size_rows)
            futures: List[Future] = []
            current_pedido_id_offset = 0
            current_item_id_offset = 0 # Assumindo 1 item por pedido bruto para cálculo de offset de ID de item

            for i in range(num_chunks):
                chunk_df = df_raw_orders.slice(i * self.default_chunk_size_rows, (i + 1) * self.default_chunk_size_rows)
                if chunk_df.empty:
                    continue

                # IDs para este chunk específico
                chunk_start_pedido_id = start_overall_pedido_id + current_pedido_id_offset
                # Assumimos que transform_new_orders_worker gera 1 item_pedido por linha do chunk_df
                chunk_start_item_id = start_overall_item_id + current_item_id_offset

                logger.debug(f"{self.name}: Submetendo chunk {i+1}/{num_chunks} (linhas: {chunk_df.shape[0]}) para '{task_id}' com IDs Pedido={chunk_start_pedido_id}, Item={chunk_start_item_id}")

                # transform_new_orders_worker precisa de db_path, não de uma conexão,
                # pois ele mesmo abre/fecha conexões se for executado em outro processo.
                future = self.hybrid_pool.submit(
                    transform_new_orders_worker,
                    chunk_df,
                    self.ecommerce_db_path, # Worker abre sua própria conexão
                    chunk_start_pedido_id,
                    chunk_start_item_id
                )
                futures.append(future)

                current_pedido_id_offset += chunk_df.shape[0] # Avança o offset pelo número de pedidos no chunk
                current_item_id_offset += chunk_df.shape[0] # Assumindo 1 item por pedido

            all_pedidos_dfs: List[DataFrame] = []
            all_itens_dfs: List[DataFrame] = []
            for future in as_completed(futures):
                try:
                    df_p, df_i = future.result()
                    if df_p is not None and not df_p.empty:
                        all_pedidos_dfs.append(df_p)
                    if df_i is not None and not df_i.empty:
                        all_itens_dfs.append(df_i)
                except Exception as e:
                    logger.error(f"{self.name}: Erro em um worker de transformação de pedidos para '{task_id}': {e}", exc_info=True)

            if not all_pedidos_dfs: # Se nenhum pedido foi processado com sucesso
                logger.warning(f"{self.name}: Nenhum DataFrame de pedido válido retornado pelos workers para '{task_id}'.")
                final_pedidos_df = DataFrame() # Schema pode ser adicionado se conhecido
                final_itens_df = DataFrame()
            else:
                final_pedidos_df = DataFrame.concat_dfs(all_pedidos_dfs) if len(all_pedidos_dfs) > 1 else all_pedidos_dfs[0]
                final_itens_df = DataFrame.concat_dfs(all_itens_dfs) if len(all_itens_dfs) > 1 and all_itens_dfs else \
                                 (all_itens_dfs[0] if all_itens_dfs else DataFrame())


            logger.info(f"{self.name}: Processamento de pedidos para '{task_id}' concluído. Pedidos gerados: {final_pedidos_df.shape}, Itens gerados: {final_itens_df.shape}")
            return task_id, {'pedidos': final_pedidos_df, 'itens': final_itens_df}, task_payload

        except Exception as e:
            logger.error(f"{self.name}: Erro crítico no processamento de pedidos para '{task_id}': {e}", exc_info=True)
            # Enviar DataFrames vazios em caso de erro crítico na stage
            return task_id, {'pedidos': DataFrame(), 'itens': DataFrame()}, task_payload

class LoadStage(ThreadWrapper[str, DataFrame]): # Input: (task_id, DataFrame)
    def __init__(
        self,
        in_queue: queue.Queue[DataFrameQueueItem],
        # No out_queue for the final stage, but could have one for errors/status
        db_path_or_conn: Union[str, Path, sqlite3.Connection], # Can take path or existing conn
        target_table_prefix: str = "", # e.g., "etl_output_"
        if_exists_mode: str = "append", # "append", "replace", "fail"
        name: str = "LoaderStage",
    ):
        super().__init__(in_queue, out_queue=None, name=name) # No out_queue
        self.repo = RepoData()
        self.db_path_or_conn = db_path_or_conn
        self.target_table_prefix = target_table_prefix
        self.if_exists_mode = if_exists_mode
        self._times_lock = threading.Lock() # Copied from original LoaderThread
        self._times_file = PATH_CONFIG.get("TIMES_FILE_PATH", Path("etl_times.txt")) # Configurable

    def _log_time(self, table_name: str, operation_start_ns: int) -> None:
        # Simple time logging
        duration_ns = time.time_ns() - operation_start_ns
        with self._times_lock, self._times_file.open("a", encoding="utf-8") as fh:
            fh.write(f"{self.name} {table_name} duration_ms:{duration_ns / 1_000_000:.2f}\n")


    def handle_item(self, task_id: str, df_to_load: DataFrame) -> None: # Returns None as it's a sink
        table_name = f"{self.target_table_prefix}{task_id}" # e.g., "etl_output_produtos"
        logger.info("Loader: Loading DataFrame for task '%s' (shape %s) into table '%s'", task_id, df_to_load.shape, table_name)
        
        start_ns = time.time_ns()
        try:
            self.repo.set_strategy_for_loading(
                kind=RepoData.Kind.SQL,
                db_conn_or_path=self.db_path_or_conn,
                table_name=table_name,
                if_exists=self.if_exists_mode
            )
            self.repo.load_data(df_to_load)
            logger.info("Loader: Successfully loaded data for task '%s' into '%s'", task_id, table_name)
        except Exception as e:
            logger.error("Loader: Failed to load data for task '%s' into table '%s': %s", task_id, table_name, e, exc_info=True)
        finally:
            self._log_time(table_name, start_ns)
            # Important: RepoData now manages closing its own connection if it created one
            # self.repo._close_previous_strategy_connection() # Ensure connection is closed by RepoData
        return None # No output item for the next stage



def setup_etl_benchmark_data(sim_root: Path, regenerate: bool = False):
    logger.info(f"Setting up benchmark data in {sim_root}")
    sim_root.mkdir(parents=True, exist_ok=True)

    # Define paths for data used by ETL_new.py's example_tasks
    # Ensure these paths match what's in your example_tasks
    # CSV example (e.g., for 'products' task)
    etl_csv_path = sim_root / "contaverde" / "products_benchmark.txt" # Use .txt if tasks expect .txt
    etl_csv_path.parent.mkdir(parents=True, exist_ok=True)
    if regenerate or not etl_csv_path.exists():
        logger.info(f"Generating CSV for ETL benchmark: {etl_csv_path}")
        # generate_csv creates many columns, ensure your 'products' task can handle it
        # or simplify the generate_csv call if 'products' expects a simpler schema.
        # For this example, let's assume the 'products' task is flexible or you adapt it.
        etl_generate_csv(n_rows=1000000, out_path=etl_csv_path) # Adjust n_rows

    # JSON example (e.g., for 'new_orders' task)
    etl_json_path = sim_root / "orders_benchmark.json"
    if regenerate or not etl_json_path.exists():
        logger.info(f"Generating JSON for ETL benchmark: {etl_json_path}")
        etl_generate_json(n_rows=500000, out_path=etl_json_path) # Adjust n_rows
        
    # TXT example (e.g., for 'cade_data' task)
    etl_txt_path = sim_root / "cadeanalytics" / "cade_benchmark.txt"
    etl_txt_path.parent.mkdir(parents=True, exist_ok=True)
    if regenerate or not etl_txt_path.exists():
        logger.info(f"Generating TXT (as CSV) for ETL benchmark: {etl_txt_path}")
        # Using generate_csv for TXT with a specific delimiter
        etl_generate_csv(n_rows=300000, out_path=etl_txt_path) # Will be comma-delimited

    # DataCat example (if you want to benchmark it)
    datacat_dir = sim_root / "datacat_benchmark" / "behaviour"
    datacat_dir.mkdir(parents=True, exist_ok=True)
    if regenerate or not list(datacat_dir.glob("*.txt")):
        logger.info(f"Generating DataCat files for ETL benchmark in {datacat_dir}")
        for i in range(5): # Generate a few files
            etl_generate_csv(n_rows=100000, out_path=datacat_dir / f"events_log_bench_{i}.txt")


# --- Main ETL Orchestration ---
def run_generic_etl_pipeline(
    tasks: List[ExtractionTaskItem],
    # Optional user-defined transform function
    transform_function: Optional[Callable[[DataFrame, Any], DataFrame]] = None,
    transform_params: Optional[Dict[str, Any]] = None,
    # DB config for loader
    db_target: Union[str, Path, sqlite3.Connection] = PATH_CONFIG["DEFAULT_OUTPUT_DB_PATH"],
    db_table_prefix: str = "output_",
    db_if_exists: str = "append",
    # Concurrency config
    num_workers_hybrid_pool: int = min(CPU_LIMIT, (os.cpu_count() or 1)),
    queue_capacity: int = 100,
):
    logger.info("Starting Generic ETL Pipeline...")
    pipeline_start_time = time.perf_counter()
    logger.info("Path Config: %s", PATH_CONFIG)
    logger.info("Hybrid Pool Workers: %d (CPU_LIMIT: %d)", num_workers_hybrid_pool, CPU_LIMIT)

    # --- Initialize Queues ---
    # (task_id, task_payload_dict)
    extract_tasks_q = queue.Queue[ExtractionTaskItem](maxsize=queue_capacity)
    # (task_id, DataFrame_extracted)
    extracted_data_q = queue.Queue[DataFrameQueueItem](maxsize=queue_capacity)
    # (task_id, DataFrame_transformed)
    transformed_data_q = queue.Queue[DataFrameQueueItem](maxsize=queue_capacity)

    # --- Initialize Shared Resources ---
    map_mutex = MapMutex[str]() # For state like last_processed_file in datacat
    hybrid_pool = HybridPool(limit=num_workers_hybrid_pool)

    # --- Initialize Pipeline Stages (Threads) ---
    extractor = ExtractStage(
        extract_tasks_q,
        extracted_data_q, # Output to transformer or loader
        map_mutex,
        hybrid_pool,
        PATH_CONFIG
    )
    
    current_input_for_loader = extracted_data_q
    if transform_function:
        transformer = TransformStage(
            extracted_data_q, # Input from extractor
            transformed_data_q, # Output to loader
            hybrid_pool,
            user_transform_function=transform_function,
            user_transform_params=transform_params
        )
        current_input_for_loader = transformed_data_q # Loader now takes from transformer
        transformer.start()
    else: # No transform stage
        transformer = None # Explicitly
        logger.info("No transformation stage configured.")


    loader = LoadStage(
        current_input_for_loader, # Input from extractor or transformer
        db_path_or_conn=db_target,
        target_table_prefix=db_table_prefix,
        if_exists_mode=db_if_exists
    )

    # --- Start Threads ---
    extractor.start()
    loader.start() # Start loader irrespective of transformer

    # --- Enqueue Initial Tasks ---
    for task_id, task_payload in tasks:
        logger.debug("Enqueuing extraction task: %s", task_id)
        extract_tasks_q.put((task_id, task_payload))

    # --- Wait for Pipeline Completion ---
    # Graceful shutdown:
    # 1. Wait for all tasks to be put on the first queue.
    # 2. Signal extractor to stop once its input queue is empty (by joining its queue).
    # 3. Wait for extractor to finish processing.
    # 4. Signal transformer (if exists) to stop, wait for it.
    # 5. Signal loader to stop, wait for it.
    
    # Wait for all extraction tasks to be picked up and processed by extractor
    # This assumes task_done() is called appropriately in ThreadWrapper
    # For simplicity, we'll join on the queues. A more robust way is to use sentinel values.
    
    logger.info("All extraction tasks enqueued. Waiting for extractor to process...")
    if hasattr(extract_tasks_q, 'join'): extract_tasks_q.join() # If JoinableQueue, but ThreadWrapper uses task_done
    
    # Wait for extractor to finish all its current work after input queue is conceptually empty
    # A simple way is to wait until its output queue is also processed by the next stage
    # This requires careful coordination or sentinels.

    # Simpler join logic for now:
    # After all tasks are enqueued, we wait for the extractor to empty its queue.
    # Then, we tell it to stop. It will finish its current item.
    
    # This loop ensures all tasks are processed through the pipeline
    while not extract_tasks_q.empty():
        time.sleep(0.5)
    logger.info("Extractor input queue is empty.")
    
    # Wait for the next queue to empty, if transformer exists
    if transformer:
        while not extracted_data_q.empty():
            time.sleep(0.5)
        logger.info("Transformer input queue (extracted_data_q) is empty.")
    
    # Wait for the loader's input queue to empty
    while not current_input_for_loader.empty():
        time.sleep(0.5)
    logger.info("Loader input queue is empty.")


    # Now stop the threads
    logger.info("Stopping pipeline stages...")
    extractor.stop()
    if transformer:
        transformer.stop()
    loader.stop()

    # Join threads to ensure they have exited
    extractor.join(timeout=10)
    if transformer:
        transformer.join(timeout=10)
    loader.join(timeout=10)

    # Shutdown the hybrid pool
    logger.info("Shutting down Hybrid Pool...")
    hybrid_pool.shutdown() # Waits for all submitted tasks to complete

    pipeline_end_time = time.perf_counter()
    pipeline_time_duration = pipeline_end_time - pipeline_start_time
    logger.info("ETL Pipeline execution time: %.2f seconds", pipeline_time_duration)

    logger.info("Generic ETL Pipeline finished.")

    return pipeline_time_duration


if __name__ == "__main__":
    # --- Define Example ETL Tasks ---
    # These tasks would typically come from a config file, database, or orchestrator.
    # Paths are now interpreted by ExtractStage._resolve_path_from_task
    # which uses PATH_CONFIG to find data relative to defined roots (e.g., SIMULATOR_DATA_ROOT).

    # --- Accept CLI arguments ---
    cli_parser = argparse.ArgumentParser(description="Run Generic ETL Pipeline with configurable options.")
    cli_parser.add_argument("--workers", type=int,
                        default=max(1, (os.cpu_count() or 2) // 2), # Default from original script
                        help="Number of workers for the HybridPool in the ETL pipeline.")
    cli_parser.add_argument("--setup-data-etl", action="store_true",
                        help="Run data generation for ETL_new.py benchmark and exit.")
    # You can add more args here if needed, e.g., for db_if_exists

    cli_args = cli_parser.parse_args()

    sim_data_root = PATH_CONFIG.get("SIMULATOR_DATA_ROOT", Path("."))
    if cli_args.setup_data_etl:
        print("Setting up data for ETL_new.py benchmark...")
        setup_etl_benchmark_data(sim_data_root, regenerate=True) # Regenerate when this flag is used
        print("ETL_new.py data setup complete. Exiting.")
        exit()

    sim_data_root = PATH_CONFIG.get("SIMULATOR_DATA_ROOT", Path("."))
    if not sim_data_root.is_dir(): # Check if it's a directory
        logger.warning(f"SIMULATOR_DATA_ROOT '{sim_data_root}' does not exist or is not a directory. Example tasks may fail.")
        # Attempt to create dummy structure and files for the example to run
        try:
            (sim_data_root / "contaverde").mkdir(parents=True, exist_ok=True)
            (sim_data_root / "cadeanalytics").mkdir(parents=True, exist_ok=True)
            datacat_behaviour_dir = sim_data_root / "datacat" / "behaviour"
            datacat_behaviour_dir.mkdir(parents=True, exist_ok=True)
            
            # Dummy CSV for products
            products_path = sim_data_root / "contaverde" / "products.txt"
            if not products_path.exists():
                with open(products_path, "w", encoding="utf-8") as f:
                    f.write("product_id,product_name,category,price\n")
                    f.write("P001,Laptop,Electronics,1200.00\n")
                    f.write("P002,Mouse,Electronics,25.00\n")
                    f.write("P003,Keyboard,Electronics,75.00\n")
                    f.write("P004,Desk Chair,Furniture,150.00\n")

            # Dummy TXT for CADE analytics
            cade_path = sim_data_root / "cadeanalytics" / "cade_analytics.txt"
            if not cade_path.exists():
                with open(cade_path, "w", encoding="utf-8") as f:
                    f.write("company_id|segment|market_share\n")
                    f.write("C100|Tech|0.35\n")
                    f.write("C200|Retail|0.20\n")

            # Dummy JSON for new orders
            orders_path = sim_data_root / "orders_new.json" # Place it at sim_data_root for simplicity
            if not orders_path.exists():
                with open(orders_path, "w", encoding="utf-8") as f:
                    json.dump({"all_orders": [
                        {"order_id": "O1", "customer_id": "Cust1", "total": 1225.00, "items_count": 2},
                        {"order_id": "O2", "customer_id": "Cust2", "total": 150.00, "items_count": 1}
                    ]}, f, indent=2)
            
            # Dummy DataCat files
            if not list(datacat_behaviour_dir.glob("*.txt")): # If no .txt files exist
                with open(datacat_behaviour_dir / "events_log_20230101_00.txt", "w", encoding="utf-8") as f:
                    f.write("userA|click|button1\nuserB|view|page2\n")
                with open(datacat_behaviour_dir / "events_log_20230101_01.txt", "w", encoding="utf-8") as f:
                    f.write("userA|purchase|productX\nuserC|search|termY\n")
                # Create a slightly older file to test incremental logic (if marker is empty initially)
                with open(datacat_behaviour_dir / "events_log_20221231_23.txt", "w", encoding="utf-8") as f:
                    f.write("userOld|session_start|systemZ\n")


        except Exception as e_setup:
            logger.error(f"Failed to set up dummy files/directories: {e_setup}")


    example_tasks: List[ExtractionTaskItem] = [
        ("products", { # task_id
            "kind": RepoData.Kind.CSV, 
            "path": "contaverde/products.txt", # Path relative to a configured root (e.g., SIMULATOR_DATA_ROOT)
            "csv_has_header": True,
            "chunkable": True, # Indicate this CSV can be chunked
            "expected_columns": ["product_id", "product_name", "category", "price"] # For empty DF schema
        }),
        ("cade_data", {
            "kind": RepoData.Kind.TXT,
            "path": "cadeanalytics/cade_analytics.txt",
            "txt_delimiter": "|",
            "txt_has_header": True,
            "chunkable": True,
            "expected_columns": ["company_id", "segment", "market_share"]
        }),
        ("new_orders", {
            "kind": RepoData.Kind.JSON,
            "path": "orders_new.json", # Relative to SIMULATOR_DATA_ROOT
            "json_records_path": "all_orders", # Path within JSON to the list of records
            "chunkable": False, # Standard JSON arrays are harder to chunk effectively
            "expected_columns": ["order_id", "customer_id", "total", "items_count"]
        }),
        ("datacat_events", { # Renamed task_id from 'datacat_loader'
            "kind": "datacat_loader", # Special kind for _handle_datacat logic in ExtractStage
            "path": "datacat/behaviour", # Path to the directory, relative to a root
            "txt_delimiter": "|", # Params for the files *within* datacat
            "txt_has_header": False, # Assuming log files might not have headers
            "encoding": "utf-8",
            "expected_columns": ["user_id", "event_type", "event_detail"] # Example schema
        }),
        ("products_benchmark", {
            "kind": RepoData.Kind.CSV,
            "path": "contaverde/products_benchmark.txt",
            "csv_has_header": True, "chunkable": True,
        }),
        ("orders_benchmark", {
            "kind": RepoData.Kind.JSON,
            "path": "orders_benchmark.json",
            "json_records_path": "pedidos",
            "chunkable": False,
        }),
        ("cade_benchmark", {
            "kind": RepoData.Kind.TXT,
            "path": "cadeanalytics/cade_benchmark.txt",
            "txt_delimiter": ",", "txt_has_header": True, "chunkable": True,
        }),
    ]
    

    # --- Define an example transformation function (optional) ---
    def example_transform_function(df: DataFrame, extra_param: str = "default_value") -> DataFrame:
        logger.info(f"Applying example_transform_function with extra_param='{extra_param}' to DataFrame shape {df.shape}")
        
        # Example: Add a new column
        if "new_transformed_column" not in df.columns:
            new_col_data = [f"{extra_param}_{i}" for i in range(df.shape[0])]
            df.add_column("new_transformed_column", new_col_data)

        # Example: Convert a column to uppercase if it exists
        if "product_name" in df.columns:
            df["product_name"] = df["product_name"].astype(lambda x: str(x).upper() if x is not None else None)
        
        logger.info(f"After transform, DataFrame shape {df.shape}, columns: {df.columns}")
        return df

    # --- Run the pipeline ---
    # You can choose to run with or without the transformation stage
    run_generic_etl_pipeline(
        tasks=example_tasks,
        transform_function=example_transform_function, # Pass the function itself
        transform_params={"extra_param": "ETL_RUN"},   # Pass any params for the function
        db_target=PATH_CONFIG["DEFAULT_OUTPUT_DB_PATH"], # Output DB
        db_table_prefix="etl_",      # Tables will be like "etl_products", "etl_cade_data"
        db_if_exists="replace",      # "replace", "append", or "fail"
        num_workers_hybrid_pool=max(1, (os.cpu_count() or 2) // 2), # Use half the cores for the pool for this example
        queue_capacity=20            # Smaller capacity for quicker testing of flow
    )

    # Example run without transformation
    # run_generic_etl_pipeline(
    #     tasks=example_tasks[:1], # Just one task for this run
    #     transform_function=None, 
    #     db_target="output_data/etl_no_transform.db",
    #     db_table_prefix="raw_",
    #     db_if_exists="replace"
    # )