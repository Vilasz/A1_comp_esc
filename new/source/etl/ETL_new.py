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

# --- gRPC database variables (for SQL extraction example) ---
root_dir = Path(__file__).resolve().parent.parent.parent
grpc_db_path = root_dir / 'gRPC' / 'data_messages.db'
db_expected_columns = [
    "cliente_id", "produto_id", "categoria_id", "produto", "quantidade",
    "preco_unitario", "valor_total", "data_pedido", "hora_pedido", "mes", "ano",
    "canal_venda", "centro_logistico_mais_proximo", "cidade_cliente",
    "estado_cliente", "dias_para_entrega",
]

from new.source.etl.data_generators import generate_csv as etl_generate_csv
from new.source.etl.data_generators import generate_json as etl_generate_json

# Removed ETL_POSTGRES_DB_CONFIG as SQL loading is removed

import os
import json

# --- Project-specific imports ---
from new.source.utils.dataframe import DataFrame
# Assuming RepoData might still be used for extraction from SQL, or if LoadStage was using it for non-SQL loads.
# If RepoData's SQL loading methods are exclusively for what LoadStage did, this could be refined.
from new.source.etl.ETL_utils import RepoData, MapMutex # Removed PostgresLoaderEtl if it was only for loading
from new.source.framework.using_threads import ThreadWrapper
from new.source.framework.hybrid_pool import HybridPool, CPU_LIMIT

LOG_FORMAT = "%(asctime)s [%(levelname)8s] [%(threadName)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


# --- Type Definitions for Queue Items ---
ExtractionTaskPayload = Dict[str, Any]
ExtractionTaskItem = Tuple[str, ExtractionTaskPayload]

# This item is the output of ExtractStage and input for TransformStage/OrderProcessingStage
ExtractedDataQueueItem = Tuple[str, DataFrame, ExtractionTaskPayload] # (task_id, df, original_payload)
ProcessedDataQueueItem = Tuple[str, Dict[str, DataFrame], ExtractionTaskPayload] # Output of OrderProcessingStage

DataFrameQueueItem = Tuple[str, DataFrame] # Used as output of TransformStage (for single DFs) and input for SinkStage


try:
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
    PATH_CONFIG = {
        "SIMULATOR_DATA_ROOT": _PROJECT_ROOT / "simulator_data",
        "DEFAULT_SQLITE_OUTPUT_DB_PATH": _PROJECT_ROOT / "output_data" / "etl_sqlite_output.db", # May be used by SQL extraction or data setup
    }
    # Ensure output directory for potential non-SQL outputs or logs exists
    (_PROJECT_ROOT / "output_data").mkdir(parents=True, exist_ok=True)

except Exception as e:
    logger.warning(f"Could not determine project paths based on __file__: {e}. Paths may need manual configuration.")
    PATH_CONFIG = {"SIMULATOR_DATA_ROOT": Path("."), "DEFAULT_SQLITE_OUTPUT_DB_PATH": Path("etl_sqlite_output.db")}
    Path("output_data").mkdir(parents=True, exist_ok=True)


# --- CPU-Bound Worker Functions (to be run in HybridPool) ---
def parse_file_chunk_worker(
    strategy_kind: str,
    file_path: Optional[Path], # File path might be None for non-file based sources (e.g. SQL query)
    chunk_definition: Dict[str, Any],
    repo_extraction_params: Dict[str, Any]
) -> Optional[DataFrame]:
    try:
        repo = RepoData()
        source_identifier = str(file_path) if file_path else repo_extraction_params.get("table_name", "SQL query")
        logger.debug(f"Worker: Parsing chunk for {source_identifier} using {strategy_kind}. Chunk def: {str(chunk_definition)[:100]}")

        if "lines_to_parse" in chunk_definition and (strategy_kind == RepoData.Kind.CSV or strategy_kind == RepoData.Kind.TXT) :
            lines = chunk_definition["lines_to_parse"]
            header = chunk_definition.get("header_for_chunk")

            if not lines: return DataFrame(columns=header or [])

            if header:
                df_chunk = DataFrame(columns=header)
                delim = repo_extraction_params.get("delimiter", ",")
                for line_str in lines:
                    values = [val.strip() for val in line_str.split(delim)]
                    if len(values) == len(header):
                        df_chunk.append(dict(zip(header, values)))
                    else:
                        logger.warning(f"Worker: Mismatched columns for line in chunk for {source_identifier}: {line_str[:50]} (expected {len(header)}, got {len(values)})")
            else:
                logger.warning(f"Worker: Chunk parsing for {source_identifier} without header. Creating DataFrame with auto-columns.")
                if lines:
                    delim = repo_extraction_params.get("delimiter", ",")
                    num_cols = len(lines[0].split(delim)) if lines[0] else 0
                    default_cols = [f"col_{i}" for i in range(num_cols)]
                    df_chunk = DataFrame(columns=default_cols)
                    for line_str in lines:
                        values = [val.strip() for val in line_str.split(delim)]
                        if len(values) == len(default_cols):
                             df_chunk.append(dict(zip(default_cols, values)))
                        else:
                            logger.warning(f"Worker: Mismatched columns for line in chunk (auto-cols) for {source_identifier}: {line_str[:50]}")
                else:
                    df_chunk = DataFrame()
            return df_chunk

        # Fallback to full source processing (e.g. whole file, SQL query)
        # Ensure 'path' is correctly passed for file-based, and connection params for SQL
        
        # Initialize parameters for the RepoData call
        # repo_extraction_params comes from ExtractStage.
        # For SQL, it contains 'db_conn_or_path'. For files, it has other specific params.
        # The 'file_path' argument to this worker is the resolved Path object for files, or None for SQL.
        final_call_params = {"kind": strategy_kind}
        # Start by adding all parameters passed from ExtractStage
        final_call_params.update(repo_extraction_params)

        if strategy_kind == RepoData.Kind.SQL:
            # For SQL strategy, RepoData.set_strategy_for_extraction expects 'path' for the database file.
            # In repo_extraction_params (from ExtractStage), this path is under 'db_conn_or_path'.
            if "db_conn_or_path" in final_call_params:
                # Rename 'db_conn_or_path' to 'path'
                final_call_params["path"] = final_call_params.pop("db_conn_or_path")
            elif "path" not in final_call_params:
                # If 'path' is still missing (neither 'path' nor 'db_conn_or_path' was present in repo_extraction_params)
                logger.error(
                    f"Worker: SQL strategy for table '{final_call_params.get('table_name', 'unknown table')}' "
                    f"is missing the database path. Params: {final_call_params}"
                )
                # This situation would lead to the TypeError you're encountering.
        elif file_path is not None: 
            # For file-based strategies (CSV, TXT, JSON, etc.),
            # the 'file_path' argument (which is a Path object) is the correct one to use.
            final_call_params["path"] = file_path
        elif "path" not in final_call_params:
            # This case means: not an SQL strategy, AND file_path argument is None, AND 'path' key is not already in params.
            # This would be an issue if a file-based strategy is called without a path.
            logger.error(
                f"Worker: Strategy {strategy_kind} is missing 'path'. "
                f"File_path arg was None. Params: {final_call_params}"
            )
            # This will likely also lead to an error if RepoData requires 'path'.

        # Now, final_call_params should have the 'path' key correctly populated.
        repo.set_strategy_for_extraction(**final_call_params)
        return repo.extract_data()

    except Exception as e:
        source_identifier_err = str(file_path) if file_path else repo_extraction_params.get("table_name", "SQL query")
        logger.error(f"Worker: Error parsing chunk/source for {source_identifier_err}: {e}", exc_info=True)
        return None

def transform_dataframe_chunk_worker(
    df_chunk: DataFrame,
    transform_function: Callable[[DataFrame], DataFrame],
    transform_params: Optional[Dict[str, Any]] = None
) -> Optional[DataFrame]:
    try:
        logger.debug(f"Worker: Transforming DataFrame chunk, shape {df_chunk.shape}")
        if transform_params:
            return transform_function(df_chunk, **transform_params)
        return transform_function(df_chunk)
    except Exception as e:
        logger.error(f"Worker: Error transforming DataFrame chunk: {e}", exc_info=True)
        return None

# --- ETL Pipeline Threads ---
# This is what ExtractStage will put on its output queue (data_for_processing_q)
ExtractStageOutputItem = Tuple[str, Tuple[DataFrame, ExtractionTaskPayload]]

class ExtractStage(ThreadWrapper[ExtractionTaskItem, Optional[ExtractedDataQueueItem]]): # Output type changed
    def __init__(
        self,
        in_queue: queue.Queue[ExtractionTaskItem],
        out_queue: queue.Queue[Optional[ExtractedDataQueueItem]], # Type of items in out_queue
        map_mutex: MapMutex,
        hybrid_pool: HybridPool,
        path_config: Dict[str, Path],
        default_chunk_size_lines: int = 100000,
        name: str = "ExtractorStage",
    ):
        super().__init__(in_queue, out_queue, name=name)
        self.map_mutex = map_mutex
        self.hybrid_pool = hybrid_pool
        self.path_config = path_config
        self.default_chunk_size_lines = default_chunk_size_lines

    # ... (_resolve_path_from_task, _split_file_into_line_chunk_definitions remain the same) ...

    def handle_item(self, task_id: str, task_payload: ExtractionTaskPayload) -> Optional[ExtractedDataQueueItem]: # Return type changed
        data_kind = task_payload.get("kind")
        file_path_str = task_payload.get("path")
        chunkable = task_payload.get("chunkable", False)
        current_file_path: Optional[Path] = None

        # --- Initial checks for data_kind and path (simplified for brevity, keep your original logic) ---
        if not data_kind:
            logger.error("Extractor: Task '%s' missing 'kind'.", task_id)
            return None # Item cannot be processed

        if data_kind not in [RepoData.Kind.SQL, "datacat_loader"]:
            if not file_path_str:
                logger.error(f"Extractor: Task '{task_id}' (kind: {data_kind}) missing 'path'.")
                return None
            current_file_path = self._resolve_path_from_task(file_path_str)
            if not current_file_path.exists():
                logger.error(f"Extractor: File not found for task '{task_id}': {current_file_path}")
                return None
        elif data_kind == "datacat_loader": # Simplified, ensure your full logic is retained
            if not file_path_str: # Placeholder for your datacat path logic
                logger.error(f"Extractor: DataCat task '{task_id}' missing 'path'.")
                return None
            current_file_path = self._resolve_path_from_task(file_path_str) # Should be a dir
        # ... (rest of path validation)

        source_display_name = str(current_file_path) if current_file_path else file_path_str or data_kind
        logger.info(f"Extractor: Processing '{task_id}' for source: {source_display_name}")

        # --- Specific handler for incremental 'datacat' ---
        if data_kind == "datacat_loader" and current_file_path:
            # ... (your existing datacat logic) ...
            # Ensure the final DataFrame (final_df) is packaged with task_payload
            # For example, if final_df is the combined DataFrame from datacat files:
            # return task_id, final_df, task_payload
            # This section needs to be adapted from your original to return the triple.
            # For now, assuming it produces 'final_df_datacat'
            # Example return for datacat (replace with your actual logic)
            dfs_from_new_files : List[DataFrame] = [] # Placeholder
            # ... (after your datacat processing that populates dfs_from_new_files) ...
            if not dfs_from_new_files:
                logger.info(f"Extractor (DataCat): No new files processed for {current_file_path}")
                return task_id, DataFrame(columns=task_payload.get("expected_columns", [])), task_payload
            final_df_datacat = dfs_from_new_files[0]
            if len(dfs_from_new_files) > 1:
                for i in range(1, len(dfs_from_new_files)):
                    final_df_datacat = final_df_datacat.concat(dfs_from_new_files[i])
            logger.info(f"Extractor (DataCat): Processed files, combined shape {final_df_datacat.shape}")
            return task_id, final_df_datacat, task_payload


        # --- General source processing (file, SQL, etc.) ---
        repo_extraction_params = {
            k: v for k, v in {
                "csv_delimiter": task_payload.get("csv_delimiter"),
                "csv_has_header": task_payload.get("csv_has_header", True),
                "txt_delimiter": task_payload.get("txt_delimiter", "|"),
                "txt_has_header": task_payload.get("txt_has_header", False),
                "json_records_path": task_payload.get("json_records_path"),
                "encoding": task_payload.get("encoding", "utf-8"),
                "db_conn_or_path": file_path_str,
                "table_name": task_payload.get("table_name"),
                "query": task_payload.get("query")
            }.items() if v is not None
        }

        # Chunking for file-based types (CSV/TXT)
        if chunkable and current_file_path and data_kind in [RepoData.Kind.CSV, RepoData.Kind.TXT]:
            # ... (your existing chunking logic) ...
            # Ensure the final DataFrame (final_df_chunked) is packaged with task_payload
            # Example: return task_id, final_df_chunked, task_payload
            # This section also needs to be adapted. For now, assuming it produces 'final_df_from_chunks'.
            # ... (after your chunk processing that creates 'final_df_from_chunks') ...
            # Example return for chunked files (replace with your actual logic)
            partial_dfs : List[DataFrame] = [] # Placeholder
            # ...
            if not partial_dfs:
                logger.warning(f"Extractor: All chunks failed for '{task_id}' on {current_file_path}")
                return task_id, DataFrame(columns=task_payload.get("expected_columns", [])), task_payload
            final_df_from_chunks = partial_dfs[0]
            if len(partial_dfs) > 1: # Corrected concat logic
                final_df_from_chunks = DataFrame.concat_dfs(partial_dfs) # Assuming static method for safety
            logger.info(f"Extractor: Reconstructed DataFrame from chunks for '{task_id}', shape {final_df_from_chunks.shape}")
            return task_id, final_df_from_chunks, task_payload

        # Fallback: Process whole source
        logger.info(f"Extractor: Processing whole source for task '{task_id}' ({data_kind})")
        try:
            future = self.hybrid_pool.submit(
                parse_file_chunk_worker, data_kind, current_file_path,
                {"is_full_file": True}, repo_extraction_params
            )
            df_result = future.result()

            if df_result is not None:
                logger.info(f"Extractor: Successfully processed task '{task_id}', DataFrame shape {df_result.shape}")
                # The item put on the queue will be (task_id, df_result, task_payload)
                # This will be handled by OrderProcessingStage or TransformStage based on payload or routing.
                return task_id, (df_result, task_payload)
            else:
                logger.warning(f"Extractor: Whole source processing returned None for task '{task_id}'")
                empty_df = DataFrame(columns=task_payload.get("expected_columns", []))
                return task_id, (empty_df, task_payload)
        except Exception as e:
            logger.error(f"Extractor: Error processing whole source for task '{task_id}': {e}", exc_info=True)
            empty_df_on_error = DataFrame(columns=task_payload.get("expected_columns", []))
            return task_id, (empty_df_on_error, task_payload)


# class TransformStage(ThreadWrapper[str, DataFrame]): # Input is DataFrameQueueItem: (task_id, DataFrame)
#     def __init__(
#         self,
#         in_queue: queue.Queue[DataFrameQueueItem], # Receives (task_id, df)
#         out_queue: queue.Queue[DataFrameQueueItem], # Sends (task_id, df_transformed)
#         hybrid_pool: HybridPool,
#         user_transform_function: Optional[Callable[[DataFrame, Any], DataFrame]] = None,
#         user_transform_params: Optional[Dict[str, Any]] = None,
#         default_chunk_size_rows: int = 50000,
#         name: str = "TransformerStage",
#     ):
#         super().__init__(in_queue, out_queue, name=name)
#         self.hybrid_pool = hybrid_pool
#         self.user_transform_function = user_transform_function
#         self.user_transform_params = user_transform_params or {}
#         self.default_chunk_size_rows = default_chunk_size_rows

#     def handle_item(self, task_id: str, df_input: DataFrame) -> Optional[DataFrameQueueItem]: # Output is (task_id, df_transformed)
#         if self.user_transform_function is None:
#             logger.debug(f"Transformer: No transform func for '{task_id}'. Passing through.")
#             return task_id, df_input

#         if df_input.empty:
#             logger.info(f"Transformer: Input DataFrame for '{task_id}' is empty. Passing through.")
#             return task_id, df_input
            
#         logger.info(f"Transformer: Applying transformation for '{task_id}' to DataFrame shape {df_input.shape}")
#         try:
#             if df_input.shape[0] <= self.default_chunk_size_rows * 1.5: # Heuristic
#                 transformed_df = transform_dataframe_chunk_worker(
#                     df_input, self.user_transform_function, self.user_transform_params
#                 )
#                 if transformed_df is None:
#                     logger.error(f"Transformer: Transformation returned None for '{task_id}'")
#                     return None 
#                 return task_id, transformed_df
#             else: # Chunk DataFrame for parallel transformation
#                 logger.info(f"Transformer: Chunking DataFrame for '{task_id}' (shape {df_input.shape})")
#                 df_chunks: List[DataFrame] = []
#                 for i in range(0, df_input.shape[0], self.default_chunk_size_rows):
#                     df_slice = df_input.slice(i, min(i + self.default_chunk_size_rows, df_input.shape[0]))
#                     if not df_slice.empty: df_chunks.append(df_slice)
                
#                 if not df_chunks: # Should not happen if df_input was not empty
#                     return task_id, df_input # Pass original if no chunks

#                 futures = [self.hybrid_pool.submit(transform_dataframe_chunk_worker, chunk, self.user_transform_function, self.user_transform_params) for chunk in df_chunks]
#                 transformed_dfs = [f.result() for f in as_completed(futures) if f.result() is not None and not f.result().empty]
                
#                 if not transformed_dfs:
#                     logger.error(f"Transformer: All transform chunks failed or empty for '{task_id}'")
#                     return None

#                 final_df = transformed_dfs[0]
#                 if len(transformed_dfs) > 1:
#                     for i in range(1, len(transformed_dfs)): final_df = final_df.concat(transformed_dfs[i])
#                 return task_id, final_df
#         except Exception as e:
#             logger.error(f"Transformer: Error during transformation for '{task_id}': {e}", exc_info=True)
#             return None

TransformStageInputData = Tuple[DataFrame, ExtractionTaskPayload]

class TransformStage(ThreadWrapper[str, TransformStageInputData]): # Input and Output types updated
    def __init__(
        self,
        in_queue: queue.Queue[Optional[ExtractedDataQueueItem]], # Receives (task_id, df, payload)
        out_queue: queue.Queue[Optional[DataFrameQueueItem]],   # Sends (task_id, df_transformed)
        hybrid_pool: HybridPool,
        user_transform_function: Optional[Callable[..., DataFrame]] = None, # Can be (df, **params) or ((df1, df2), **params)
        user_transform_params: Optional[Dict[str, Any]] = None,
        default_chunk_size_rows: int = 50000,
        name: str = "TransformerStage",
    ):
        super().__init__(in_queue, out_queue, name=name)
        self.hybrid_pool = hybrid_pool
        self.user_transform_function = user_transform_function
        self.user_transform_params = user_transform_params or {}
        self.default_chunk_size_rows = default_chunk_size_rows
        # For storing DataFrames that are part of a pair, waiting for their counterpart
        self.pending_pairs: Dict[str, Dict[str, DataFrame]] = {} # Key: pair_id, Value: {"member_role": DataFrame}

    def handle_item(self, task_id: str, data_for_transform: TransformStageInputData) -> Optional[DataFrameQueueItem]:
        df_input, task_payload = data_for_transform

        if self.user_transform_function is None:
            logger.debug(f"Transformer ({self.name}): No transform func for '{task_id}'. Passing through.")
            return task_id, df_input

        if df_input.empty: # An individual df_input might be empty, but if it's part of a pair, the other might not be.
                           # This check is for df_input from (df_input, task_payload).
            logger.info(f"Transformer ({self.name}): Input DataFrame for '{task_id}' is empty. Processing accordingly.")
            # If it's part of a pair, it will be stored as empty and the pair logic will handle it.
            # If it's for a single DF transform, it will likely pass through or be handled by that logic.

        pair_id = task_payload.get("pair_transform_id")
        member_role = task_payload.get("pair_member")

        # Determine if the current user_transform_function is intended for paired DataFrames.
        # This allows flexibility if different transform functions are passed to the pipeline.
        # Your multi-step function is named 'transform_function' in the __main__ scope.
        function_is_for_pairs = False
        if hasattr(self.user_transform_function, '__name__'):
            # Add names of functions that specifically take a tuple of DataFrames (paired data)
            # This list should contain the names of functions designed to accept (df1, df2) as first arg.
            pair_handling_function_names = ['agrupa_dados', 'transform_function']
            if self.user_transform_function.__name__ in pair_handling_function_names:
                function_is_for_pairs = True

        if pair_id and member_role: # Task is configured for pairing
            if function_is_for_pairs:
                if df_input.empty:
                     logger.info(f"Transformer ({self.name}): Received empty DataFrame for member '{member_role}' of pair '{pair_id}'. Storing.")
                
                if pair_id not in self.pending_pairs:
                    self.pending_pairs[pair_id] = {}
                self.pending_pairs[pair_id][member_role] = df_input
                logger.info(f"Transformer ({self.name}): Stored member '{member_role}' (shape {df_input.shape if df_input is not None else 'None'}) for pair '{pair_id}'. "
                            f"Current members: {list(self.pending_pairs[pair_id].keys())}")

                # Define expected roles for the pair. These should match your task definitions.
                expected_role1 = "orders_history"
                expected_role2 = "new_orders"

                if expected_role1 in self.pending_pairs[pair_id] and expected_role2 in self.pending_pairs[pair_id]:
                    df1 = self.pending_pairs[pair_id][expected_role1]
                    df2 = self.pending_pairs[pair_id][expected_role2]
                    
                    logger.info(f"Transformer ({self.name}): Both members for pair '{pair_id}' received. "
                                f"DF1 shape: {df1.shape if df1 else 'None'}, DF2 shape: {df2.shape if df2 else 'None'}. "
                                f"Applying paired transformation using '{self.user_transform_function.__name__}'.")
                    try:
                        # The user_transform_function (e.g., your multi-step one) expects a tuple (df1, df2)
                        transformed_df = self.user_transform_function((df1, df2), **self.user_transform_params)
                        del self.pending_pairs[pair_id] # Clean up after processing

                        if transformed_df is None:
                            logger.warning(f"Transformer ({self.name}): Paired transformation for '{pair_id}' by '{self.user_transform_function.__name__}' returned None. Ensure this is intended.")
                            # If the sink expects a DataFrame, this could be an issue.
                            # However, your modified transform_function should return top_5_products.
                            return None # Propagate None if that's the actual result.
                        
                        logger.info(f"Transformer ({self.name}): Paired transformation for '{pair_id}' complete. Output shape: {transformed_df.shape}")
                        return pair_id, transformed_df # Use pair_id as the task_id for the combined result
                    except Exception as e:
                        logger.error(f"Transformer ({self.name}): Error during paired transformation for '{pair_id}' using '{self.user_transform_function.__name__}': {e}", exc_info=True)
                        if pair_id in self.pending_pairs: # Ensure cleanup on error
                            del self.pending_pairs[pair_id]
                        return None
                else:
                    logger.debug(f"Transformer ({self.name}): Pair '{pair_id}' not yet complete. Waiting for all members.")
                    return None # Pair not yet complete, item consumed but no output to next stage yet
            else: # Task is for pairing, but the configured transform_function is not for pairs
                logger.warning(f"Transformer ({self.name}): Task '{task_id}' (payload pair_id: {pair_id}) is for pairing, "
                               f"but the configured function '{self.user_transform_function.__name__}' is not designated for paired data. Passing through original DataFrame for task_id '{task_id}'.")
                return task_id, df_input # Pass through the current df_input associated with this task_id

        elif not pair_id: # Task is NOT configured for pairing (it's a single DataFrame input for the transform stage)
            if not function_is_for_pairs: # And the function is suitable for single DataFrames
                if df_input.empty: # Check again specifically for single DF transforms
                    logger.info(f"Transformer ({self.name}): Input DataFrame for single transform task '{task_id}' is empty. Passing through.")
                    return task_id, df_input

                logger.info(f"Transformer ({self.name}): Applying single-DataFrame transformation for '{task_id}' to DataFrame shape {df_input.shape} using '{self.user_transform_function.__name__}'")
                try:
                    # Chunking logic for single DataFrame transformations (ensure this matches your DataFrame capabilities)
                    if df_input.shape[0] <= self.default_chunk_size_rows * 1.5: # Heuristic for small DFs
                        transformed_df = transform_dataframe_chunk_worker(
                            df_input, self.user_transform_function, self.user_transform_params
                        )
                        if transformed_df is None:
                            logger.error(f"Transformer ({self.name}): Single DF transformation returned None for '{task_id}'")
                            return None # Or return original df_input if preferred on transform failure
                        return task_id, transformed_df
                    else: # Chunk DataFrame for parallel transformation
                        logger.info(f"Transformer ({self.name}): Chunking DataFrame for '{task_id}' (shape {df_input.shape}) for single-DF transform")
                        df_chunks: List[DataFrame] = []
                        for i in range(0, df_input.shape[0], self.default_chunk_size_rows):
                            df_slice = df_input.slice(i, min(i + self.default_chunk_size_rows, df_input.shape[0]))
                            if not df_slice.empty: df_chunks.append(df_slice)
                        
                        if not df_chunks:
                            logger.warning(f"Transformer ({self.name}): No chunks created for single DF transform of '{task_id}'. Passing original.")
                            return task_id, df_input

                        futures = [self.hybrid_pool.submit(transform_dataframe_chunk_worker, chunk, self.user_transform_function, self.user_transform_params) for chunk in df_chunks]
                        transformed_dfs = [f.result() for f in as_completed(futures) if f.result() is not None and not f.result().empty]
                        
                        if not transformed_dfs:
                            logger.error(f"Transformer ({self.name}): All transform chunks failed or returned empty for single DF task '{task_id}'")
                            return None # Or return original df_input

                        # Assuming DataFrame.concat_dfs is a static method that can handle a list of DataFrames
                        final_df = DataFrame.concat_dfs(transformed_dfs)
                        logger.info(f"Transformer ({self.name}): Recombined {len(transformed_dfs)} chunks for single DF task '{task_id}'. Output shape: {final_df.shape}")
                        return task_id, final_df
                except Exception as e:
                    logger.error(f"Transformer ({self.name}): Error during single-DataFrame transformation for '{task_id}': {e}", exc_info=True)
                    return None # Or return original df_input
            else: # Task is single DF, but the configured transform_function IS for pairs (mismatch)
                logger.warning(f"Transformer ({self.name}): Task '{task_id}' is single-input, but the configured function "
                                f"'{self.user_transform_function.__name__}' is designated for paired data. Passing through.")
                return task_id, df_input
        else:
            # This case should ideally not be reached if the logic for pair_id presence/absence is exhaustive.
            logger.error(f"Transformer ({self.name}): Unhandled case for task '{task_id}' with payload {task_payload}. "
                        f"Function is '{self.user_transform_function.__name__ if self.user_transform_function else 'None'}', "
                        f"function_is_for_pairs: {function_is_for_pairs}. Passing through.")
            return task_id, df_input


class OrderProcessingStage(ThreadWrapper[str, Union[DataFrame, Dict[str, DataFrame]]]): # Input is ExtractedDataQueueItem
    def __init__(
        self,
        in_queue: queue.Queue[ExtractedDataQueueItem], 
        out_queue: queue.Queue[ProcessedDataQueueItem], # Sends (task_id, {'pedidos': df, 'itens': df}, task_payload)
        hybrid_pool: HybridPool,
        ecommerce_db_path: Path, # Path to SQLite DB for get_last_pk etc.
        default_chunk_size_rows: int = 10000,
        name: str = "OrderProcessingStage",
    ):
        super().__init__(in_queue, out_queue, name=name)
        self.hybrid_pool = hybrid_pool
        self.ecommerce_db_path = ecommerce_db_path
        self.default_chunk_size_rows = default_chunk_size_rows
        self._id_lock = threading.Lock() 
        self._ecommerce_conn_for_ids: Optional[sqlite3.Connection] = None

        if not ORDERS_LOGIC_AVAILABLE:
            logger.error(f"{self.name} created, but orders_ingest.py logic is unavailable.")

    def _get_reusable_ecommerce_connection(self) -> sqlite3.Connection:
        if self._ecommerce_conn_for_ids is None or self._is_connection_closed(self._ecommerce_conn_for_ids):
            try:
                self._ecommerce_conn_for_ids = get_db_connection(self.ecommerce_db_path)
            except Exception as e:
                logger.error(f"{self.name}: Failed to get e-commerce DB connection: {e}", exc_info=True)
                raise
        return self._ecommerce_conn_for_ids

    def _is_connection_closed(self, conn: sqlite3.Connection) -> bool:
        try:
            conn.execute("SELECT 1").fetchone()
            return False
        except (sqlite3.ProgrammingError, sqlite3.OperationalError): return True
        except Exception: return True

    # item is (task_id, df_raw_orders, task_payload)
    def handle_item(self, task_id: str, df_raw_orders: DataFrame, task_payload: ExtractionTaskPayload) -> Optional[ProcessedDataQueueItem]:
        if not ORDERS_LOGIC_AVAILABLE:
            logger.error(f"{self.name}: Orders logic unavailable, skipping '{task_id}'.")
            return task_id, {'pedidos': DataFrame(columns=["error"]), 'itens': DataFrame()}, task_payload

        # This stage only processes tasks explicitly marked for it.
        if task_payload.get("processing_type") != "ecommerce_new_orders":
            # This case should ideally be filtered before reaching this queue or ExtractStage should route correctly.
            logger.warning(f"{self.name}: Received task '{task_id}' not for e-commerce order processing. Discarding.")
            return None 

        if df_raw_orders.empty:
            logger.info(f"{self.name}: Raw orders DataFrame for '{task_id}' is empty.")
            return task_id, {'pedidos': DataFrame(), 'itens': DataFrame()}, task_payload

        logger.info(f"{self.name}: Processing {df_raw_orders.shape[0]} raw orders for '{task_id}'.")
        try:
            with self._id_lock:
                conn = self._get_reusable_ecommerce_connection()
                start_pedido_id = get_last_pk(conn, "pedidos") + 1
                start_item_id = get_last_pk(conn, "itens_pedido") + 1
            
            num_chunks = math.ceil(df_raw_orders.shape[0] / self.default_chunk_size_rows)
            futures: List[Future] = []
            pedido_offset, item_offset = 0, 0

            for i in range(num_chunks):
                chunk_df = df_raw_orders.slice(i * self.default_chunk_size_rows, (i + 1) * self.default_chunk_size_rows)
                if chunk_df.empty: continue

                fut = self.hybrid_pool.submit(
                    transform_new_orders_worker, chunk_df, self.ecommerce_db_path, 
                    start_pedido_id + pedido_offset, start_item_id + item_offset
                )
                futures.append(fut)
                pedido_offset += chunk_df.shape[0]
                item_offset += chunk_df.shape[0] # Assumes 1 item_pedido per raw order row

            all_pedidos_dfs, all_itens_dfs = [], []
            for fut in as_completed(futures):
                try:
                    df_p, df_i = fut.result()
                    if df_p is not None and not df_p.empty: all_pedidos_dfs.append(df_p)
                    if df_i is not None and not df_i.empty: all_itens_dfs.append(df_i)
                except Exception as e_fut:
                    logger.error(f"{self.name}: Error in order processing worker for '{task_id}': {e_fut}", exc_info=True)

            final_pedidos_df = DataFrame.concat_dfs(all_pedidos_dfs) if all_pedidos_dfs else DataFrame()
            final_itens_df = DataFrame.concat_dfs(all_itens_dfs) if all_itens_dfs else DataFrame()
            
            logger.info(f"{self.name}: Order processing for '{task_id}' done. Pedidos: {final_pedidos_df.shape}, Itens: {final_itens_df.shape}")
            return task_id, {'pedidos': final_pedidos_df, 'itens': final_itens_df}, task_payload
        except Exception as e:
            logger.error(f"{self.name}: Critical error processing orders for '{task_id}': {e}", exc_info=True)
            return task_id, {'pedidos': DataFrame(), 'itens': DataFrame()}, task_payload


class SinkStage(ThreadWrapper[str, Any]): # Consumes various types of data from previous stages
    def __init__(self, in_queue: queue.Queue, name: str = "SinkStage"):
        super().__init__(in_queue, out_queue=None, name=name) # No out_queue

    def handle_item(self, task_id: str, data_item: Any) -> None:
        item_description = "Unknown item type"
        if isinstance(data_item, DataFrame):
            item_description = f"DataFrame with shape {data_item.shape}"
        elif isinstance(data_item, dict): # e.g., from OrderProcessingStage
            parts = []
            for key, val in data_item.items():
                if isinstance(val, DataFrame):
                    parts.append(f"'{key}': DataFrame shape {val.shape}")
                else:
                    parts.append(f"'{key}': type {type(val).__name__}")
            item_description = f"Dict with items: {'; '.join(parts)}"
        
        logger.info(f"{self.name}: Received and processed data for task '{task_id}'. Item: {item_description}. This is the end of the pipeline for this item.")
        # In a real scenario, this stage might write to a non-SQL sink, a message queue, or perform final checks.
        return None


def setup_etl_benchmark_data(sim_root: Path, regenerate: bool = False):
    logger.info(f"Setting up benchmark data in {sim_root}")
    sim_root.mkdir(parents=True, exist_ok=True)

    (sim_root / "contaverde").mkdir(parents=True, exist_ok=True)
    etl_csv_path = sim_root / "contaverde" / "products_benchmark.txt"
    if regenerate or not etl_csv_path.exists():
        logger.info(f"Generating CSV: {etl_csv_path}")
        etl_generate_csv(n_rows=1000, out_path=etl_csv_path)

    orders_json_path = sim_root / "orders_benchmark.json"
    if regenerate or not orders_json_path.exists():
        logger.info(f"Generating JSON: {orders_json_path}")
        # Ensure this JSON structure matches what your "orders_benchmark" task expects (e.g. a root key)
        dummy_json_data = {"pedidos": [{"id": i, "val": i*100} for i in range(500)]}
        with open(orders_json_path, 'w') as f:
            json.dump(dummy_json_data, f)

    (sim_root / "cadeanalytics").mkdir(parents=True, exist_ok=True)
    etl_txt_path = sim_root / "cadeanalytics" / "cade_benchmark.txt"
    if regenerate or not etl_txt_path.exists():
        logger.info(f"Generating TXT: {etl_txt_path}")
        etl_generate_csv(n_rows=300, out_path=etl_txt_path, delimiter='|')

    datacat_dir = sim_root / "datacat_benchmark" / "behaviour"
    datacat_dir.mkdir(parents=True, exist_ok=True)
    if regenerate or not list(datacat_dir.glob("*.txt")):
        logger.info(f"Generating DataCat files in {datacat_dir}")
        for i in range(2):
            etl_generate_csv(n_rows=100, out_path=datacat_dir / f"events_log_bench_file{i+1}.txt", delimiter='|', has_header=False)


# --- Main ETL Orchestration ---
def run_generic_etl_pipeline(
    tasks: List[ExtractionTaskItem],
    transform_function: Optional[Callable[..., DataFrame]] = None, # Adjusted type hint for flexibility
    transform_params: Optional[Dict[str, Any]] = None,
    num_workers_hybrid_pool: int = min(CPU_LIMIT, (os.cpu_count() or 1)),
    queue_capacity: int = 100,
    ecommerce_db_path_for_orders: Optional[Path] = None):
    logger.info("Starting Generic ETL Pipeline...")
    pipeline_start_time = time.perf_counter()

    extract_tasks_q = queue.Queue[ExtractionTaskItem](maxsize=queue_capacity)
    # This queue will hold items from Extractor: (task_id, df, original_task_payload)
    data_for_processing_q = queue.Queue[Optional[ExtractStageOutputItem]](maxsize=queue_capacity) # Type updated
    
    # Output from Transformer or OrderProcessor
    # Transformer outputs DataFrameQueueItem: (task_id, df)
    # OrderProcessor outputs ProcessedDataQueueItem: (task_id, {'pedidos': df, 'itens': df}, payload)
    # Sink consumes Any.
    final_data_q = queue.Queue[Any](maxsize=queue_capacity) # Remains Any for flexibility

    map_mutex = MapMutex[str]()
    hybrid_pool = HybridPool(limit=num_workers_hybrid_pool)

    extractor = ExtractStage(
        extract_tasks_q,
        data_for_processing_q, # Extractor outputs ExtractedDataQueueItem here
        map_mutex, hybrid_pool, PATH_CONFIG
    )

    order_processor = None
    # Assuming task_payload is a dictionary, and ExtractionTaskItem is a tuple like (id, payload)
    has_order_processing_task = any(task_item[1].get("processing_type") == "ecommerce_new_orders" for task_item in tasks if isinstance(task_item, tuple) and len(task_item) > 1 and isinstance(task_item[1], dict))
    if ORDERS_LOGIC_AVAILABLE and has_order_processing_task:
        if ecommerce_db_path_for_orders:
            order_processor = OrderProcessingStage(
                data_for_processing_q, # Consumes ExtractedDataQueueItem
                final_data_q,
                hybrid_pool, ecommerce_db_path_for_orders
            )
        # ... (else warning) ...
        else:
            logger.info("Order processing task detected, but ecommerce_db_path_for_orders not provided.")


    transformer_thread = None
    if transform_function:
        transformer_thread = TransformStage(
            data_for_processing_q, # Consumes ExtractedDataQueueItem
            final_data_q,          # Outputs DataFrameQueueItem
            hybrid_pool, transform_function, transform_params
        )
    # ... (rest of the pipeline setup, start, wait, stop logic remains largely the same) ...
    sink = SinkStage(final_data_q)

    # --- Start Threads ---
    extractor.start()
    if order_processor: order_processor.start()
    if transformer_thread: transformer_thread.start()
    sink.start()

    # --- Enqueue Initial Tasks ---
    for task_item in tasks: # Assuming tasks is a list of ExtractionTaskItem
        # Ensure task_item is in the expected format (task_id, task_payload)
        if isinstance(task_item, tuple) and len(task_item) == 2:
            task_id, task_payload = task_item
            logger.debug(f"Enqueuing extraction task: {task_id} with payload: {task_payload}")
            extract_tasks_q.put(task_item) # Put the whole ExtractionTaskItem
        else:
            logger.info(f"Skipping malformed task item: {task_item}")


    # ... (Wait for Pipeline Completion - your existing logic should be okay) ...
    # Example: Wait for queues to be empty and workers to finish
    extract_tasks_q.join() # If tasks are added and then task_done() is called by consumer
    data_for_processing_q.join()
    final_data_q.join()

    # ... (Stop Threads - your existing logic should be okay) ...
    # Signal end of data by putting None (sentinel values)
    # This needs to be handled by each stage's run loop
    logger.info("Sending sentinel to data_for_processing_q for Extractor completion.")
    data_for_processing_q.put(None) # For transformer or order_processor

    if order_processor and not transform_function: # If only order_processor consumes from data_for_processing_q
        pass # Sentinel already sent
    elif transform_function and not order_processor: # If only transformer consumes
        pass # Sentinel already sent
    elif transform_function and order_processor:
        # This case is complex: data_for_processing_q might be read by either.
        # The current structure suggests ExtractedDataQueueItem goes to EITHER OrderProcessor OR Transformer,
        # based on some internal logic or item type, or one of them is conditional.
        # If they both can run and consume from the same queue, you'd need two sentinels
        # or a more robust way to signal them.
        # For simplicity, assuming only one path is active for a given item, or the sentinel is handled correctly.
        logger.info("Sending additional sentinel to data_for_processing_q if both transformer and order_processor could run (review logic).")
        # data_for_processing_q.put(None) # Potentially needed if they can operate in parallel on different items

    logger.info("Sending sentinel to final_data_q for Transformer/OrderProcessor completion.")
    final_data_q.put(None) # For sink

    # Wait for threads to actually finish after processing sentinels
    # extractor.join() # If your stages are Threads and implement join()
    # if order_processor: order_processor.join()
    # if transformer_thread: transformer_thread.join()
    # sink.join()

    logger.info("Shutting down Hybrid Pool...")
    hybrid_pool.shutdown()

    pipeline_end_time = time.perf_counter()
    logger.info(f"ETL Pipeline execution time: {pipeline_end_time - pipeline_start_time:.2f} seconds")
    return pipeline_end_time - pipeline_start_time


def agrupa_dados(data: tuple[DataFrame, DataFrame], **kwargs) -> DataFrame:
    df_orders_history, df_new_orders = data
    
    print(30 * "=")
    print(f"Current state of 'pedidos' table")
    print(df_orders_history)
    print(30 * "=")
    
    df_orders_history.vstack(df_new_orders)    

    return df_orders_history

def agrupa_por_produto(df: DataFrame) -> DataFrame:
    """
    Retorna um novo DataFrame com a soma das quantidades de cada produto.
    """
    coluna_produto = "produto"
    coluna_quantidade = "quantidade"
    df_agrupado = df.group_by(coluna_produto, sum, [coluna_quantidade])
    return df_agrupado

def pega_top_5(df: DataFrame) -> DataFrame:
    """
    Retorna um novo DataFrame com os 5 produtos mais vendidos,
    baseado na soma da coluna de quantidade.
    """
    coluna_produto = "produto"
    coluna_quantidade = "quantidade"
    df_ordenado = df.sort_by(f"sum({coluna_quantidade})", ascending=False)
    top_5 = df_ordenado.rows[:5]
    return DataFrame(df_ordenado.columns, top_5)

def print_top_5(df: DataFrame):
    """
    Imprime no terminal um ranking dos 5 produtos mais vendidos.
    
    Entradas:
        df (DataFrame): DataFrame com os top 5 produtos e suas quantidades vendidas.
    """
    coluna_produto = "produto"
    coluna_quantidade = "sum(quantidade)"
    dados = df.to_dicts()
    
    print("\n📊 TOP 5 PRODUTOS MAIS VENDIDOS\n")
    print(f"{'RANK':<5} | {'PRODUTO':<15} | {'QUANTIDADE':>10}")
    print("-" * 40)
    
    for i, item in enumerate(dados, start=1):
        produto = item[coluna_produto]
        quantidade = item[coluna_quantidade]
        print(f"{i:<5} | {produto:<15} | {quantidade:>10}")
    print("-" * 40)
    
    # Since this is the last stage, it's okay to return None
    # But for consistency, you could return the data as is
    # return df

if __name__ == "__main__":
    cli_parser = argparse.ArgumentParser(description="Run Generic ETL Pipeline (No SQL Loading).")
    cli_parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) // 2), help="Number of workers for HybridPool.")
    cli_parser.add_argument("--setup-data-etl", action="store_true", help="Generate benchmark data and exit.")
    cli_args = cli_parser.parse_args()

    sim_data_root = PATH_CONFIG.get("SIMULATOR_DATA_ROOT", Path("simulator_data"))
    sim_data_root.mkdir(parents=True, exist_ok=True)

    # E-commerce DB path for OrderProcessingStage (if used)
    # This is an example, point it to your actual e-commerce DB for reading PKs
    ecommerce_orders_db_file = sim_data_root / "internal_ecommerce_data.db"


    if cli_args.setup_data_etl:
        print(f"Setting up benchmark data in {sim_data_root}...")
        setup_etl_benchmark_data(sim_data_root, regenerate=True)
        
        # Also setup a dummy ecommerce_orders_db_file if it's part of setup
        if not ecommerce_orders_db_file.exists() and ORDERS_LOGIC_AVAILABLE:
             logger.info(f"Creating dummy e-commerce DB for OrderProcessingStage at {ecommerce_orders_db_file}")
             try:
                conn = get_db_connection(ecommerce_orders_db_file) # Uses your import
                cursor = conn.cursor()
                # Ensure tables 'pedidos' and 'itens_pedido' exist for get_last_pk
                cursor.execute("CREATE TABLE IF NOT EXISTS pedidos (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)")
                cursor.execute("CREATE TABLE IF NOT EXISTS itens_pedido (id INTEGER PRIMARY KEY AUTOINCREMENT, pedido_id INTEGER, produto TEXT)")
                conn.commit()
                conn.close()
             except Exception as e_db_setup:
                 logger.error(f"Failed to setup dummy e-commerce DB: {e_db_setup}")
        print("Benchmark data setup complete. Exiting.")
        exit()

    # Ensure dummy files and gRPC DB for extraction tasks exist
    (sim_data_root / "contaverde").mkdir(parents=True, exist_ok=True)
    if not (sim_data_root / "contaverde" / "products.txt").exists():
        with open(sim_data_root / "contaverde" / "products.txt", "w") as f: f.write("id,name,cat\n1,Book,A\n2,Pen,B\n")
    
    (sim_data_root / "cadeanalytics").mkdir(parents=True, exist_ok=True)
    if not (sim_data_root / "cadeanalytics" / "cade_data.txt").exists():
        with open(sim_data_root / "cadeanalytics" / "cade_data.txt", "w") as f: f.write("id|data\n10|abc\n20|xyz\n")

    grpc_db_path.parent.mkdir(parents=True, exist_ok=True)
    if not grpc_db_path.exists():
        logger.info(f"Creating dummy SQLite DB for gRPC pedidos task at {grpc_db_path}")
        conn_grpc = sqlite3.connect(grpc_db_path)
        cursor_grpc = conn_grpc.cursor()
        cols_str = ", ".join([f'"{col_name}" TEXT' for col_name in db_expected_columns]) # Quote column names
        cursor_grpc.execute(f"CREATE TABLE IF NOT EXISTS pedidos ({cols_str})")
        sample_row = ["c1","p1","cat1","ProdX","1","10.0","10.0","2023-01-01","12:00","1","2023","web","loc1","City","ST","2"]
        if len(sample_row) == len(db_expected_columns):
            placeholders = ",".join(["?"] * len(db_expected_columns))
            cursor_grpc.execute(f"INSERT INTO pedidos VALUES ({placeholders})", sample_row)
        conn_grpc.commit()
        conn_grpc.close()
    
    # Dummy e-commerce DB for OrderProcessingStage's get_last_pk
    if not ecommerce_orders_db_file.exists() and ORDERS_LOGIC_AVAILABLE:
        logger.info(f"Creating dummy e-commerce DB for OrderProcessingStage at {ecommerce_orders_db_file}")
        try:
            conn_ecom = get_db_connection(ecommerce_orders_db_file)
            cursor_ecom = conn_ecom.cursor()
            cursor_ecom.execute("CREATE TABLE IF NOT EXISTS pedidos (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)")
            cursor_ecom.execute("CREATE TABLE IF NOT EXISTS itens_pedido (id INTEGER PRIMARY KEY AUTOINCREMENT, pedido_id INTEGER, produto TEXT)")
            # Optionally insert some dummy data so get_last_pk doesn't start from -1 or 0 if table is empty
            cursor_ecom.execute("INSERT INTO pedidos (data) VALUES ('dummy_order_data')")
            cursor_ecom.execute("INSERT INTO itens_pedido (pedido_id, produto) VALUES (1, 'dummy_item')")
            conn_ecom.commit()
            conn_ecom.close()
        except Exception as e_db_main:
            logger.error(f"Failed to create/setup dummy e-commerce DB in main: {e_db_main}")


    paired_sql_tasks: List[ExtractionTaskItem] = [
        ("sql_orders_history_for_agrupa", {
            "kind": RepoData.Kind.SQL, "path": str(grpc_db_path),
            "table_name": "pedidos", "expected_columns": db_expected_columns,
            "pair_transform_id": "agrupa_pedidos_main", # Unique ID for this pair operation
            "pair_member": "orders_history"            # Role of this DataFrame in the pair
        }),
        ("sql_new_orders_for_agrupa", { # Extracts the same table
            "kind": RepoData.Kind.SQL, "path": str(grpc_db_path),
            "table_name": "pedidos", "expected_columns": db_expected_columns,
            "pair_transform_id": "agrupa_pedidos_main", # Must match the other member's pair_id
            "pair_member": "new_orders"                # Role of this DataFrame
        }),
        # You can add other non-paired tasks as well
        # ("another_task_id", {"kind": "CSV", "path": "some_file.csv"}),
    ]

    # Your original example_transform_function (for single DFs)
    def example_transform_function_single_df(df: DataFrame, extra_param: str = "default_value") -> DataFrame:
        logger.info(f"SINGLE DF TRANSFORM: DF shape {df.shape} with param '{extra_param}'")
        if not df.empty and "transformed_col" not in df.columns:
            df.add_column("transformed_col", [f"tr_{i}" for i in range(df.shape[0])])
        return df

    def transform_function(data: tuple[DataFrame, DataFrame], **kwargs) -> DataFrame: # Added return type hint
        """
        Processes paired DataFrame data through grouping, aggregation, top-N selection, and printing.
        Returns the DataFrame containing the top N products.
        """
        logger.info(f"Executing multi-step transform_function with data: DF1 shape {data[0].shape if data[0] else 'None'}, DF2 shape {data[1].shape if data[1] else 'None'}")
        
        grouped_data = agrupa_dados(data) # agrupa_dados already returns a DataFrame
        
        if not isinstance(grouped_data, DataFrame) or grouped_data.empty:
            logger.warning("Initial grouping in multi-step transform resulted in empty or invalid DataFrame. Skipping further product aggregation.")
            # Depending on desired behavior, you might return grouped_data or an empty DataFrame with expected schema
            return grouped_data if isinstance(grouped_data, DataFrame) else DataFrame(columns=["produto", "sum(quantidade)"]) # Example schema

        grouped_by_product = agrupa_por_produto(grouped_data)
        
        if grouped_by_product.empty:
            logger.warning("Grouping by product resulted in an empty DataFrame. Skipping top_5.")
            return grouped_by_product

        top_5_products = pega_top_5(grouped_by_product)
        print_top_5(top_5_products) # Printing is a side effect

        return top_5_products # Return the final DataFrame

    logger.info("Running ETL with paired transform for 'agrupa_dados'...")
    run_generic_etl_pipeline(
        tasks=paired_sql_tasks, # Use the tasks defined for pairing
        # transform_function=agrupa_dados, # Pass agrupa_dados directly
        transform_function=agrupa_dados, # Pass agrupa_dados directly
        transform_params={}, # agrupa_dados (as modified) can take kwargs, but doesn't need specific ones here
        num_workers_hybrid_pool=cli_args.workers,
        ecommerce_db_path_for_orders=ecommerce_orders_db_file if ORDERS_LOGIC_AVAILABLE else None
    )

    def example_transform_function(df: DataFrame, extra_param: str = "default_value") -> DataFrame:
        logger.info(f"Transforming DF shape {df.shape} with param '{extra_param}'")
        if not df.empty and "transformed_col" not in df.columns:
            df.add_column("transformed_col", [f"tr_{i}" for i in range(df.shape[0])])
        return df
