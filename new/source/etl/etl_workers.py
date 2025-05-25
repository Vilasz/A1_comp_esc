import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable

from new.source.utils.dataframe import DataFrame
from new.source.etl.ETL_utils import RepoData # Assuming path

logger = logging.getLogger(__name__)

def parse_file_chunk_worker(
    strategy_kind: str,
    file_path: Path,
    chunk_definition: Dict[str, Any], # e.g., {'lines': ['line1', 'line2']} or {'offset': N, 'bytes': M}
    repo_extraction_params: Dict[str, Any] # e.g., delimiter, header_in_chunk, records_path
) -> Optional[DataFrame]:
    """
    Worker function to parse a defined chunk of a file.
    """
    try:
        repo = RepoData() # Each worker gets its own instance
        logger.debug(f"Worker: Parsing chunk for {file_path} using {strategy_kind}. Chunk def: {str(chunk_definition)[:100]}")

        if "lines_to_parse" in chunk_definition and (strategy_kind == RepoData.Kind.CSV or strategy_kind == RepoData.Kind.TXT) :
            lines = chunk_definition["lines_to_parse"]
            header = repo_extraction_params.get("header_for_chunk")
            
            if not lines: return DataFrame(columns=header or [])
            
            if header:
                df_chunk = DataFrame(columns=header)
                delim = repo_extraction_params.get("delimiter", ",")
                for line_str in lines:
                    values = [val.strip() for val in line_str.split(delim)]
                    if len(values) == len(header):
                        df_chunk.append(dict(zip(header, values)))
                    else:
                        logger.warning(f"Worker: Mismatched columns for line in chunk: {line_str[:50]}")
            else: 
                logger.warning("Worker: Chunk parsing without header is highly unreliable. Returning raw.")
                return DataFrame(data=[line.split(repo_extraction_params.get("delimiter", ",")) for line in lines])
            return df_chunk

        repo.set_strategy_for_extraction(
            kind=strategy_kind,
            path=file_path,
            **repo_extraction_params
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