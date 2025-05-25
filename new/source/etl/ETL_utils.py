# new/source/etl/etl_utils.py
from __future__ import annotations

import logging
import sqlite3
import threading # For MapMutex
from collections import defaultdict # For MapMutex
from contextlib import contextmanager # For MapMutex
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Iterator, TypeVar, Generic, Callable, Tuple


import psycopg2 # Add this
from psycopg2.extras import execute_values, DictCursor

# Assuming dataframe.py is at new.source.utils.dataframe
from new.source.utils.dataframe import DataFrame, read_csv, read_json

logger = logging.getLogger(__name__)
T = TypeVar("T") # For MapMutex generic type

# --- Base Strategy ---
class BaseStrategy:
    def extract(self) -> DataFrame:
        raise NotImplementedError("Subclasses must implement the method extract.")
    def load(self, df: DataFrame) -> None:
        raise NotImplementedError("Subclasses must implement the method load.")

# --- ETL Specific Extractors ---
class CSVExtractorEtl(BaseStrategy):
    def __init__(self, path: Path, delimiter: Optional[str] = None, has_header: bool = True):
        self.path = path
        self.delimiter = delimiter
        self.has_header = has_header

    def extract(self) -> DataFrame:
        logger.info("ETL: Lendo CSV %s", self.path)
        return read_csv(self.path, delimiter=self.delimiter, has_header=self.has_header)

class JSONExtractorEtl(BaseStrategy):
    def __init__(self, path: Path, records_path: Optional[Union[str, List[str]]] = None):
        self.path = path
        self.records_path = records_path

    def extract(self) -> DataFrame:
        logger.info("ETL: Lendo JSON %s", self.path)
        return read_json(self.path, records_path=self.records_path)

class TXTExtractorEtl(BaseStrategy):
    def __init__(self, path: Path, delimiter: str = "|", has_header: bool = False):
        self.path = path
        self.delimiter = delimiter
        self.has_header = has_header

    def extract(self) -> DataFrame:
        logger.info("ETL: Lendo TXT %s (delimiter '%s')", self.path, self.delimiter)
        return read_csv(self.path, delimiter=self.delimiter, has_header=self.has_header) # Uses read_csv

# --- ETL Specific Loader ---
class SQLLoaderEtl(BaseStrategy):
    def __init__(self, conn_or_path: Union[sqlite3.Connection, str, Path], table_name: str, if_exists: str = "append"):
        self._conn_managed_internally = False
        if isinstance(conn_or_path, (str, Path)):
            self.conn = sqlite3.connect(str(conn_or_path), check_same_thread=False) # check_same_thread for LoaderThread
            self._conn_managed_internally = True
        elif isinstance(conn_or_path, sqlite3.Connection):
            self.conn = conn_or_path
        else:
            raise TypeError("conn_or_path must be a sqlite3.Connection, str, or Path.")
            
        self.table_name = table_name
        self.if_exists = if_exists.lower()
        if self.if_exists not in ["append", "replace", "fail"]:
            raise ValueError(f"if_exists must be 'append', 'replace', or 'fail', not {self.if_exists}")

    def load(self, df: DataFrame) -> None:
        if df.shape[0] == 0:
            logger.info("ETL: DataFrame vazio para tabela '%s'. Nada para gravar.", self.table_name)
            return
        if not df.columns:
            logger.warning("ETL: DataFrame sem colunas para tabela '%s'. Nada para gravar.", self.table_name)
            return

        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (self.table_name,))
        table_exists = cursor.fetchone()

        if table_exists:
            if self.if_exists == "fail":
                raise ValueError(f"ETL: Tabela '{self.table_name}' já existe e if_exists='fail'.")
            if self.if_exists == "replace":
                logger.info("ETL: Substituindo tabela '%s'.", self.table_name)
                cursor.execute(f'DROP TABLE IF EXISTS "{self.table_name}"')
                table_exists = False

        if not table_exists:
            col_defs = ", ".join(f'"{c}" TEXT' for c in df.columns)
            logger.info("ETL: Criando tabela '%s' com colunas: %s", self.table_name, df.columns)
            cursor.execute(f'CREATE TABLE "{self.table_name}" ({col_defs})')

        quoted_cols = ", ".join(f'"{c}"' for c in df.columns)
        placeholders = ", ".join("?" for _ in df.columns)
        insert_sql = f'INSERT INTO "{self.table_name}" ({quoted_cols}) VALUES ({placeholders})'
        
        rows_to_insert: List[Tuple[Any, ...]] = []
        for i in range(df.shape[0]):
            row_dict = df.get_row_dict(i)
            row_tuple = tuple(str(row_dict.get(col)) if row_dict.get(col) is not None else None for col in df.columns)
            rows_to_insert.append(row_tuple)

        if rows_to_insert:
            try:
                cursor.executemany(insert_sql, rows_to_insert)
                self.conn.commit()
                logger.info("ETL: Gravadas %d linhas em '%s'", len(rows_to_insert), self.table_name)
            except Exception as e:
                logger.error(f"ETL: Erro ao inserir dados na tabela {self.table_name}: {e}")
                self.conn.rollback() # Rollback on error
                raise
        
    def close_connection_if_managed(self):
        if self._conn_managed_internally and self.conn:
            self.conn.close()
            logger.debug(f"SQLLoaderEtl for '{self.table_name}' closed its managed connection.")

    def __del__(self):
        self.close_connection_if_managed()

class PostgresEventExtractor(BaseStrategy):
    def __init__(self, db_config: dict, batch_size: int = 100, table_name: str = "received_events"):
        self.db_config = db_config
        self.batch_size = batch_size
        self.table_name = table_name
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._lock = threading.Lock() # To protect connection acquisition if shared (though not strictly needed per instance here)

    def _get_conn(self) -> psycopg2.extensions.connection:
        with self._lock:
            if self._conn is None or self._conn.closed:
                logger.debug(f"PostgresEventExtractor: Connecting to DB {self.db_config.get('database')} on {self.db_config.get('host')}")
                self._conn = psycopg2.connect(**self.db_config)
            return self._conn

    def _close_conn(self) -> None:
        with self._lock:
            if self._conn and not self._conn.closed:
                self._conn.close()
                self._conn = None
                logger.debug("PostgresEventExtractor: DB connection closed.")

    def extract(self) -> DataFrame:
        conn = self._get_conn()
        extracted_event_pks = []
        records_for_df = []
        # Define expected columns based on how TransformStage will use them, including event_pk for updates
        # Ensure these column names are consistent with your processing logic.
        df_column_names = ["event_pk", "client_event_id", "client_id", "payload", "values_array", "timestamp_emission", "timestamp_received"]

        try:
            with conn.cursor(cursor_factory=DictCursor) as cursor: # Use DictCursor for easy row access
                # Atomically select and mark events for processing to avoid multiple ETL instances picking the same batch.
                # This uses advisory locks per batch. A simpler way for one ETL instance is shown.
                # For multiple ETL instances, a more robust "claim" system is needed.
                
                # Step 1: Select batch of unprocessed events
                select_sql = f"""
                    SELECT event_pk, client_event_id, client_id, payload, values_array, timestamp_emission, timestamp_received
                    FROM {self.table_name}
                    WHERE processed_by_etl = FALSE
                    ORDER BY timestamp_received
                    LIMIT %s
                """
                cursor.execute(select_sql, (self.batch_size,))
                raw_rows = cursor.fetchall()

                if not raw_rows:
                    logger.debug("PostgresEventExtractor: No new events to process.")
                    return DataFrame(columns=df_column_names) # Return empty DataFrame with schema

                for row_dict in raw_rows:
                    extracted_event_pks.append(row_dict["event_pk"])
                    # Ensure all df_column_names are present in row_dict or handle missing ones
                    records_for_df.append({col: row_dict.get(col) for col in df_column_names})
                
                # Step 2: Mark these selected events as processing started
                if extracted_event_pks:
                    update_sql = f"""
                        UPDATE {self.table_name}
                        SET processed_by_etl = TRUE, etl_processing_start_time = CURRENT_TIMESTAMP
                        WHERE event_pk = ANY(%s)
                    """ # Using = ANY for a list of PKs
                    cursor.execute(update_sql, (extracted_event_pks,))
                conn.commit()
            logger.info(f"PostgresEventExtractor: Extracted and marked {len(records_for_df)} events for processing.")
            return DataFrame(columns=df_column_names, data=records_for_df)

        except psycopg2.Error as e:
            if conn: conn.rollback()
            logger.error(f"PostgresEventExtractor: DB error during event extraction: {e}", exc_info=True)
            # Potentially re-raise or return empty DF to allow pipeline to continue with other tasks
            return DataFrame(columns=df_column_names) # Empty on error
        except Exception as e:
            if conn: conn.rollback() # Ensure rollback for generic errors too
            logger.error(f"PostgresEventExtractor: Generic error during event extraction: {e}", exc_info=True)
            return DataFrame(columns=df_column_names)
        # Don't close connection here, ExtractStage might call extract() multiple times.
        # Connection will be closed when the ETL_new.py process exits or by a dedicated close method.

    def __del__(self): # Ensure connection is closed when object is destroyed
        self._close_conn()


class PostgresLoaderEtl(BaseStrategy):
    def __init__(self, db_config: dict, table_name: str, if_exists: str = "append", 
                 columns_to_insert: Optional[List[str]] = None,
                 upsert_on_conflict_columns: Optional[List[str]] = None, # e.g. ["produto", "centro"]
                 upsert_update_columns_map: Optional[Dict[str,str]] = None # e.g. {"total_valor": "EXCLUDED.total_valor"}
                 ):
        self.db_config = db_config
        self.table_name = table_name
        self.if_exists = if_exists.lower()
        self.columns_to_insert = columns_to_insert # If None, infer from DataFrame matching table
        self.upsert_on_conflict_columns = upsert_on_conflict_columns
        self.upsert_update_columns_map = upsert_update_columns_map
        self._conn: Optional[psycopg2.extensions.connection] = None
        self._lock = threading.Lock()

        if self.if_exists not in ["append", "replace", "fail", "upsert"]:
            raise ValueError(f"if_exists must be 'append', 'replace', 'fail', or 'upsert', not {self.if_exists}")
        if self.if_exists == "upsert" and not self.upsert_on_conflict_columns:
            raise ValueError("upsert_on_conflict_columns must be provided if if_exists is 'upsert'.")

    def _get_conn(self) -> psycopg2.extensions.connection:
        with self._lock:
            if self._conn is None or self._conn.closed:
                self._conn = psycopg2.connect(**self.db_config)
            return self._conn

    def _close_conn(self) -> None:
        with self._lock:
            if self._conn and not self._conn.closed:
                self._conn.close()
                self._conn = None

    def load(self, df: DataFrame) -> None:
        if df.shape[0] == 0:
            logger.info(f"ETL (Postgres): DataFrame empty for '{self.table_name}'. Nothing to load.")
            return
        
        final_columns_to_insert = self.columns_to_insert or df.columns
        missing_cols = [col for col in final_columns_to_insert if col not in df.columns]
        if missing_cols:
            logger.error(f"ETL (Postgres): DataFrame for '{self.table_name}' is missing required columns for insertion: {missing_cols}. Provided: {df.columns}")
            return

        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                # Table existence and creation logic (simplified: assume tables exist as per Step 2 schema)
                # If 'replace' is needed, you'd add DROP TABLE and CREATE TABLE here.
                # For this project, analytical tables are predefined.

                rows_to_insert = []
                for i in range(df.shape[0]):
                    row_dict = df.get_row_dict(i)
                    row_tuple = tuple(row_dict.get(col) for col in final_columns_to_insert)
                    rows_to_insert.append(row_tuple)

                if not rows_to_insert: return

                quoted_insert_cols = ", ".join([f'"{c}"' for c in final_columns_to_insert])
                placeholders = ", ".join(["%s"] * len(final_columns_to_insert))
                
                if self.if_exists == "upsert" and self.upsert_on_conflict_columns:
                    conflict_target = ", ".join([f'"{c}"' for c in self.upsert_on_conflict_columns])
                    update_clauses = []
                    if self.upsert_update_columns_map: # Use map if provided
                        for dest_col, source_expr in self.upsert_update_columns_map.items():
                            update_clauses.append(f'"{dest_col}" = {source_expr}') # source_expr could be "EXCLUDED.col_name" or "table.col_name + EXCLUDED.col_name"
                    else: # Default: update all insertable columns that are not conflict columns
                        for col in final_columns_to_insert:
                            if col not in self.upsert_on_conflict_columns:
                                update_clauses.append(f'"{col}" = EXCLUDED."{col}"')
                    
                    # Always update analysis_timestamp if it's one of the target table columns
                    # This assumes the target table has an 'analysis_timestamp' column
                    # We should check if 'analysis_timestamp' is part of the table schema.
                    # For now, we add it if it's not explicitly in upsert_update_columns_map
                    has_analysis_ts_in_map = any('analysis_timestamp' in k for k in (self.upsert_update_columns_map or {}).keys())
                    if not has_analysis_ts_in_map: # And if table schema has it (check omitted for brevity)
                        update_clauses.append(f'"analysis_timestamp" = CURRENT_TIMESTAMP')


                    set_clause = ", ".join(update_clauses)
                    sql = f"""
                        INSERT INTO "{self.table_name}" ({quoted_insert_cols})
                        VALUES %s
                        ON CONFLICT ({conflict_target}) DO UPDATE
                        SET {set_clause}
                    """
                    execute_values(cursor, sql, rows_to_insert, page_size=500)
                else: # Append or other modes
                    sql = f'INSERT INTO "{self.table_name}" ({quoted_insert_cols}) VALUES %s'
                    execute_values(cursor, sql, rows_to_insert, page_size=500)
                
                conn.commit()
                logger.info(f"ETL (Postgres): Loaded/Upserted {len(rows_to_insert)} rows into '{self.table_name}'.")
        except psycopg2.Error as e:
            if conn: conn.rollback()
            logger.error(f"ETL (Postgres): DB error for '{self.table_name}': {e}", exc_info=True)
            raise
        except Exception as e:
            if conn: conn.rollback()
            logger.error(f"ETL (Postgres): Generic error for '{self.table_name}': {e}", exc_info=True)
            raise
        # Connection not closed here, to allow reuse by the RepoData instance if needed.
        # It will be closed by RepoData.__del__ or if ETL_new.py closes its RepoData instances.

    def __del__(self):
        self._close_conn()


# --- RepoData for ETL Strategies ---
class RepoData:
    class Kind:
        CSV = "csv"
        TXT = "txt"
        SQL = "sql"
        JSON = "json"

    def __init__(self) -> None:
        self._strategy: Optional[BaseStrategy] = None
        # self._managed_sql_loader: Optional[SQLLoaderEtl] = None # To manage connection closing

    def set_strategy_for_extraction(
        self,
        kind: str,
        path: Union[str, Path],
        csv_delimiter: Optional[str] = None,
        csv_has_header: bool = True,
        txt_delimiter: str = "|",
        txt_has_header: bool = False,
        json_records_path: Optional[Union[str, List[str]]] = None,
    ) -> None:
        self._close_previous_strategy_connection() # Close connection if previous was SQL loader
        actual_path = Path(path)

        if kind == self.Kind.CSV:
            self._strategy = CSVExtractorEtl(actual_path, delimiter=csv_delimiter, has_header=csv_has_header)
        elif kind == self.Kind.TXT:
            self._strategy = TXTExtractorEtl(actual_path, delimiter=txt_delimiter, has_header=txt_has_header)
        elif kind == self.Kind.JSON:
            self._strategy = JSONExtractorEtl(actual_path, records_path=json_records_path)
        else:
            raise ValueError(f"ETL: Tipo de estratégia de extração desconhecido ou não suportado: {kind}")
        self._managed_sql_loader = None

    def set_strategy_for_loading(
        self,
        kind: str, # Should almost always be SQL for loading here
        # db_conn_or_path: Union[sqlite3.Connection, str, Path],
        db_config_or_conn: Union[Dict, Any],
        table_name: str,
        if_exists: str = "append",
    ) -> None:
        self._close_previous_strategy_connection()
        if kind == self.Kind.SQL:
            if isinstance(db_config_or_conn, dict):
                # Assuming it's a Postgres config
                self._strategy = PostgresLoaderEtl(db_config_or_conn, table_name, if_exists=if_exists)
            elif isinstance(db_config_or_conn, sqlite3.Connection):
                # Assuming it's a SQLite connection
                self._strategy = SQLLoaderEtl(db_config_or_conn, table_name, if_exists=if_exists)
            else:
                raise TypeError("db_config_or_conn must be a dict (Postgres config) or sqlite3.Connection.")
            # if loader._conn_managed_internally: # If SQLLoader created the connection
            #     self._managed_sql_loader = loader
            # else:
            #     self._managed_sql_loader = None # Connection managed externally
        else:
            raise ValueError(f"ETL: Tipo de estratégia de carregamento desconhecido ou não suportado: {kind}")

    def extract_data(self) -> DataFrame:
        if not self._strategy or not hasattr(self._strategy, 'extract'):
            raise RuntimeError("ETL: Estratégia de extração não configurada ou não suporta extract().")
        return self._strategy.extract()

    def load_data(self, df: DataFrame) -> None:
        if not self._strategy or not hasattr(self._strategy, 'load'):
            raise RuntimeError("ETL: Estratégia de carregamento não configurada ou não suporta load().")
        self._strategy.load(df)
        
    def _close_previous_strategy_connection(self):
        if self._managed_sql_loader:
            self._managed_sql_loader.close_connection_if_managed()
            self._managed_sql_loader = None
            
    def __del__(self):
        self._close_previous_strategy_connection()


# --- MapMutex (Per-key locking dictionary wrapper) ---
class MapMutex(Generic[T]):
    def __init__(self) -> None:
        self._data: Dict[str, T] = {}
        self._key_locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)

    @contextmanager
    def key_lock(self, key: str) -> Iterator[None]: # Yields None, lock is internal
        lock_for_key = self._key_locks[key] # defaultdict creates if not exists
        lock_for_key.acquire()
        try:
            yield
        finally:
            lock_for_key.release()

    def get(self, key: str, default: Optional[T] = None) -> Optional[T]:
        with self.key_lock(key):
            return self._data.get(key, default)

    def set(self, key: str, value: T) -> None:
        with self.key_lock(key):
            self._data[key] = value
            
    def update(self, key: str, update_function: Callable[[Optional[T]], T], default_if_missing: Optional[T] = None) -> T:
        with self.key_lock(key):
            current_value = self._data.get(key, default_if_missing)
            new_value = update_function(current_value)
            self._data[key] = new_value
            return new_value