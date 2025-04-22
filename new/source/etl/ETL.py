from __future__ import annotations

import csv
import logging
import queue
import sqlite3
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine, text

from new.source.utils.dataframe import DataFrame, Series

LOG_FORMAT = "%(asctime)s [%(levelname)8s] [%(threadName)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

def _detect_delimiter(sample: str, candidates: Iterable[str] = (",", ";", "|", "\t")) -> str:
    """Heurística simples para escolher o delimitador mais frequente."""
    counts = {delim: sample.count(delim) for delim in candidates}
    return max(counts, key=counts.get)


def _read_table(
    path: Path,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    encoding: str = "utf-8",
) -> DataFrame[str]:
    """Lê arquivo delimitado e devolve ``DataFrame``.

    Todos os dados são carregados como *string*.  Conversões podem ser feitas
    posteriormente através dos métodos ``Series.astype`` etc.
    """

    if not path.exists():
        raise FileNotFoundError(path)

    with path.open("r", encoding=encoding, newline="") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        delim = delimiter or _detect_delimiter(sample)
        reader = csv.reader(fh, delimiter=delim)
        rows = list(reader)

    if not rows:
        return DataFrame()

    first_row = rows[0]
    columns: List[str]
    start_idx: int

    if has_header:
        columns = [c.strip() for c in first_row]
        start_idx = 1
    else:
        columns = [f"col{i}" for i in range(len(first_row))]
        start_idx = 0

    df = DataFrame(columns, [Series() for _ in columns])
    for row in rows[start_idx:]:
        record = {col: (row[i].strip() if i < len(row) else "") for i, col in enumerate(columns)}
        df.append(record)
    return df

class MapMutex:
    """Dicionário protegido por *locks* individuais por *key*."""

    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}
        self._locks: Dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    def _get_or_create_lock(self, key: str) -> threading.Lock:
        with self._global_lock:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            return self._locks[key]

    def getLock(self, key: str) -> threading.Lock: 
        """Retorna o *lock* associado e **já bloqueado** para ``key``."""
        lock = self._get_or_create_lock(key)
        lock.acquire()
        return lock

    def get(self, key: str, default: Any = "") -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value

class BaseStrategy:
    """Interface comum a extratores e carregadores."""

    def extract(self) -> DataFrame[Any]: 
        raise NotImplementedError

    def load(self, df: DataFrame[Any]) -> None: 
        raise NotImplementedError

@dataclass
class CSVExtractor(BaseStrategy):
    path: Path

    def extract(self) -> DataFrame[str]:
        logger.info("Lendo CSV %s", self.path)
        return _read_table(self.path, delimiter=None, has_header=True)

    def load(self, df: DataFrame[Any]) -> None: 
        raise RuntimeError("CSVExtractor não implementa load()")


@dataclass
class TXTExtractor(BaseStrategy):
    path: Path

    def extract(self) -> DataFrame[str]:
        logger.info("Lendo TXT %s", self.path)
        return _read_table(self.path, delimiter="|", has_header=False)

    def load(self, df: DataFrame[Any]) -> None:  
        raise RuntimeError("TXTExtractor não implementa load()")

@dataclass
class SQLLoader(BaseStrategy):
    engine: Any 
    table_name: str
    if_exists: str = "append" 

    def extract(self) -> DataFrame[Any]:  
        raise RuntimeError("SQLLoader não implementa extract()")

    def load(self, df: DataFrame[Any]) -> None:  
        if df.shape[0] == 0:
            logger.warning("DataFrame vazio → nada para gravar em '%s'", self.table_name)
            return

        logger.info("Gravando %d linhas em '%s'", df.shape[0], self.table_name)
        cols = df.columns
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join([":%s" % c for c in cols])

        with self.engine.begin() as conn:
            if self.if_exists == "replace":
                conn.exec_driver_sql(f'DROP TABLE IF EXISTS "{self.table_name}"')
            cols_def = ", ".join(f'"{c}" TEXT' for c in cols)
            conn.exec_driver_sql(
                f'CREATE TABLE IF NOT EXISTS "{self.table_name}" ({cols_def})'
            )

        rows: List[Dict[str, str]] = []
        for i in range(df.shape[0]):
            row = {col: str(df[col][i]) for col in cols} 
            rows.append(row)

        with self.engine.begin() as conn:
            insert_sql = text(
                f'INSERT INTO "{self.table_name}" ({cols_sql}) VALUES ({placeholders})'
            )
            conn.execute(insert_sql, rows)  


class RepoData:
    class StrategyType:
        CSV = "csv"
        TXT = "txt"
        SQL = "sql"

    def __init__(self) -> None:
        self._strategy: Optional[BaseStrategy] = None

    def setStrategy(
        self,
        strategy_type: str,
        path: str = "",
        db: Optional[sqlite3.Connection | Any] = None,
        table_name: str = "",
    ) -> None:
        if strategy_type == self.StrategyType.CSV:
            self._strategy = CSVExtractor(Path(path))
        elif strategy_type == self.StrategyType.TXT:
            self._strategy = TXTExtractor(Path(path))
        elif strategy_type == self.StrategyType.SQL:
            if db is None:
                raise ValueError("db não pode ser None para SQLLoader")
            engine = db if not isinstance(db, str) else create_engine(f"sqlite:///{db}")
            self._strategy = SQLLoader(engine, table_name)
        else:
            raise ValueError(f"StrategyType desconhecido: {strategy_type}")

    def extractData(self) -> DataFrame[Any]:
        if not self._strategy:
            raise RuntimeError("Strategy não configurada")
        return self._strategy.extract()

    def loadData(self, df: DataFrame[Any]) -> None:
        if not self._strategy:
            raise RuntimeError("Strategy não configurada")
        self._strategy.load(df)

class ThreadWrapper(threading.Thread):
    def __init__(
        self,
        in_queue: "queue.Queue[Tuple[str, Any]]",
        out_queue: Optional["queue.Queue[Tuple[str, Any]]"] = None,
        name: str | None = None,
        daemon: bool = True,
    ) -> None:
        super().__init__(name=name, daemon=daemon)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self._running = threading.Event()
        self._running.set()

    def stop(self) -> None:
        self._running.clear()

    @property
    def running(self) -> bool:  
        return self._running.is_set()


class ExtractThread(ThreadWrapper):
    """Responsável por extrair ``DataFrame`` a partir de arquivos."""

    def __init__(
        self,
        in_queue: "queue.Queue[Tuple[str, str]]",
        out_queue: "queue.Queue[Tuple[str, DataFrame[Any]]]",
        map_mutex: MapMutex,
        name: str = "ExtractThread",
    ) -> None:
        super().__init__(in_queue, out_queue, name=name)
        self.map_mutex = map_mutex
        self.repo_data = RepoData()

    def run(self) -> None:  
        logger.info("%s iniciado", self.name)
        while self.running:
            try:
                key, value = self.in_queue.get(timeout=0.5)
            except queue.Empty:
                continue  

            try:
                df = self._handle_key(key, value)
                if df is not None and self.out_queue is not None:
                    self.out_queue.put((key, df))
            except Exception as exc: 
                logger.exception("Erro processando chave %s: %s", key, exc)
            finally:
                self.in_queue.task_done()
        logger.info("%s finalizado", self.name)

    def _handle_key(self, key: str, value: str) -> Optional[DataFrame[Any]]: 
        if key == "produtos":
            self.repo_data.setStrategy(RepoData.StrategyType.CSV, "../simulator/data/contaverde/products.txt")
            return self.repo_data.extractData()
        elif key == "estoque":
            self.repo_data.setStrategy(RepoData.StrategyType.CSV, "../simulator/data/contaverde/stock.txt")
            return self.repo_data.extractData()
        elif key == "compras":
            self.repo_data.setStrategy(RepoData.StrategyType.CSV, "../simulator/data/contaverde/purchase_orders.txt")
            return self.repo_data.extractData()
        elif key == "cade":
            self.repo_data.setStrategy(RepoData.StrategyType.TXT, "../simulator/data/cadeanalytics/cade_analytics.txt")
            return self.repo_data.extractData()
        elif key == "datacat":
            return self._handle_datacat()
        else:
            logger.warning("Chave desconhecida: %s", key)
            return None

    def _handle_datacat(self) -> Optional[DataFrame[Any]]:
        base_path = Path("../simulator/data/datacat/behaviour/")
        if not base_path.exists():
            logger.warning("Diretório %s inexistente", base_path)
            return None

        with self.map_mutex.getLock("datacat"):
            last_path = str(self.map_mutex.get("datacat_behaviour", ""))
            max_path = last_path
            dfs: List[DataFrame[Any]] = []

            for entry in sorted(base_path.iterdir()):
                filename = str(entry)
                if filename > last_path:
                    self.repo_data.setStrategy(RepoData.StrategyType.TXT, filename)
                    dfs.append(self.repo_data.extractData())
                    max_path = max(max_path, filename)
            self.map_mutex.set("datacat_behaviour", max_path)

        if dfs:
            combined = dfs[0]
            for d in dfs[1:]:
                combined = combined.concat(d)
            return combined
        return None

class LoaderThread(ThreadWrapper):
    """Carrega ``DataFrame`` no SQLite e registra tempos de execução."""

    def __init__(
        self,
        in_queue: "queue.Queue[Tuple[str, DataFrame[Any]]]",
        db_path: str = "new.db",
        name: str = "LoaderThread",
    ) -> None:
        super().__init__(in_queue, None, name=name)
        self.engine = create_engine(f"sqlite:///{db_path}")
        self._meta: set[str] = set()  # tabelas já criadas
        self.repo_data = RepoData()
        self._times_file_lock = threading.Lock()
        self._times_file_path = Path("times.txt")
    def run(self) -> None: 
        logger.info("%s iniciado", self.name)
        while self.running:
            try:
                key, df = self.in_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                key_name, key_time = self._split_key(key)
                self._ensure_table(key_name, df)
                self.repo_data.setStrategy(RepoData.StrategyType.SQL, db=self.engine, table_name=key_name)
                self.repo_data.loadData(df)
                self._log_time(key_name, key_time)
            except Exception as exc:  # pragma: no cover
                logger.exception("Erro carregando chave %s: %s", key, exc)
            finally:
                self.in_queue.task_done()
        logger.info("%s finalizado", self.name)
    @staticmethod
    def _split_key(key: str) -> Tuple[str, str]:
        if " " in key:
            name, timestamp = key.split(" ", 1)
        else:
            name, timestamp = key, "0"
        return name, timestamp

    def _ensure_table(self, table_name: str, df: DataFrame[Any]) -> None:
        if table_name in self._meta:
            return
        logger.info("Criando estrutura da tabela '%s' (%d colunas)", table_name, len(df.columns))
        cols_def = ", ".join(f'"{c}" TEXT' for c in df.columns)
        with self.engine.begin() as conn:
            conn.exec_driver_sql(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols_def})')
        self._meta.add(table_name)

    def _log_time(self, key_name: str, key_time: str) -> None:
        now_ns = time.time_ns()
        with self._times_file_lock, self._times_file_path.open("a", encoding="utf-8") as fh:
            fh.write(f"{key_name} {key_time} {now_ns}\n")


TASKS: List[Tuple[str, str]] = [
    ("produtos", ""),
    ("estoque", ""),
    ("compras", ""),
    ("cade", ""),
    ("datacat", ""),
]


def enqueue_initial_tasks(q: "queue.Queue[Tuple[str, str]]") -> None:
    for item in TASKS:
        q.put(item)

def main() -> None: 
    extract_q: "queue.Queue[Tuple[str, str]]" = queue.Queue()
    load_q: "queue.Queue[Tuple[str, DataFrame[Any]]]" = queue.Queue()

    map_mutex = MapMutex()

    extract_thread = ExtractThread(extract_q, load_q, map_mutex, name="Extractor")
    loader_thread = LoaderThread(load_q, name="Loader")

    extract_thread.start()
    loader_thread.start()

    enqueue_initial_tasks(extract_q)

    try:
        extract_q.join()
        load_q.join()
    except KeyboardInterrupt:
        logger.info("Interrupção recebida, finalizando…")
    finally:
        extract_thread.stop()
        loader_thread.stop()
        extract_thread.join()
        loader_thread.join()
        logger.info("Pipeline encerrado com sucesso")


if __name__ == "__main__":
    main()
