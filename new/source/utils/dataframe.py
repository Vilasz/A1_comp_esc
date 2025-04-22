from __future__ import annotations

import csv
import logging
import queue
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from new.source.utils.dataframe import DataFrame, Series

LOG_FORMAT = "%(asctime)s [%(levelname)8s] [%(threadName)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

def _detect_delimiter(sample: str, candidates: Iterable[str] = (",", ";", "|", "\t")) -> str:
    """Escolhe o delimitador mais frequente numa amostra."""
    counts = {d: sample.count(d) for d in candidates}
    return max(counts, key=counts.get)


def _read_table(
    path: Path,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    encoding: str = "utf‑8",
) -> DataFrame[str]:
    """Lê arquivo texto delimitado → DataFrame (tudo string)."""
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

    header = rows[0] if has_header else [f"col{i}" for i in range(len(rows[0]))]
    start = 1 if has_header else 0

    df = DataFrame(header, [Series() for _ in header])
    for row in rows[start:]:
        record = {col: (row[i].strip() if i < len(row) else "") for i, col in enumerate(header)}
        df.append(record)
    return df

class MapMutex:
    """Dicionário simples + locks independentes por chave."""

    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}
        self._locks: Dict[str, threading.Lock] = {}
        self._global = threading.Lock()

    def _lock_for(self, key: str) -> threading.Lock:
        with self._global:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            return self._locks[key]
    def getLock(self, key: str) -> threading.Lock:
        lock = self._lock_for(key)
        lock.acquire()
        return lock

    def get(self, key: str, default: Any = "") -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value

class BaseStrategy:
    def extract(self) -> DataFrame[Any]:
        raise NotImplementedError

    def load(self, df: DataFrame[Any]) -> None:
        raise NotImplementedError
class CSVExtractor(BaseStrategy):
    def __init__(self, path: Path) -> None:
        self.path = path

    def extract(self) -> DataFrame[str]:
        logger.info("Lendo CSV %s", self.path)
        return _read_table(self.path, delimiter=None, has_header=True)

    def load(self, df: DataFrame[Any]) -> None: 
        raise RuntimeError("CSVExtractor não implementa load()")

class TXTExtractor(BaseStrategy):
    def __init__(self, path: Path) -> None:
        self.path = path

    def extract(self) -> DataFrame[str]:
        logger.info("Lendo TXT %s", self.path)
        return _read_table(self.path, delimiter="|", has_header=False)

    def load(self, df: DataFrame[Any]) -> None:  
        raise RuntimeError("TXTExtractor não implementa load()")
class SQLLoader(BaseStrategy):
    """Grava DataFrame em tabela SQLite usando apenas sqlite3."""

    def __init__(self, conn: sqlite3.Connection, table: str, if_exists: str = "append") -> None:
        self.conn = conn
        self.table = table
        self.if_exists = if_exists.lower()

    def extract(self) -> DataFrame[Any]: 
        raise RuntimeError("SQLLoader não implementa extract()")

    def load(self, df: DataFrame[Any]) -> None:
        if df.shape[0] == 0:
            logger.warning("DataFrame vazio → nada para gravar em '%s'", self.table)
            return

        cols = df.columns
        cursor = self.conn.cursor()
        if self.if_exists == "replace":
            cursor.execute(f'DROP TABLE IF EXISTS "{self.table}"')
        col_defs = ", ".join(f'"{c}" TEXT' for c in cols)
        cursor.execute(f'CREATE TABLE IF NOT EXISTS "{self.table}" ({col_defs})')

        placeholders = ", ".join("?" for _ in cols)
        insert_sql = f'INSERT INTO "{self.table}" ({", ".join(cols)}) VALUES ({placeholders})'

        rows: List[Tuple[str, ...]] = []
        for i in range(df.shape[0]):
            rows.append(tuple(str(df[c][i]) for c in cols))  

        cursor.executemany(insert_sql, rows)
        self.conn.commit()

class RepoData:
    class Kind:
        CSV = "csv"
        TXT = "txt"
        SQL = "sql"

    def __init__(self) -> None:
        self._strategy: Optional[BaseStrategy] = None

    def setStrategy(
        self,
        kind: str,
        *,
        path: str = "",
        db: Optional[sqlite3.Connection | str] = None,
        table_name: str = "",
        if_exists: str = "append",
    ) -> None:
        if kind == self.Kind.CSV:
            self._strategy = CSVExtractor(Path(path))
        elif kind == self.Kind.TXT:
            self._strategy = TXTExtractor(Path(path))
        elif kind == self.Kind.SQL:
            if db is None:
                raise ValueError("db não pode ser None para SQLLoader")
            conn = db if isinstance(db, sqlite3.Connection) else sqlite3.connect(db, check_same_thread=False)
            self._strategy = SQLLoader(conn, table_name, if_exists)
        else:
            raise ValueError(f"Tipo de estratégia desconhecido: {kind}")
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
        in_q: "queue.Queue",
        out_q: Optional["queue.Queue"] = None,
        *,
        name: Optional[str] = None,
        daemon: bool = True,
    ) -> None:
        super().__init__(name=name, daemon=daemon)
        self.in_q = in_q
        self.out_q = out_q
        self._alive = threading.Event()
        self._alive.set()

    def stop(self) -> None:
        self._alive.clear()

    @property
    def running(self) -> bool:
        return self._alive.is_set()
class ExtractThread(ThreadWrapper):
    """Extrai DataFrames a partir dos arquivos listados em TASKS."""

    def __init__(
        self,
        in_q: "queue.Queue[Tuple[str, str]]",
        out_q: "queue.Queue[Tuple[str, DataFrame[Any]]]",
        map_mutex: MapMutex,
        *,
        name: str = "Extractor",
    ) -> None:
        super().__init__(in_q, out_q, name=name)
        self.map_mutex = map_mutex
        self.repo = RepoData()

    def _handle_key(self, key: str, value: str) -> Optional[DataFrame[Any]]:
        base = Path("../simulator/data")

        if key == "produtos":
            self.repo.setStrategy(RepoData.Kind.CSV, path=base / "contaverde/products.txt")
            return self.repo.extractData()

        if key == "estoque":
            self.repo.setStrategy(RepoData.Kind.CSV, path=base / "contaverde/stock.txt")
            return self.repo.extractData()

        if key == "compras":
            self.repo.setStrategy(RepoData.Kind.CSV, path=base / "contaverde/purchase_orders.txt")
            return self.repo.extractData()

        if key == "cade":
            self.repo.setStrategy(RepoData.Kind.TXT, path=base / "cadeanalytics/cade_analytics.txt")
            return self.repo.extractData()

        if key == "datacat":
            return self._handle_datacat()

        logger.warning("Chave desconhecida: %s", key)
        return None
    def _handle_datacat(self) -> Optional[DataFrame[Any]]:
        base = Path("../simulator/data/datacat/behaviour")
        if not base.exists():
            logger.warning("Diretório %s inexistente", base)
            return None

        with self.map_mutex.getLock("datacat"):
            last = str(self.map_mutex.get("datacat_behaviour", ""))
            newest = last
            dfs: List[DataFrame[Any]] = []

            for entry in sorted(base.iterdir()):
                fname = str(entry)
                if fname > last:
                    self.repo.setStrategy(RepoData.Kind.TXT, path=fname)
                    dfs.append(self.repo.extractData())
                    newest = max(newest, fname)

            self.map_mutex.set("datacat_behaviour", newest)

        if dfs:
            out = dfs[0]
            for d in dfs[1:]:
                out = out.concat(d)
            return out
        return None

    def run(self) -> None:  # noqa: D401
        logger.info("%s iniciado", self.name)
        while self.running:
            try:
                key, value = self.in_q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                df = self._handle_key(key, value)
                if df is not None and self.out_q is not None:
                    self.out_q.put((key, df))
            except Exception as exc:  # pragma: no cover
                logger.exception("Erro processando chave %s: %s", key, exc)
            finally:
                self.in_q.task_done()
        logger.info("%s finalizado", self.name)
class LoaderThread(ThreadWrapper):
    """Recebe DataFrames, grava em SQLite e loga tempos."""

    def __init__(
        self,
        in_q: "queue.Queue[Tuple[str, DataFrame[Any]]]",
        *,
        db_path: str = "new.db",
        name: str = "Loader",
    ) -> None:
        super().__init__(in_q, None, name=name)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.repo = RepoData()
        self._times_lock = threading.Lock()
        self._times_file = Path("times.txt")

    @staticmethod
    def _split_key(key: str) -> Tuple[str, str]:
        return key.split(" ", 1) if " " in key else (key, "0")

    def _log_time(self, key: str, ts: str) -> None:
        now = time.time_ns()
        with self._times_lock, self._times_file.open("a", encoding="utf-8") as fh:
            fh.write(f"{key} {ts} {now}\n")

    def run(self) -> None:  # noqa: D401
        logger.info("%s iniciado", self.name)
        while self.running:
            try:
                key, df = self.in_q.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                name, ts = self._split_key(key)
                self.repo.setStrategy(RepoData.Kind.SQL, db=self.conn, table_name=name)
                self.repo.loadData(df)
                self._log_time(name, ts)
            except Exception as exc:  # pragma: no cover
                logger.exception("Erro carregando chave %s: %s", key, exc)
            finally:
                self.in_q.task_done()
        self.conn.close()
        logger.info("%s finalizado", self.name)

TASKS: List[Tuple[str, str]] = [
    ("produtos", ""),
    ("estoque", ""),
    ("compras", ""),
    ("cade", ""),
    ("datacat", ""),
]

def enqueue_tasks(q: "queue.Queue[Tuple[str, str]]") -> None:
    for t in TASKS:
        q.put(t)

def main() -> None:  # noqa: D401
    q_extract: "queue.Queue[Tuple[str, str]]" = queue.Queue()
    q_load: "queue.Queue[Tuple[str, DataFrame[Any]]]" = queue.Queue()

    mutex = MapMutex()
    extractor = ExtractThread(q_extract, q_load, mutex)
    loader = LoaderThread(q_load)

    extractor.start()
    loader.start()
    enqueue_tasks(q_extract)

    try:
        q_extract.join()
        q_load.join()
    except KeyboardInterrupt:
        logger.info("Interrupção recebida, encerrando…")
    finally:
        extractor.stop()
        loader.stop()
        extractor.join()
        loader.join()
        logger.info("Pipeline encerrado com sucesso")

if __name__ == "__main__":
    main()
