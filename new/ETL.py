# etl_pipeline.py
"""ETL Pipeline reimplementado em Python

Estrutura Geral:
    ├─ MapMutex               – dicionário thread‑safe com locks por‑chave
    ├─ Strategy Pattern       – CSVExtractor, TXTExtractor, SQLLoader
    ├─ RepoData               – facade que delega ao strategy ativo
    ├─ ThreadWrapper          – classe base para threads de pipeline
    ├─ ExtractThread          – extrai arquivos de dados → DataFrame
    ├─ LoaderThread           – escreve DataFrames no SQLite
    └─ main()                 – orquestra o pipeline

"""
from __future__ import annotations

import logging
import os
import queue
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine


# Configuração de logging

LOG_FORMAT = "%(asctime)s [%(levelname)8s] [%(threadName)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


# MapMutex – dicionário thread‑safe 

class MapMutex:
    """Dicionário protegido por locks individuais por chave."""

    def __init__(self) -> None:
        self._data: Dict[str, Any] = {}
        self._locks: Dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    def _get_or_create_lock(self, key: str) -> threading.Lock:
        with self._global_lock:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            return self._locks[key]

    # Interface inspirada no código C++ ------------------------------------
    def getLock(self, key: str) -> threading.Lock:
        """Retorna o *lock* associado à *key* (já bloqueado)."""
        lock = self._get_or_create_lock(key)
        lock.acquire()
        return lock

    def get(self, key: str, default: Any = "") -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value


# Strategy Pattern para extração/carregamento

class BaseStrategy:
    def extract(self) -> pd.DataFrame:  
        """Extrai dados e devolve *DataFrame*. Subclasses devem implementar."""
        raise NotImplementedError

    def load(self, df: pd.DataFrame) -> None:  
        """Carrega *DataFrame* de entrada em destino. Subclasses devem implementar."""
        raise NotImplementedError

# CSV ----------------------------------------------------------------------
@dataclass
class CSVExtractor(BaseStrategy):
    path: Path

    def extract(self) -> pd.DataFrame:
        logger.info("Lendo CSV %s", self.path)
        return pd.read_csv(self.path, delimiter=";|,|\t", engine="python")

    def load(self, df: pd.DataFrame) -> None:
        raise RuntimeError("CSVExtractor não implementa load()")

# TXT ----------------------------------------------------------------------
@dataclass
class TXTExtractor(BaseStrategy):
    path: Path

    def extract(self) -> pd.DataFrame:
        logger.info("Lendo TXT %s", self.path)
        return pd.read_csv(self.path, sep="|", header=None)

    def load(self, df: pd.DataFrame) -> None:  
        raise RuntimeError("TXTExtractor não implementa load()")

# SQL ----------------------------------------------------------------------
@dataclass
class SQLLoader(BaseStrategy):
    engine: Any  # sqlalchemy Engine
    table_name: str
    if_exists: str = "append"

    def extract(self) -> pd.DataFrame:  
        raise RuntimeError("SQLLoader não implementa extract()")

    def load(self, df: pd.DataFrame) -> None:
        logger.info("Gravando %d linhas em '%s'", len(df), self.table_name)
        df.to_sql(self.table_name, self.engine, if_exists=self.if_exists, index=False)


# RepoData – delega para strategy ativo

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
        db: Optional[sqlite3.Connection | create_engine] = None,
        table_name: str = "",
    ) -> None:
        if strategy_type == self.StrategyType.CSV:
            self._strategy = CSVExtractor(Path(path))
        elif strategy_type == self.StrategyType.TXT:
            self._strategy = TXTExtractor(Path(path))
        elif strategy_type == self.StrategyType.SQL:
            if db is None:
                raise ValueError("db não pode ser None para SQLLoader")
            # Aceita tanto objeto Engine quanto caminho de DB
            engine = db if not isinstance(db, str) else create_engine(f"sqlite:///{db}")
            self._strategy = SQLLoader(engine, table_name)
        else:
            raise ValueError(f"StrategyType desconhecido: {strategy_type}")

    # Facade 
    def extractData(self) -> pd.DataFrame:
        if not self._strategy:
            raise RuntimeError("Strategy não configurada")
        return self._strategy.extract()

    def loadData(self, df: pd.DataFrame) -> None:
        if not self._strategy:
            raise RuntimeError("Strategy não configurada")
        self._strategy.load(df)


# ThreadWrapper – base para threads do pipeline

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

    # Cada sub‑classe deve sobrepor run()


# ExtractThread

class ExtractThread(ThreadWrapper):
    """Responsável por extrair DataFrames a partir de arquivos."""

    def __init__(
        self,
        in_queue: "queue.Queue[Tuple[str, str]]",
        out_queue: "queue.Queue[Tuple[str, pd.DataFrame]]",
        map_mutex: MapMutex,
        name: str = "ExtractThread",
    ) -> None:
        super().__init__(in_queue, out_queue, name=name)
        self.map_mutex = map_mutex
        self.repo_data = RepoData()

    def run(self) -> None:  
        logger.info("%s iniciado", self.name)
        while self.running:
            try:
                key, value = self.in_queue.get(timeout=0.5)
            except queue.Empty:
                continue  # Permite verificar self.running periodicamente

            try:
                df = self._handle_key(key, value)
                if df is not None and self.out_queue is not None:
                    self.out_queue.put((key, df))
            except Exception as exc:  
                logger.exception("Erro processando chave %s: %s", key, exc)
            finally:
                self.in_queue.task_done()
        logger.info("%s finalizado", self.name)

    def _handle_key(self, key: str, value: str) -> Optional[pd.DataFrame]:
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
            logger.warning("Chave desconhecida: %s", key)
            return None

    def _handle_datacat(self) -> Optional[pd.DataFrame]:
        base_path = Path("../simulator/data/datacat/behaviour/")
        if not base_path.exists():
            logger.warning("Diretório %s inexistente", base_path)
            return None

        with self.map_mutex.getLock("datacat"):  # lock já adquirido
            last_path = str(self.map_mutex.get("datacat_behaviour", ""))
            max_path = last_path
            dfs: list[pd.DataFrame] = []

            for entry in sorted(base_path.iterdir()):
                filename = str(entry)
                if filename > last_path:
                    self.repo_data.setStrategy(RepoData.StrategyType.TXT, filename)
                    dfs.append(self.repo_data.extractData())
                    max_path = max(max_path, filename)
            self.map_mutex.set("datacat_behaviour", max_path)

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return None


# LoaderThread

class LoaderThread(ThreadWrapper):
    """Carrega DataFrames extraídos no SQLite e registra tempos de execução."""

    def __init__(
        self,
        in_queue: "queue.Queue[Tuple[str, pd.DataFrame]]",
        db_path: str = "new.db",
        name: str = "LoaderThread",
    ) -> None:
        super().__init__(in_queue, None, name=name)
        self.engine = create_engine(f"sqlite:///{db_path}")
        self._meta = set()
        self.repo_data = RepoData()
        # Arquivo para registrar tempos
        self._times_file_lock = threading.Lock()
        self._times_file_path = Path("times.txt")

    def run(self) -> None:  
        logger.info("%s iniciado", self.name)
        while self.running:
            try:
                # ("produtos", DataFrame) ou ("compras 123", DataFrame)
                key, df = self.in_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                key_name, key_time = self._split_key(key)
                self.repo_data.setStrategy(RepoData.StrategyType.SQL, db=self.engine, table_name=key_name)
                if key_name not in self._meta:
                    df.head(0).to_sql(key_name, self.engine, index=False, if_exists="replace")  # Cria tabela vazia
                    self._meta.add(key_name)
                self.repo_data.loadData(df)
                self._log_time(key_name, key_time)
            except Exception as exc:  
                logger.exception("Erro carregando chave %s: %s", key, exc)
            finally:
                self.in_queue.task_done()
        logger.info("%s finalizado", self.name)

    @staticmethod
    def _split_key(key: str) -> Tuple[str, str]:
        if " " in key:
            name, timestamp = key.split(" ", 1)
        else:
            name, timestamp = key, "0"
        return name, timestamp

    def _log_time(self, key_name: str, key_time: str) -> None:
        now_ns = time.time_ns()
        with self._times_file_lock, self._times_file_path.open("a", encoding="utf-8") as f:
            f.write(f"{key_name} {key_time} {now_ns}\n")


# Função utilitária para enfileirar tarefas

TASKS = [
    ("produtos", ""),
    ("estoque", ""),
    ("compras", ""),
    ("cade", ""),
    ("datacat", ""),
]


def enqueue_initial_tasks(q: "queue.Queue[Tuple[str, str]]") -> None:
    for item in TASKS:
        q.put(item)


# main() – orquestração do pipeline


def main() -> None:  
    extract_q: "queue.Queue[Tuple[str, str]]" = queue.Queue()
    load_q: "queue.Queue[Tuple[str, pd.DataFrame]]" = queue.Queue()

    map_mutex = MapMutex()

    extract_thread = ExtractThread(extract_q, load_q, map_mutex, name="Extractor")
    loader_thread = LoaderThread(load_q, name="Loader")

    extract_thread.start()
    loader_thread.start()

    enqueue_initial_tasks(extract_q)

    try:
        # Aguarda conclusão de todas as tarefas
        extract_q.join()
        load_q.join()
    except KeyboardInterrupt:
        logger.info("Interrupção recebida, finalizando...")
    finally:
        extract_thread.stop()
        loader_thread.stop()
        extract_thread.join()
        loader_thread.join()
        logger.info("Pipeline encerrado com sucesso")


if __name__ == "__main__":
    main()
