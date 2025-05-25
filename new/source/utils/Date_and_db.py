# miniframework.py
"""Mini‑framework que reimplementa as estruturas usadas no seu projeto
(DataFrame, Series, DateTime, filas thread‑safe, mutexes, triggers etc.).

Dependências mínimas: `numpy` (opcional mas recomendado), `sqlite3` (builtin)

"""
from __future__ import annotations

import dataclasses
import logging
import sqlite3
import threading
import time
import types
import warnings
from collections import defaultdict, deque
from contextlib import contextmanager
from datetime import datetime as _dt, timedelta as _td
from pathlib import Path
from queue import Queue as _StdQueue
from typing import Any, Callable, Dict, Generic, Hashable, Iterable, Iterator, List, Optional, Sequence, Tuple, TypeVar, Union
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
import random
import os
from concurrent.futures import as_completed


from new.source.utils.dataframe import Series, DataFrame
from new.source.utils.metrics import METRICS
from new.source.framework.hybrid_pool import HybridPool
from new.source.framework.worker import process_chunk, merge_int, merge_num, merge_pair
from new.source.framework.mvp_pipeline import run_pipeline
from new.source.framework.scheduler import Scheduler



CPU_LIMIT = min(os.cpu_count() or 4, 12)
random.seed(42)  # reprodutibilidade dos exemplos / métricas



# Configuração de logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(threadName)10s] %(levelname)8s: %(message)s")
logger = logging.getLogger(__name__)


# Helpers & Tipos genéricos

T = TypeVar("T")
K = TypeVar("K", bound=Hashable)
V = TypeVar("V")

DefaultObject = Union[int, bool, float, str, "DateTime", "TimeDelta"]

# Decorador para fallback quando numpy não estiver presente -----------------




# TimeDelta & DateTime – wrappers sobre datetime / timedelta

@dataclasses.dataclass
class TimeDelta:

    _delta: _td

    # Construtores auxiliares
    @classmethod
    def seconds(cls, seconds: int) -> "TimeDelta":
        return cls(_td(seconds=seconds))

    @classmethod
    def from_timedelta(cls, td: _td) -> "TimeDelta":
        return cls(td)

    # Cálculos
    def years(self) -> int:  # aprox.
        return self._delta.days // 365

    def months(self) -> int:  # aprox.
        return self._delta.days // 30

    def days(self) -> int:
        return self._delta.days

    def hours(self) -> int:
        return self._delta.seconds // 3600

    def minutes(self) -> int:
        return self._delta.seconds // 60

    def seconds_val(self) -> int:  # evitar nome conflitando com método
        return int(self._delta.total_seconds())

    # Operadores
    def __int__(self) -> int:  # para compat.
        return self.seconds_val()

    def __str__(self) -> str:
        return str(self.seconds_val())

    def __repr__(self) -> str:  # pragma: no cover
        return f"<TimeDelta {self}>"

    # Suporte a aritmética com DateTime
    def __add__(self, other: "DateTime") -> "DateTime":  # noqa: D401
        return other + self  # delega

    def to_timedelta(self) -> _td:
        return self._delta


@dataclasses.dataclass
class DateTime:

    _dt: _dt = dataclasses.field(default_factory=lambda: _dt.fromtimestamp(time.time()))

    # Construtores extras --------------------------------------------------
    @classmethod
    def now(cls) -> "DateTime":
        return cls(_dt.now())

    @classmethod
    def from_timestamp(cls, ts: int | float) -> "DateTime":
        return cls(_dt.fromtimestamp(ts))

    @classmethod
    def from_components(
        cls,
        year: int,
        month: int,
        day: int,
        hour: int = 0,
        minute: int = 0,
        second: int = 0,
    ) -> "DateTime":
        return cls(_dt(year, month, day, hour, minute, second))

    @classmethod
    def from_string(cls, s: str, fmt: str = "%Y-%m-%d %H:%M:%S") -> "DateTime":
        return cls(_dt.strptime(s, fmt))

    def year(self) -> int:
        return self._dt.year

    def month(self) -> int:
        return self._dt.month

    def day(self) -> int:
        return self._dt.day

    def hour(self) -> int:
        return self._dt.hour

    def minute(self) -> int:
        return self._dt.minute

    def second(self) -> int:
        return self._dt.second

    def replace(
        self,
        year: int = -1,
        month: int = -1,
        day: int = -1,
        hour: int = -1,
        minute: int = -1,
        second: int = -1,
    ) -> None:
        kwargs: Dict[str, int] = {}
        if year != -1:
            kwargs["year"] = year
        if month != -1:
            kwargs["month"] = month
        if day != -1:
            kwargs["day"] = day
        if hour != -1:
            kwargs["hour"] = hour
        if minute != -1:
            kwargs["minute"] = minute
        if second != -1:
            kwargs["second"] = second
        self._dt = self._dt.replace(**kwargs)

    def strftime(self, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
        return self._dt.strftime(fmt)

    # Operadores -----------------------------------------------------------
    def __sub__(self, other: "DateTime") -> TimeDelta:  # noqa: D401
        return TimeDelta.from_timedelta(self._dt - other._dt)

    def __add__(self, delta: TimeDelta) -> "DateTime":  # noqa: D401
        return DateTime(self._dt + delta.to_timedelta())

    def __lt__(self, other: "DateTime") -> bool:
        return self._dt < other._dt

    def __le__(self, other: "DateTime") -> bool:
        return self._dt <= other._dt

    def __gt__(self, other: "DateTime") -> bool:
        return self._dt > other._dt

    def __ge__(self, other: "DateTime") -> bool:
        return self._dt >= other._dt

    def __eq__(self, other: object) -> bool:  
        return isinstance(other, DateTime) and self._dt == other._dt

    def __ne__(self, other: object) -> bool:  
        return not self.__eq__(other)

    def __str__(self) -> str:
        return self.strftime()

    def __repr__(self) -> str:  # pragma: no cover
        return f"<DateTime {self}>"


# Series – estrutura unidimensional (lista tipada por DefaultObject)


# SQLite DB Helper – equivalente ao db.h

class DB:
    """Wrapper minimalista em torno de `sqlite3.Connection`.  Usa transações."""

    def __init__(self, db_path: str | Path):
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._lock = threading.Lock()
        self._create_tables()

    def _create_tables(self) -> None:
        cur = self.conn.cursor()
        for t in ("T1", "T2", "T3", "T4", "T5", "T6"):
            cur.execute(f"CREATE TABLE IF NOT EXISTS {t} (datetime TEXT PRIMARY KEY, count FLOAT);")
        self.conn.commit()

    def insert_data(self, table: str, datetime_str: str, count: float) -> None:
        with self._lock:
            cur = self.conn.cursor()
            cur.execute(
                f"INSERT OR REPLACE INTO {table} (datetime, count) VALUES (?, ?);",
                (datetime_str, count),
            )
            self.conn.commit()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    # Para usar com `with DB(...) as db:` ---------------------------------
    def __enter__(self) -> "DB":
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: D401
        self.close()

# ⇨ ADICIONAR antes da seção Queue / MapMutex

# MÉTRICAS (contadores + histogramas) – thread‑safe



# Queue – fila thread‑safe com capacidade (Condition + deque opcional)


# Handler – roteador entre várias filas de saída



# Trigger – base + TimeTrigger / RequestTrigger

# ⇨ ADICIONAR antes do bloco “Self‑test”

# TASK SYSTEM + SCHEDULER Round‑Robin

TaskFn = Callable[[], Any]
_TASKS: dict[str, TaskFn] = {}
_DEPS: dict[str, set[str]] = defaultdict(set)

def task(*, depends: Sequence[str] | None = None):
    """Decorador que registra a função como task do DAG."""
    def deco(fn: TaskFn) -> TaskFn:
        name = fn.__name__
        _TASKS[name] = fn
        for d in depends or []:
            _DEPS[name].add(d)
        return fn
    return deco




# Self‑test rápido quando executado como script

# ⇨ SUBSTITUIR o bloco “Self‑test rápido …” inteiro pelo abaixo
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # ------- exemplo de DataFrame / Series antigo ----------
    s1, s2 = Series([1, 2]), Series([10, 20])
    df = DataFrame(["x", "y"], [s1, s2])
    df.append({"x": 3, "y": 30})
    df.print()

    # ------- DAG de tasks usando scheduler + hybrid pool ---
    @task()
    def extract():
        logger.info("extract ...")
        time.sleep(0.3)
        METRICS.counter("extract_calls").inc()

    @task(depends=["extract"])
    def transform():
        logger.info("transform ...")
        time.sleep(0.5)
        METRICS.histogram("transform_latency").observe(0.5)

    @task(depends=["transform"])
    def load():
        logger.info("load ...")
        time.sleep(0.2)

    sched = Scheduler()
    # Define a simple HybridPool class as a placeholder
    pool = HybridPool()
    futures: dict[str, Future] = {}

    # Envio inicial
    while True:
        n = sched.next()
        if n:
            futures[n] = pool.submit(_TASKS[n])
        done = [k for k, f in futures.items() if f.done()]
        for k in done:
            sched.done(k)
            del futures[k]
        if not futures and not n:
            break
        time.sleep(0.05)

    pool.shutdown()
    print("--- MÉTRICAS ---\n", METRICS.report())

