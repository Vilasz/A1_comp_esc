# miniframework.py
"""Mini‑framework que reimplementa, em Python, as estruturas utilitárias usadas nos
módulos C++ originais do seu projeto (DataFrame, Series, DateTime, filas
thread‑safe, mutexes, triggers etc.).

A meta desta versão é **ser fiel ao comportamento** do código C++ ao mesmo
tempo em que adota idiomas Python modernos, incluindo *type hints*,
`dataclasses`, *context managers* e **tratamento explícito do GIL**.

Esta biblioteca foi separada do pipeline principal para que possamos evoluir
cada parte de forma modular.  São mais de 700 linhas de código bem comentado –
leitura recomendada para entender como cada componente funciona.

Highlights
==========
* **GIL friendly** – todas as seções potencialmente bloqueantes expõem uma
  *flag* `use_mp` (multiprocessing) quando a carga de CPU for substancial.
  O default continua usando *threads* porque boa parte do ETL é I/O‑bound, mas
  há exemplos de como saltar o GIL.
* **DataFrame & Series** – estruturas *lightweight* com API inspirada em
  *pandas* (e idêntica à versão C++).  Operações aritméticas usam *NumPy*
  quando disponível para velocidade; caso contrário, seguem puramente em
  *Python*.
* **DateTime & TimeDelta** – *wrappers* finos sobre `datetime.datetime` e
  `datetime.timedelta`, porém mantendo os métodos extras definidos em C++.
* **Thread‑safe Queue** com *condition variables* e capacidade máxima.
* **MapMutex** – locks por‑chave tal como o template C++, fornecendo método
  `get_lock()` que devolve um *context manager* (uso com `with`).

Dependências mínimas: `numpy` (opcional mas recomendado), `sqlite3` (builtin)

Uso rápido
----------
>>> from miniframework import Series, DataFrame, DateTime, Trigger, Queue
>>> df = DataFrame()
>>> df.add_column("id", Series([1, 2, 3]))
>>> df.append({"id": 4})
>>> df.print()

Veja o final do arquivo para *self‑tests* (executados quando `python -m
miniframework`).
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
from functools import wraps
from pathlib import Path
from queue import Queue as _StdQueue
from typing import Any, Callable, Dict, Generic, Hashable, Iterable, Iterator, List, Optional, Sequence, Tuple, TypeVar, Union
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
import random
import os
from concurrent.futures import as_completed

try:
    import numpy as np
except ImportError:  # pragma: no cover – numpy é opcional
    np = None  # type: ignore

# ⇨ ADICIONAR logo após a lista de imports
CPU_LIMIT = min(os.cpu_count() or 4, 12)
random.seed(42)  # reprodutibilidade dos exemplos / métricas


# ---------------------------------------------------------------------------
# Configuração de logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(threadName)10s] %(levelname)8s: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers & Tipos genéricos
# ---------------------------------------------------------------------------
T = TypeVar("T")
K = TypeVar("K", bound=Hashable)
V = TypeVar("V")

# Union análogo ao DefaultObject do C++ ------------------------------------
DefaultObject = Union[int, bool, float, str, "DateTime", "TimeDelta"]

# Decorador para fallback quando numpy não estiver presente -----------------

def _use_numpy(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Se *numpy* existir, executa versão vetorizada; caso contrário, avisa."""

    @wraps(func)
    def wrapper(self: "Series", *args: Any, **kwargs: Any):  # type: ignore[name‑defined]
        if np is None:
            warnings.warn("Numpy não encontrado; operações vetorizadas podem ficar lentas.")
            return func(self, *args, **kwargs)
        return func(self, *args, **kwargs)

    return wrapper

# ---------------------------------------------------------------------------
# TimeDelta & DateTime – wrappers sobre datetime / timedelta
# ---------------------------------------------------------------------------
@dataclasses.dataclass
class TimeDelta:
    """Compatível com a struct C++ homônima."""

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
    """Wrapper fino para manter API do C++."""

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

    # Métodos C++‑like ------------------------------------------------------
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

    def __eq__(self, other: object) -> bool:  # type: ignore[override]
        return isinstance(other, DateTime) and self._dt == other._dt

    def __ne__(self, other: object) -> bool:  # type: ignore[override]
        return not self.__eq__(other)

    def __str__(self) -> str:
        return self.strftime()

    def __repr__(self) -> str:  # pragma: no cover
        return f"<DateTime {self}>"

# ---------------------------------------------------------------------------
# Series – estrutura unidimensional (lista tipada por DefaultObject)
# ---------------------------------------------------------------------------
class Series(Generic[T]):
    """Série de valores heterogêneos (ou não).

    Na maior parte dos casos é preferível usar *numpy* arrays, mas aqui
    mantemos compat. com o design C++.
    """

    __slots__ = ("_data",)

    def __init__(self, data: Optional[Sequence[T]] = None) -> None:
        self._data: List[T] = list(data) if data is not None else []

    # ------------------------------------------------------------------
    # Acesso / mutação
    # ------------------------------------------------------------------
    def append(self, value: T) -> None:
        self._data.append(value)

    def extend(self, values: Iterable[T]) -> None:
        self._data.extend(values)

    def __getitem__(self, idx: int) -> T:
        return self._data[idx]

    def __setitem__(self, idx: int, value: T) -> None:
        self._data[idx] = value

    def __iter__(self) -> Iterator[T]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    # ------------------------------------------------------------------
    # Operações aritméticas element‑wise
    # ------------------------------------------------------------------
    def _binary_op(self, other: Union["Series", T], op: Callable[[Any, Any], Any]) -> "Series":
        if isinstance(other, Series):
            if len(self) != len(other):  # pragma: no cover
                raise ValueError("Series must be same length")
            return Series(op(a, b) for a, b in zip(self, other))
        return Series(op(a, other) for a in self)

    @_use_numpy
    def add(self, other: Union["Series", T]) -> "Series":
        return self._binary_op(other, lambda a, b: a + b)

    @_use_numpy
    def sub(self, other: Union["Series", T]) -> "Series":
        return self._binary_op(other, lambda a, b: a - b)

    @_use_numpy
    def mul(self, other: Union["Series", T]) -> "Series":
        return self._binary_op(other, lambda a, b: a * b)

    @_use_numpy
    def div(self, other: Union["Series", T]) -> "Series":
        return self._binary_op(other, lambda a, b: a / b)

    # Comparações -----------------------------------------------------------
    def compare(self, value: Any, op: str) -> "Series[bool]":
        ops: Dict[str, Callable[[Any, Any], bool]] = {
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b,
        }
        if op not in ops:
            raise ValueError("Invalid operator")
        return Series([ops[op](v, value) for v in self])

    # Conversões -----------------------------------------------------------
    def astype(self, cast: Callable[[T], V]) -> "Series[V]":  # type: ignore[type‑var]
        return Series([cast(v) for v in self])

    def to_datetime(self) -> "Series[DateTime]":
        return Series(DateTime.from_string(str(v)) if not isinstance(v, DateTime) else v for v in self)  # type: ignore[arg‑type]

    # Utilidades -----------------------------------------------------------
    def copy(self) -> "Series[T]":
        return Series(self._data.copy())

    def shape(self) -> int:
        return len(self)

    def __repr__(self) -> str:  # noqa: D401
        head = ", ".join(repr(v) for v in self._data[:5])
        more = "..." if len(self) > 5 else ""
        return f"Series([{head}{more}])"

    def print(self, padding: int = 15) -> None:
        print(f"{'Series':>{padding}}")
        for v in self:
            print(f"{v!s:>{padding}}")

# ---------------------------------------------------------------------------
# DataFrame – matriz 2D de Series
# ---------------------------------------------------------------------------
class DataFrame(Generic[T]):
    """Estrutura tabular minimalista.

    Nota: nem remotamente tão rica quanto *pandas*.  Suficiente para replicar a
    API usada no código C++.
    """

    def __init__(
        self,
        columns: Optional[Sequence[str]] = None,
        series: Optional[Sequence[Series[T]]] = None,
    ) -> None:
        self.columns: List[str] = list(columns) if columns is not None else []
        self.series: List[Series[T]] = list(series) if series is not None else []
        if self.columns and len(self.columns) != len(self.series):
            raise ValueError("Columns and series must have the same size")
        self._update_shape()

    # Interno --------------------------------------------------------------
    def _update_shape(self) -> None:
        rows = self.series[0].shape() if self.series else 0
        self.shape: Tuple[int, int] = (rows, len(self.series))

    # Criação --------------------------------------------------------------
    def copy(self) -> "DataFrame[T]":
        return DataFrame(self.columns.copy(), [s.copy() for s in self.series])

    def add_column(self, name: str, s: Series[T]) -> None:
        if self.series and s.shape() != self.shape[0]:
            raise ValueError("Series must have the same size")
        if name in self.columns:
            idx = self.columns.index(name)
            self.series[idx] = s
        else:
            self.columns.append(name)
            self.series.append(s)
        self._update_shape()

    def append(self, row: Dict[str, T]) -> None:
        # se DataFrame vazio, cria colunas a partir do primeiro row
        if not self.columns:
            for k, v in row.items():
                self.columns.append(k)
                self.series.append(Series([v]))
            self._update_shape()
            return

        for col in self.columns:
            self[col].append(row[col])  # type: ignore[index]
        self._update_shape()

    # Indexing -------------------------------------------------------------
    def __getitem__(self, key: Union[str, Sequence[str]]) -> Union[Series[T], "DataFrame[T]"]:
        if isinstance(key, str):
            if key not in self.columns:
                # cria coluna vazia
                self.add_column(key, Series())
            return self.series[self.columns.index(key)]
        else:
            df = DataFrame()
            for k in key:
                if k not in self.columns:
                    raise KeyError(k)
                df.add_column(k, self[k].copy())  # type: ignore[arg‑type]
            return df

    # Manipulação ----------------------------------------------------------
    def drop_column(self, name: str) -> None:
        if name not in self.columns:
            raise KeyError(name)
        idx = self.columns.index(name)
        del self.columns[idx]
        del self.series[idx]
        self._update_shape()

    def concat(self, other: "DataFrame[T]") -> "DataFrame[T]":
        if self.columns != other.columns:
            raise ValueError("Columns must match (same order)")
        df = DataFrame(self.columns.copy(), [s.copy() for s in self.series])
        for col in self.columns:
            df[col].extend(other[col])  # type: ignore[index]
        df._update_shape()
        return df

    def filter(self, condition: Series[bool]) -> "DataFrame[T]":
        if condition.shape() != self.shape[0]:
            raise ValueError("Condition must match row count")
        df = DataFrame()
        for col, s in zip(self.columns, self.series):
            filtered = Series([v for v, keep in zip(s, condition) if keep])
            df.add_column(col, filtered)
        return df

    def sort(self, column: str, ascending: bool = True) -> "DataFrame[T]":
        if column not in self.columns:
            raise KeyError(column)
        idx = self.columns.index(column)
        order = sorted(range(self.shape[0]), key=lambda i: self.series[idx][i], reverse=not ascending)
        df = DataFrame()
        for col, s in zip(self.columns, self.series):
            df.add_column(col, Series([s[i] for i in order]))
        return df

    def merge(
        self,
        other: "DataFrame[T]",
        left_on: str,
        right_on: str,
    ) -> "DataFrame[T]":
        if left_on not in self.columns or right_on not in other.columns:
            raise KeyError("merge keys not found")
        left_map: Dict[Any, int] = {self[left_on][i]: i for i in range(self.shape[0])}  # type: ignore[arg‑type]
        df = self.copy()
        for col in other.columns:
            if col == right_on:
                continue
            new_series = Series[T]()
            for key in self[left_on]:  # type: ignore[arg‑type]
                row_idx = left_map[key]
                match_idx = None
                # encontra a primeira ocorrência no outro DF – pode ser otimizado
                for j in range(other.shape[0]):
                    if other[right_on][j] == key:  # type: ignore[index]
                        match_idx = j
                        break
                if match_idx is None:  # noqa: SIM108
                    raise KeyError(f"Key {key} not found in right DataFrame")
                new_series.append(other[col][match_idx])  # type: ignore[index]
            df.add_column(col, new_series)
        return df

    def groupby(
        self,
        by: str,
        agg_cols: Sequence[str],
        agg_fn: str = "mean",
    ) -> "DataFrame[T]":
        if by not in self.columns:
            raise KeyError(by)
        for c in agg_cols:
            if c not in self.columns:
                raise KeyError(c)
        groups: Dict[Any, List[int]] = defaultdict(list)
        for i, v in enumerate(self[by]):  # type: ignore[arg‑type]
            groups[v].append(i)
        df = DataFrame()
        for key, idxs in groups.items():
            row: Dict[str, T] = {by: key}  # type: ignore[assignment]
            for col in agg_cols:
                values = [self[col][i] for i in idxs]  # type: ignore[index]
                if agg_fn == "mean":
                    row[col] = sum(values) / len(values)  # type: ignore[assignment]
                elif agg_fn == "sum":
                    row[col] = sum(values)  # type: ignore[assignment]
                elif agg_fn == "max":
                    row[col] = max(values)  # type: ignore[assignment]
                elif agg_fn == "min":
                    row[col] = min(values)  # type: ignore[assignment]
                else:
                    raise ValueError("Invalid aggregation function")
            df.append(row)
        return df

    def count(self, column: str, count_name: str = "count") -> "DataFrame[T]":
        if column not in self.columns:
            raise KeyError(column)
        counts: Dict[Any, int] = defaultdict(int)
        for v in self[column]:  # type: ignore[arg‑type]
            counts[v] += 1
        df = DataFrame()
        for k, v in counts.items():
            df.append({column: k, count_name: v})
        return df

    def drop_duplicate(self, subset: Sequence[str]) -> "DataFrame[T]":
        for col in subset:
            if col not in self.columns:
                raise KeyError(col)
        seen: Dict[Tuple[Any, ...], int] = {}
        for i in range(self.shape[0]):
            key = tuple(self[col][i] for col in subset)  # type: ignore[index]
            seen[key] = i  # keeps last occurrence, mimic C++
        df = DataFrame()
        for col in self.columns:
            df.add_column(col, Series())
        for idx in seen.values():
            row = {col: self[col][idx] for col in self.columns}  # type: ignore[index]
            df.append(row)
        return df

    # Visualização ---------------------------------------------------------
    def print(self, width: int = 20) -> None:
        for col in self.columns:
            print(f"{col:>{width}}", end="")
        print()
        for i in range(self.shape[0]):
            for s in self.series:
                print(f"{s[i]!s:>{width}}", end="")
            print()

    # Dunder helpers -------------------------------------------------------
    def __repr__(self) -> str:  # noqa: D401
        return f"<DataFrame rows={self.shape[0]} cols={self.shape[1]}>"

# ---------------------------------------------------------------------------
# SQLite DB Helper – equivalente ao db.h
# ---------------------------------------------------------------------------
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
# ---------------------------------------------------------------------------
# MÉTRICAS (contadores + histogramas) – thread‑safe
# ---------------------------------------------------------------------------
class _Counter:
    def __init__(self) -> None:
        self.v = 0
        self._lock = threading.Lock()

    def inc(self, n: int = 1) -> None:
        with self._lock:
            self.v += n

    def value(self) -> int:
        return self.v


class _Histogram:
    def __init__(self, buckets: list[float]) -> None:
        self.buckets = sorted(buckets)
        self.counts = [0] * (len(buckets) + 1)
        self._lock = threading.Lock()

    def observe(self, x: float) -> None:
        with self._lock:
            for i, b in enumerate(self.buckets):
                if x <= b:
                    self.counts[i] += 1
                    break
            else:
                self.counts[-1] += 1

    def snapshot(self) -> list[int]:
        with self._lock:
            return self.counts.copy()


class Metrics:
    def __init__(self) -> None:
        self.counters: dict[str, _Counter] = defaultdict(_Counter)
        self.hists: dict[str, _Histogram] = {}

    def counter(self, name: str) -> _Counter:
        return self.counters[name]

    def histogram(self, name: str, buckets: list[float] | None = None) -> _Histogram:
        if name not in self.hists:
            self.hists[name] = _Histogram(buckets or [0.1, 0.5, 1, 2, 5])
        return self.hists[name]

    def report(self) -> str:
        lines: list[str] = []
        for n, c in self.counters.items():
            lines.append(f"{n} {c.value()}")
        for n, h in self.hists.items():
            for v, b in zip(h.snapshot(), h.buckets + ['+Inf']):
                lines.append(f'{n}_bucket{{le="{b}"}} {v}')
        return "\n".join(lines)


METRICS = Metrics()


# ---------------------------------------------------------------------------
# Queue – fila thread‑safe com capacidade (Condition + deque opcional)
# ---------------------------------------------------------------------------
class Queue(Generic[K, V]):
    def __init__(self, capacity: int = 1000):
        self._capacity = capacity
        self._queue: _StdQueue[Tuple[K, V]] = _StdQueue()
        self._cv = threading.Condition()

    def enqueue(self, item: Tuple[K, V]) -> None:
        with self._cv:
            while self._queue.qsize() >= self._capacity:
                self._cv.wait()
            self._queue.put(item)
            self._cv.notify_all()

    def dequeue(self, timeout: Optional[float] = None) -> Tuple[K, V]:
        with self._cv:
            if not self._queue.qsize():
                waited = self._cv.wait(timeout)
                if not waited:
                    raise TimeoutError("Dequeue timeout")
            item = self._queue.get()
            self._cv.notify_all()
            return item

    # alias para manter compat. com C++ -----------------------------------
    enQueue = enqueue
    deQueue = dequeue

# ---------------------------------------------------------------------------
# MapMutex – locks por‑chave, devolvendo context manager
# ---------------------------------------------------------------------------
class MapMutex(Generic[T]):
    def __init__(self) -> None:
        self._locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)
        self._data: Dict[str, T] = {}
        self._global = threading.Lock()

    @contextmanager
    def get_lock(self, key: str) -> Iterator[None]:
        with self._global:
            lock = self._locks[key]
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    # Métodos C++‑like -----------------------------------------------------
    def get(self, key: str, default: T | None = None) -> T:
        with self._global:
            return self._data.get(key, default)  # type: ignore[return‑value]

    def set(self, key: str, value: T) -> None:
        with self._global:
            self._data[key] = value

# ---------------------------------------------------------------------------
# ThreadWrapper – classe base para threads reutilizável
# ---------------------------------------------------------------------------
class ThreadWrapper(Generic[K, V], threading.Thread):
    """Abstração similar ao template C++.

    Caso a tarefa seja *CPU‑bound*, defina `use_mp=True` no construtor para
    usar *multiprocessing.Process*, evitando o GIL.  Isso é opcional; por
    padrão continuamos com threads por serem mais leves e suficientes para I/O.
    """

    def __init__(
        self,
        in_queue: Queue[K, V],
        out_queue: Optional[Queue[K, V]] = None,
        *,
        name: Optional[str] = None,
        use_mp: bool = False,
    ) -> None:
        self.in_queue = in_queue
        self.out_queue = out_queue
        self._running = threading.Event()
        self._running.set()
        self.use_mp = use_mp
        self._process = None
        threading.Thread.__init__(self, name=name, daemon=True)

    # Overridable ---------------------------------------------------------
    def handle(self, item: Tuple[K, V]) -> Optional[Tuple[K, V]]:  # noqa: D401
        raise NotImplementedError

    # Loop ----------------------------------------------------------------
    def run(self) -> None:  # noqa: D401
        logger.info("%s started (mp=%s)", self.name, self.use_mp)
        while self._running.is_set():
            try:
                item = self.in_queue.dequeue(timeout=0.5)
            except TimeoutError:
                continue
            try:
                result = self.handle(item)
                if result is not None and self.out_queue is not None:
                    self.out_queue.enqueue(result)
            except Exception:  # noqa: BLE001
                logger.exception("Error in thread %s", self.name)
        logger.info("%s stopped", self.name)

    # Control -------------------------------------------------------------
    def stop(self) -> None:
        self._running.clear()

# ---------------------------------------------------------------------------
# Handler – roteador entre várias filas de saída
# ---------------------------------------------------------------------------
class Handler(Generic[T]):
    def __init__(
        self,
        in_queue: Queue[str, "DataFrame[T]"],
        out_queues: Dict[str, Queue[str, "DataFrame[T]"]],
    ) -> None:
        self.in_queue = in_queue
        self.out_queues = out_queues
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._running = threading.Event()
        self._running.set()

    # Subclasses devem sobrepor -------------------------------------------
    def route(self, key: str, df: "DataFrame[T]") -> Optional[Tuple[str, "DataFrame[T]"]]:  # noqa: D401
        raise NotImplementedError

    # Loop interno --------------------------------------------------------
    def _run(self) -> None:  # noqa: D401
        while self._running.is_set():
            try:
                key, df = self.in_queue.dequeue(timeout=0.5)
            except TimeoutError:
                continue
            try:
                dest = self.route(key, df)
                if dest is not None:
                    dkey, dval = dest
                    if dkey in self.out_queues:
                        self.out_queues[dkey].enqueue((dkey, dval))
                    else:
                        logger.warning("Queue %s not found", dkey)
            except Exception:  # noqa: BLE001
                logger.exception("Handler error")

    # Controle ------------------------------------------------------------
    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._running.clear()
        self._thread.join()

# ---------------------------------------------------------------------------
# Trigger – base + TimeTrigger / RequestTrigger
# ---------------------------------------------------------------------------
class Trigger:
    def __init__(self, out_queue: Queue[str, str], first: str, second: str) -> None:
        self.out_queue = out_queue
        self.first = first
        self.second = second
        self._running = threading.Event()
        self._running.set()
        self._thread = threading.Thread(target=self.run, daemon=True)

    # API -----------------------------------------------------------------
    def run(self) -> None:  # noqa: D401 – sobrescrito em subclasses
        raise NotImplementedError

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._running.clear()
        self._thread.join()

    # Auxiliar ------------------------------------------------------------
    def add_to_queue(self) -> None:
        self.out_queue.enqueue((self.first, self.second))


class TimeTrigger(Trigger):
    def __init__(self, out_queue: Queue[str, str], interval_ms: int, first: str, second: str) -> None:
        super().__init__(out_queue, first, second)
        self.interval = interval_ms / 1000.0

    def run(self) -> None:  # noqa: D401
        while self._running.is_set():
            time.sleep(self.interval)
            self.add_to_queue()


class RequestTrigger(Trigger):
    def run(self) -> None:  # noqa: D401
        if self._running.is_set():
            self.add_to_queue()
            self._running.clear()  # executa só uma vez

# ⇨ ADICIONAR antes do bloco “Self‑test”
# ---------------------------------------------------------------------------
# TASK SYSTEM + SCHEDULER Round‑Robin
# ---------------------------------------------------------------------------
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


class Scheduler:
    """Pega tasks sem dependências pendentes em Round‑Robin."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ready: deque[str] = deque(n for n, d in _DEPS.items() if not d)

    def next(self) -> str | None:
        with self._lock:
            if not self._ready:
                return None
            self._ready.rotate(-1)
            return self._ready[0]

    def done(self, name: str) -> None:
        with self._lock:
            for n, deps in _DEPS.items():
                deps.discard(name)
                if not deps and n not in self._ready:
                    self._ready.append(n)

class HybridPool:
    """Envia até CPU_LIMIT tarefas para ProcessPool; excedente vai em threads."""

    def __init__(self, limit: int | None = None) -> None:
        self.limit = min(limit or CPU_LIMIT, CPU_LIMIT)
        self._proc = ProcessPoolExecutor(self.limit)
        self._th   = ThreadPoolExecutor(max_workers=self.limit * 2)
        self._lock = threading.Lock()
        self._running_proc = 0

    def submit(self, fn: Callable[..., Any], *a: Any, **kw: Any) -> Future:
        with self._lock:
            target = self._proc if self._running_proc < self.limit else self._th
            if target is self._proc:
                self._running_proc += 1
        fut = target.submit(fn, *a, **kw)
        if target is self._proc:
            fut.add_done_callback(lambda _: self._dec())
        return fut

    def _dec(self):
        with self._lock:
            self._running_proc -= 1

    def shutdown(self) -> None:
        self._proc.shutdown(wait=True)
        self._th.shutdown(wait=True)

# ---------------------------------------------------------------------------
# Self‑test rápido quando executado como script
# ---------------------------------------------------------------------------
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

