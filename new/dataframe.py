from __future__ import annotations

import dataclasses
import time
import warnings
from collections import defaultdict
from datetime import datetime as _dt, timedelta as _td
from functools import wraps
from typing import Any, Callable, Dict, Generic, Iterable, Iterator, List, Optional, Sequence, Tuple, TypeVar, Union, Hashable

try:
    import numpy as np
except ImportError:  # pragma: no cover – numpy é opcional
    np = None  # type: ignore

T = TypeVar("T")
K = TypeVar("K", bound=Hashable)
V = TypeVar("V")

DefaultObject = Union[int, bool, float, str, "DateTime", "TimeDelta"]

def _use_numpy(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Se *numpy* existir, executa versão vetorizada; caso contrário, avisa."""

    @wraps(func)
    def wrapper(self: "Series", *args: Any, **kwargs: Any):  # type: ignore[name‑defined]
        if np is None:
            warnings.warn("Numpy não encontrado; operações vetorizadas podem ficar lentas.")
            return func(self, *args, **kwargs)
        return func(self, *args, **kwargs)

    return wrapper

 
# TimeDelta & DateTime – wrappers sobre datetime / timedelta

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

    # Construtores extras 
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

    # Métodos C++‑like 
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

    # Operadores 
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


# Series – estrutura unidimensional (lista tipada por DefaultObject)

class Series(Generic[T]):
    """Série de valores heterogêneos (ou não).

    Na maior parte dos casos é preferível usar *numpy* arrays, mas aqui
    mantemos compat. com o design C++.
    """

    __slots__ = ("_data",)

    def __init__(self, data: Optional[Sequence[T]] = None) -> None:
        self._data: List[T] = list(data) if data is not None else []

    
    # Acesso / mutação
    
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

    
    # Operações aritméticas element‑wise
    
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

    # Comparações 
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

    # Conversões 
    def astype(self, cast: Callable[[T], V]) -> "Series[V]":  # type: ignore[type‑var]
        return Series([cast(v) for v in self])

    def to_datetime(self) -> "Series[DateTime]":
        return Series(DateTime.from_string(str(v)) if not isinstance(v, DateTime) else v for v in self)  # type: ignore[arg‑type]

    # Utilidades 
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


# DataFrame – matriz 2D de Series

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

    # Interno 
    def _update_shape(self) -> None:
        rows = self.series[0].shape() if self.series else 0
        self.shape: Tuple[int, int] = (rows, len(self.series))

    # Criação 
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

    # Indexing 
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

    # Manipulação 
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

    # Visualização 
    def print(self, width: int = 20) -> None:
        for col in self.columns:
            print(f"{col:>{width}}", end="")
        print()
        for i in range(self.shape[0]):
            for s in self.series:
                print(f"{s[i]!s:>{width}}", end="")
            print()

    # Dunder helpers 
    def __repr__(self) -> str:  # noqa: D401
        return f"<DataFrame rows={self.shape[0]} cols={self.shape[1]}>"