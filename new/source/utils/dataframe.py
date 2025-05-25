# dataframe.py (Enhanced and Feature-Rich)
from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import (Any, Callable, Dict, Iterable, List, Optional, Tuple, Union,
                    Sequence, TypeVar, Generic)
from collections import defaultdict, OrderedDict # For GroupBy and preserving order

LOG_FORMAT = "%(asctime)s [%(levelname)8s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

T = TypeVar('T')  # Generic type for Series data
V = TypeVar('V')  # Generic type for cast results in Series

# --- Enhanced Series Class ---
class Series(Generic[T]):
    def __init__(self, name: str, data: Optional[Sequence[T]] = None) -> None:
        self.name: str = name
        # Ensure data is stored as a mutable list
        self.data: List[T] = list(data) if data is not None else []

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, index: int) -> T:
        if not 0 <= index < len(self.data):
            raise IndexError(f"Series index {index} out of range for series '{self.name}' of length {len(self.data)}")
        return self.data[index]

    def __setitem__(self, index: int, value: T) -> None:
        if not 0 <= index < len(self.data):
            if index == len(self.data): # Allow appending via index
                self.data.append(value)
            else:
                raise IndexError(f"Series index {index} out of range for series '{self.name}' of length {len(self.data)}")
        else:
            self.data[index] = value

    def append(self, value: T) -> None:
        self.data.append(value)

    def extend(self, values: Iterable[T]) -> None:
        self.data.extend(values)

    def copy(self) -> 'Series[T]':
        return Series(name=self.name, data=list(self.data))

    def astype(self, cast_func: Callable[[T], V]) -> 'Series[V]':
        new_data: List[V] = []
        for item in self.data:
            try:
                new_data.append(cast_func(item))
            except (ValueError, TypeError) as e:
                logger.debug(f"Could not cast value '{item}' using {cast_func.__name__} for series '{self.name}': {e}. Appending as is or None.")
                new_data.append(item if isinstance(item, type(None)) else None) # type: ignore
        return Series(name=self.name, data=new_data)

    def _apply_op(self, other: Union['Series[Any]', Any], op_func: Callable[[Any, Any], Any], op_name:str="operation") -> 'Series[Any]':
        new_data: List[Any] = []
        if isinstance(other, Series):
            if len(self) != len(other):
                raise ValueError(f"Series lengths must match for '{op_name}'. Self: {len(self)}, Other: {len(other)}")
            for s_val, o_val in zip(self.data, other.data):
                try:
                    new_data.append(op_func(s_val, o_val))
                except TypeError: # Handle operations between incompatible types, e.g. None + int
                    new_data.append(None)
        else: # Scalar operation
            for s_val in self.data:
                try:
                    new_data.append(op_func(s_val, other))
                except TypeError:
                    new_data.append(None)
        return Series(name=self.name, data=new_data)

    def __add__(self, other: Union['Series[Any]', Any]) -> 'Series[Any]':
        return self._apply_op(other, lambda a, b: a + b, "addition")
    def __sub__(self, other: Union['Series[Any]', Any]) -> 'Series[Any]':
        return self._apply_op(other, lambda a, b: a - b, "subtraction")
    def __mul__(self, other: Union['Series[Any]', Any]) -> 'Series[Any]':
        return self._apply_op(other, lambda a, b: a * b, "multiplication")
    def __truediv__(self, other: Union['Series[Any]', Any]) -> 'Series[Any]':
        return self._apply_op(other, lambda a, b: a / b if b != 0 else float('nan'), "division") # Basic zero handling
    def __floordiv__(self, other: Union['Series[Any]', Any]) -> 'Series[Any]':
        return self._apply_op(other, lambda a, b: a // b if b != 0 else float('nan'), "floor division")

    def _compare_op(self, other: Union['Series[Any]', Any], op_func: Callable[[Any, Any], bool]) -> 'Series[bool]':
        # Comparison ops should generally not raise TypeError but return False if types are incomparable
        # or if None is involved, unless explicitly handled. Python's default ops do this.
        new_data: List[bool] = []
        if isinstance(other, Series):
            if len(self) != len(other):
                raise ValueError("Series lengths must match for comparison.")
            for s_val, o_val in zip(self.data, other.data):
                try:
                    new_data.append(op_func(s_val, o_val))
                except TypeError: new_data.append(False) # e.g. comparing int to None
        else:
            for s_val in self.data:
                try:
                    new_data.append(op_func(s_val, other))
                except TypeError: new_data.append(False)
        return Series(name=self.name + "_bool", data=new_data)

    def __eq__(self, other: Union['Series[Any]', Any]) -> 'Series[bool]': # type: ignore
        return self._compare_op(other, lambda a, b: a == b)
    def __ne__(self, other: Union['Series[Any]', Any]) -> 'Series[bool]': # type: ignore
        return self._compare_op(other, lambda a, b: a != b)
    def __lt__(self, other: Union['Series[Any]', Any]) -> 'Series[bool]':
        return self._compare_op(other, lambda a, b: a < b)
    def __le__(self, other: Union['Series[Any]', Any]) -> 'Series[bool]':
        return self._compare_op(other, lambda a, b: a <= b)
    def __gt__(self, other: Union['Series[Any]', Any]) -> 'Series[bool]':
        return self._compare_op(other, lambda a, b: a > b)
    def __ge__(self, other: Union['Series[Any]', Any]) -> 'Series[bool]':
        return self._compare_op(other, lambda a, b: a >= b)

    def is_null(self) -> 'Series[bool]':
        return Series(name=self.name + "_isnull", data=[x is None for x in self.data])
    def fill_na(self, value: Any, inplace: bool = False) -> Optional['Series[T]']:
        new_data = [value if x is None else x for x in self.data]
        if inplace:
            self.data = new_data
            return None
        return Series(name=self.name, data=new_data)
    def drop_na(self) -> 'Series[T]':
        return Series(name=self.name, data=[x for x in self.data if x is not None])
    def unique(self) -> List[T]:
        seen = set()
        uniques = []
        for item in self.data:
            # Basic attempt to handle unhashable types for set inclusion
            hashable_item = item
            if isinstance(item, list): hashable_item = tuple(item)
            elif isinstance(item, dict): hashable_item = tuple(sorted(item.items()))

            try:
                if hashable_item not in seen:
                    seen.add(hashable_item)
                    uniques.append(item)
            except TypeError: # Fallback if item is still unhashable
                if item not in uniques: # Slower check for truly unhashable
                    uniques.append(item)
        return uniques
    def value_counts(self) -> Dict[T, int]:
        counts = defaultdict(int)
        for item in self.data:
            counts[item] +=1
        return dict(sorted(counts.items(), key=lambda x: x[1], reverse=True))

    def __str__(self) -> str:
        max_preview = 5
        data_len = len(self.data)
        dtype_str = type(self.data[0]).__name__ if self.data and self.data[0] is not None else 'Unknown'
        if data_len <= max_preview * 2 +1 :
            preview_str = str(self.data)
        else:
            head = ", ".join(map(str, self.data[:max_preview]))
            tail = ", ".join(map(str, self.data[-max_preview:]))
            preview_str = f"[{head}, ..., {tail}]"
        return f"Series(name='{self.name}', length={data_len}, dtype={dtype_str})\nData: {preview_str}"
    def __repr__(self) -> str:
        return self.__str__()

# --- Enhanced DataFrame Class ---
class DataFrame:
    def __init__(self, columns: Optional[List[str]] = None, data: Optional[Union[List[List[Any]], List[Dict[str, Any]]]] = None) -> None:
        self._columns: List[str] = []
        self._series_map: Dict[str, Series[Any]] = OrderedDict() # Preserve column order
        self._data_rows: List[Dict[str, Any]] = []

        # Initialize columns first
        if columns is not None:
            self._columns = list(OrderedDict.fromkeys(columns)) # Unique, preserve order
            for col_name in self._columns:
                self._series_map[col_name] = Series(name=col_name)
        
        # Then populate with data
        if data is not None:
            if not data: return

            if isinstance(data, list) and data and isinstance(data[0], dict):
                # Infer columns if not already set, considering all keys from all dicts
                if not self._columns:
                    all_keys = OrderedDict() # Use OrderedDict to try and keep order of first dict
                    for row_dict in data:
                        if isinstance(row_dict, dict):
                            for k in row_dict.keys(): all_keys[k] = None # Add keys while preserving some order
                    self._columns = list(all_keys.keys())
                    for col_name in self._columns:
                        if col_name not in self._series_map:
                            self._series_map[col_name] = Series(name=col_name)
                
                for row_dict in data: # Now append data using the established/inferred columns
                    if isinstance(row_dict, dict): self.append(row_dict)

            elif isinstance(data, list) and data and isinstance(data[0], list):
                if not self._columns:
                    raise ValueError("Columns must be provided if data is a list of lists.")
                if not all(len(row_list) == len(self._columns) for row_list in data if isinstance(row_list, list)):
                    mismatched_rows = [(i, len(rl)) for i, rl in enumerate(data) if isinstance(rl, list) and len(rl) != len(self._columns)]
                    raise ValueError(f"All rows in list of lists data must match the number of columns ({len(self._columns)}). Mismatched at indices: {mismatched_rows}")
                for row_list in data:
                    if isinstance(row_list, list): self.append(dict(zip(self._columns, row_list)))
            elif not isinstance(data, list): # Single dict or list for data
                 raise TypeError("Data must be a list of dictionaries or a list of lists.")
            # else: data is an empty list, which is fine.

    @property
    def columns(self) -> List[str]:
        return list(self._columns)

    @columns.setter
    def columns(self, new_columns: List[str]) -> None:
        unique_new_cols = list(OrderedDict.fromkeys(new_columns))
        new_data_rows = []
        if self._data_rows:
            for old_row_dict in self._data_rows:
                new_row_dict = {col: old_row_dict.get(col) for col in unique_new_cols}
                new_data_rows.append(new_row_dict)
        
        new_series_map = OrderedDict()
        for col_name in unique_new_cols:
            col_data = [row.get(col_name) for row in new_data_rows]
            new_series_map[col_name] = Series(name=col_name, data=col_data)

        self._columns = unique_new_cols
        self._series_map = new_series_map
        self._data_rows = new_data_rows

    def append(self, record: Dict[str, Any], ignore_index: bool = True) -> None: # ignore_index for Pandas-like behavior (though we don't have an index yet)
        if not isinstance(record, dict):
            raise TypeError("Record to append must be a dictionary.")

        if not self._columns and record:
            self._columns = list(OrderedDict.fromkeys(record.keys()))
            for col_name in self._columns:
                self._series_map[col_name] = Series(name=col_name)
        
        new_cols_in_record = [col for col in record if col not in self._columns]
        if new_cols_in_record:
            for new_col in new_cols_in_record:
                self._columns.append(new_col)
                self._series_map[new_col] = Series(name=new_col, data=[None] * len(self._data_rows))
            for i in range(len(self._data_rows)):
                for new_col in new_cols_in_record:
                    self._data_rows[i][new_col] = None
        
        new_row_for_storage = {}
        for col_name in self._columns:
            value = record.get(col_name) # Defaults to None if key not in record
            if col_name not in self._series_map: # Should not happen if columns are managed correctly
                self._series_map[col_name] = Series(name=col_name, data=[None] * len(self._data_rows))
            self._series_map[col_name].append(value)
            new_row_for_storage[col_name] = value
        self._data_rows.append(new_row_for_storage)
        # self._rebuild_series_map_from_rows() # Keep _series_map consistent if append modifies it

    def _rebuild_series_map_from_rows(self):
        """Internal: Rebuilds _series_map based on _data_rows and _columns.
           Useful after operations that might desync them.
        """
        new_series_map = OrderedDict()
        for col_name in self._columns:
            col_data = [row_dict.get(col_name) for row_dict in self._data_rows]
            new_series_map[col_name] = Series(name=col_name, data=col_data)
        self._series_map = new_series_map


    def add_column(self, name: str, data: Union[Series[Any], List[Any], Any], allow_replace:bool = True) -> None:
        """Adds a new column or replaces an existing one if allow_replace is True."""
        num_rows = self.shape[0]

        if name in self._columns and not allow_replace:
            raise ValueError(f"Column '{name}' already exists. Set allow_replace=True to overwrite.")

        new_series_data: List[Any]
        if isinstance(data, Series):
            if num_rows > 0 and len(data) != num_rows:
                raise ValueError(f"Length of Series for column '{name}' ({len(data)}) must match DataFrame's rows ({num_rows}).")
            new_series_data = list(data.data)
        elif isinstance(data, list):
            if num_rows > 0 and len(data) != num_rows:
                raise ValueError(f"Length of list for column '{name}' ({len(data)}) must match DataFrame's rows ({num_rows}).")
            new_series_data = list(data)
        else: # Scalar
            new_series_data = [data] * num_rows if num_rows > 0 else [data] if not self._data_rows else [] # Special case for empty DF


        if name not in self._columns:
            self._columns.append(name)
        
        self._series_map[name] = Series(name=name, data=new_series_data)
        
        if not self._data_rows and new_series_data: # DF was empty, this is the first column
            self._data_rows = [{name: val} for val in new_series_data]
        else: # Update existing rows or add new ones if DF was row-empty but had columns
            for i in range(max(num_rows, len(new_series_data))):
                if i < num_rows: # Update existing row
                    self._data_rows[i][name] = new_series_data[i] if i < len(new_series_data) else None
                else: # Add new row (if new_series_data is longer than current DF)
                    new_row = {col:None for col in self.columns}
                    new_row[name] = new_series_data[i]
                    self._data_rows.append(new_row)
        # If new_series_data was shorter than num_rows, Nones are implicitly handled by .get in other places or should be filled.

    def drop_column(self, column_name_or_names: Union[str, List[str]], inplace: bool = False) -> Optional[DataFrame]:
        """Removes specified column(s). Returns new DataFrame if inplace=False."""
        cols_to_drop = [column_name_or_names] if isinstance(column_name_or_names, str) else column_name_or_names
        
        df_to_modify = self if inplace else self.copy()
        
        cols_actually_dropped = []
        for col_name in cols_to_drop:
            if col_name not in df_to_modify._columns:
                logger.warning(f"Column '{col_name}' not found for dropping. Skipping.")
                continue
            df_to_modify._columns.remove(col_name)
            del df_to_modify._series_map[col_name]
            for row_dict in df_to_modify._data_rows:
                if col_name in row_dict:
                    del row_dict[col_name]
            cols_actually_dropped.append(col_name)
        
        if not cols_actually_dropped and not inplace: # No columns were actually dropped
             return self.copy() # Return a copy of original
        if not cols_actually_dropped and inplace:
            return None


        if not inplace:
            return df_to_modify
        return None


    @property
    def shape(self) -> Tuple[int, int]:
        return len(self._data_rows), len(self._columns)

    def copy(self) -> DataFrame:
        # Creates a moderately deep copy: new lists for columns and rows, new Series objects,
        # but the data within the series/rows is shallow copied if mutable.
        new_df = DataFrame(columns=list(self.columns)) # New list of column names
        # Reconstruct _data_rows and _series_map fully to ensure independence
        for row_dict in self._data_rows:
            new_df.append(dict(row_dict)) # dict() creates a shallow copy of the row
        return new_df

    def __getitem__(self, key: Union[str, int, List[str], Series[bool]]) -> Union[Series[Any], Dict[str, Any], DataFrame]:
        if isinstance(key, str):
            if key not in self._columns:
                raise KeyError(f"Column '{key}' not found. Available: {self._columns}")
            # Ensure Series in map is consistent with _data_rows and _columns
            if key not in self._series_map or len(self._series_map[key]) != self.shape[0]:
                self._series_map[key] = Series(name=key, data=[row.get(key) for row in self._data_rows])
            return self._series_map[key]
        elif isinstance(key, int):
            if not 0 <= key < self.shape[0]:
                raise IndexError(f"DataFrame row index {key} out of range (0 to {self.shape[0]-1}).")
            return self.get_row_dict(key)
        elif isinstance(key, list) and all(isinstance(k_item, str) for k_item in key):
            missing = [k_item for k_item in key if k_item not in self._columns]
            if missing:
                raise KeyError(f"Columns not found: {missing}. Available: {self._columns}")
            
            # Preserve order of columns as requested in `key`
            selected_data_rows = [{col_name: self.get_row_dict(i).get(col_name) for col_name in key} for i in range(self.shape[0])]
            return DataFrame(columns=key, data=selected_data_rows)
        elif isinstance(key, Series) and key.data and isinstance(key.data[0], bool): # Boolean indexing
            if len(key) != self.shape[0]:
                raise ValueError(f"Boolean Series length ({len(key)}) must match DataFrame rows ({self.shape[0]}).")
            return self.filter(key)
        else:
            raise TypeError(f"Unsupported key type for DataFrame indexing: {type(key)}. Use str, int, List[str], or boolean Series.")

    def __setitem__(self, key: str, value: Union[Series[Any], List[Any], Any]) -> None:
        self.add_column(key, value, allow_replace=True)

    def get_row_dict(self, index: int) -> Dict[str, Any]:
        if not 0 <= index < self.shape[0]:
             raise IndexError(f"Row index {index} out of bounds for DataFrame of shape {self.shape}")
        return self._data_rows[index]

    def iterrows(self) -> Iterable[Tuple[int, Dict[str, Any]]]:
        """Iterates over DataFrame rows as (index, row_dict) pairs."""
        for i in range(self.shape[0]):
            yield i, self.get_row_dict(i)
            
    def __iter__(self): # Default iteration iterates over column names, like Pandas
        return iter(self.columns)

    # --- More Advanced Operations ---
    def filter(self, condition: Series[bool]) -> DataFrame:
        if not isinstance(condition, Series) or not (condition.data and isinstance(condition.data[0], bool)):
             # Check if it's an all-empty boolean series, which is valid
            if not (isinstance(condition, Series) and all(isinstance(x,bool) for x in condition.data)):
                raise TypeError("Condition for filter must be a boolean Series.")
        if len(condition) != self.shape[0]:
            raise ValueError(f"Length of condition Series ({len(condition)}) must match DataFrame rows ({self.shape[0]}).")

        filtered_rows = [self._data_rows[i] for i, keep_flag in enumerate(condition.data) if keep_flag]
        return DataFrame(columns=list(self.columns), data=filtered_rows if filtered_rows else [])

    def sort_values(self, by: Union[str, List[str]], ascending: Union[bool, List[bool]] = True) -> DataFrame:
        """Sorts DataFrame by specified column(s). Returns a new DataFrame."""
        by_cols = [by] if isinstance(by, str) else by
        if not all(isinstance(c, str) for c in by_cols):
            raise TypeError("'by' must be a column name string or list of column name strings.")
        
        missing = [c for c in by_cols if c not in self._columns]
        if missing:
            raise KeyError(f"Cannot sort by columns not in DataFrame: {missing}")

        asc_flags = [ascending] * len(by_cols) if isinstance(ascending, bool) else ascending
        if len(asc_flags) != len(by_cols):
            raise ValueError("Length of 'ascending' flags must match length of 'by' columns.")

        # Create a sortable representation of data rows
        indexed_rows = list(enumerate(self._data_rows)) # Keep original index if needed, though we discard for now

        # Custom sort key function
        def get_sort_key(item_tuple: Tuple[int, Dict[str, Any]]):
            original_idx, row_dict = item_tuple
            keys = []
            for i, col_name in enumerate(by_cols):
                val = row_dict.get(col_name)
                # Handle Nones: typically sort to one end. Python's None sorts before anything.
                # If descending, we want Nones last. If ascending, Nones first.
                # A common trick: (value is None, value) tuple for key.
                is_none = (val is None)
                # If ascending and value is None, it should be "smaller".
                # If descending and value is None, it should also be "smaller" effectively (to go to the end when reversed).
                # Let's use a simpler approach: rely on Python's None sorting and reverse flag.
                # For multi-level sort, Nones can be tricky if not handled consistently.
                keys.append(val)
            return tuple(keys) # Sort by this tuple of values

        # Sorting multiple columns: Python's sort is stable. Sort by last key first.
        # This is incorrect. We need to pass all keys to sort simultaneously.
        # sorted_indices

# --- Funções Auxiliares para Leitura de Arquivos ---

def _detect_delimiter(sample: str, candidates: Iterable[str] = (",", ";", "|", "\t")) -> str:
    """Escolhe o delimitador mais frequente numa amostra."""
    counts = {d: sample.count(d) for d in candidates}
    if not any(counts.values()): return next(iter(candidates)) # Default se nenhum for encontrado
    return max(counts, key=counts.get)

def read_csv( # Renomeado de _read_table para ser mais específico e público
    path: Union[str, Path],
    delimiter: Optional[str] = None,
    has_header: bool = True,
    encoding: str = "utf-8",
) -> DataFrame:
    """Lê um arquivo CSV e retorna um DataFrame."""
    filepath = Path(path)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    rows_from_csv: List[List[str]] = []
    try:
        with filepath.open("r", encoding=encoding, newline="") as fh:
            sample = fh.read(4096)
            fh.seek(0)
            actual_delimiter = delimiter or _detect_delimiter(sample)
            if not actual_delimiter: # Se _detect_delimiter retornar None ou vazio
                logger.warning(f"Could not detect delimiter for {filepath}, using default ','.")
                actual_delimiter = ','
            reader = csv.reader(fh, delimiter=actual_delimiter)
            rows_from_csv = list(reader)
    except Exception as e:
        logger.error(f"Error reading CSV file {filepath}: {e}")
        return DataFrame() # Retorna DataFrame vazio em caso de erro de leitura

    if not rows_from_csv:
        return DataFrame()

    header_row = rows_from_csv[0]
    actual_columns: List[str]
    data_start_index: int

    if has_header:
        actual_columns = [c.strip() for c in header_row]
        data_start_index = 1
    else:
        # Tenta gerar nomes de coluna se não houver cabeçalho
        if header_row:
             actual_columns = [f"col{i}" for i in range(len(header_row))]
        else: # Arquivo CSV completamente vazio ou com linha inicial vazia sem header
            return DataFrame()
        data_start_index = 0
    
    df = DataFrame(columns=actual_columns)

    for i, row_values in enumerate(rows_from_csv[data_start_index:]):
        record = {}
        for j, col_name in enumerate(actual_columns):
            record[col_name] = row_values[j].strip() if j < len(row_values) else None
        df.append(record)
    return df

def read_json( # Novo extrator JSON básico para dataframe.py
    path: Union[str, Path],
    records_path: Optional[Union[str, List[str]]] = None, # e.g., "pedidos" or ["data", "items"]
    encoding: str = "utf-8"
) -> DataFrame:
    """Lê um arquivo JSON e retorna um DataFrame."""
    filepath = Path(path)
    logger.info("Lendo JSON %s", filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    try:
        with filepath.open("r", encoding=encoding) as f:
            raw_data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from {filepath}: {e}")
        return DataFrame()
    except Exception as e:
        logger.error(f"Error reading JSON file {filepath}: {e}")
        return DataFrame()


    records_list: List[Dict[str, Any]] = []
    current_data = raw_data
    if records_path:
        if isinstance(records_path, str):
            records_list = current_data.get(records_path, [])
        else: # list of keys for nested path
            try:
                for key_part in records_path:
                    current_data = current_data[key_part]
                if isinstance(current_data, list):
                    records_list = current_data
                else: # O caminho existe mas não leva a uma lista
                    logger.warning(f"Path {records_path} in JSON {filepath} does not lead to a list.")
                    records_list = []
            except (KeyError, TypeError, IndexError):
                logger.warning(f"Path {records_path} not found in JSON {filepath}.")
                records_list = [] # Mantém records_list como lista vazia
    elif isinstance(current_data, list):
        records_list = current_data
    else:
        logger.error(f"JSON root is not a list and records_path not specified for {filepath}.")
        return DataFrame()

    if not records_list or not isinstance(records_list, list):
        logger.warning(f"No records found or records are not a list in JSON {filepath} at specified path.")
        return DataFrame()
    
    # Checa se a lista não está vazia antes de acessar records_list[0]
    if not records_list:
        return DataFrame() # Retorna DF vazio se a lista de registros estiver vazia

    # Garante que os itens da lista são dicionários
    if not all(isinstance(item, dict) for item in records_list):
        logger.warning(f"Not all items in the records list from JSON {filepath} are dictionaries.")
        # Opcional: filtrar ou retornar DF vazio. Vamos filtrar por enquanto.
        records_list = [item for item in records_list if isinstance(item, dict)]
        if not records_list: return DataFrame()


    # Inferir colunas do primeiro registro válido
    df_columns = list(records_list[0].keys()) if records_list else []
    df = DataFrame(columns=df_columns) # Cria com colunas inferidas ou vazias
    
    for record_dict in records_list:
        if isinstance(record_dict, dict):
             df.append(record_dict) # append irá adicionar novas colunas se necessário
        # else: # Já filtrado acima
        #     logger.warning(f"Skipping non-dictionary record in {filepath}: {record_dict}")
    return df