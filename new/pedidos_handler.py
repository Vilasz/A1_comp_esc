# etl_handlers.py
import csv
import json
import sqlite3
from multiprocessing import Pool, cpu_count

from miniframework import DataFrame, Series



# Funções de parsing executadas em subprocessos

def _parse_csv_chunk(args):
    lines_chunk, _columns = args
    rows = []
    for line in lines_chunk:
        produto     = line[0]
        quantidade  = int(line[1])
        centro      = line[2]
        rows.append([produto, quantidade, centro])
    return rows


def _parse_json_chunk(args):
    pedidos_chunk, _columns = args
    rows = []
    for pedido in pedidos_chunk:
        produto     = pedido["produto"]
        quantidade  = int(pedido["quantidade"])
        centro      = pedido["centro_logistico_mais_proximo"]
        rows.append([produto, quantidade, centro])
    return rows


def _parse_sqlite_chunk(args):
    database, table, offset, limit = args
    with sqlite3.connect(database) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table} LIMIT {limit} OFFSET {offset}")
        return cur.fetchall()



# Helper: converte lista de linhas → DataFrame do mini‑framework

def _rows_to_dataframe(list_of_rows, columns):
    """
    `list_of_rows` é uma lista de *sub‑listas* — cada sub‑lista vem de um
    chunk processado em paralelo.  Precisamos juntar tudo, transpor e
    criar Series coluna‑a‑coluna.
    """
    all_rows: list[list] = []
    for chunk in list_of_rows:
        all_rows.extend(chunk)

    if not all_rows:                                 # arquivo vazio?
        series = [Series() for _ in columns]
        return DataFrame(columns, series)

    # transpondo: linhas → colunas
    cols_data = list(zip(*all_rows))
    series    = [Series(list(col)) for col in cols_data]
    return DataFrame(columns, series)



# Handlers

class CSVHandler:
    def __init__(self, num_processes: int | None = None):
        self.num_processes = num_processes or cpu_count()

    def extract_data(self, filename):
        with open(filename, newline="", encoding="utf‑8") as f:
            reader = csv.reader(f)
            lines  = list(reader)

        columns     = lines[0]
        data_lines  = lines[1:]
        chunk_size  = (len(data_lines) + self.num_processes - 1) // self.num_processes
        chunks      = [(data_lines[i:i + chunk_size], columns)
                       for i in range(0, len(data_lines), chunk_size)]

        with Pool(self.num_processes) as pool:
            parsed_chunks = pool.map(_parse_csv_chunk, chunks)

        return _rows_to_dataframe(parsed_chunks, columns)


class JSONHandler:
    def __init__(self, num_processes: int | None = None):
        self.num_processes = num_processes or cpu_count()

    def extract_data(self, filename):
        with open(filename, encoding="utf‑8") as f:
            data = json.load(f)

        pedidos      = data["pedidos"]
        columns      = ["produto", "quantidade", "centro_logistico_mais_proximo"]
        chunk_size   = (len(pedidos) + self.num_processes - 1) // self.num_processes
        chunks       = [(pedidos[i:i + chunk_size], columns)
                        for i in range(0, len(pedidos), chunk_size)]

        with Pool(self.num_processes) as pool:
            parsed_chunks = pool.map(_parse_json_chunk, chunks)

        return _rows_to_dataframe(parsed_chunks, columns)


class SQLiteHandler:
    def __init__(self, num_processes: int | None = None):
        self.num_processes = num_processes or cpu_count()

    def extract_data(self, database, table):
        with sqlite3.connect(database) as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            total_rows = cur.fetchone()[0]

            cur.execute(f"PRAGMA table_info({table})")
            columns = [info[1] for info in cur.fetchall()]

        chunk_size = (total_rows + self.num_processes - 1) // self.num_processes
        chunks     = [(database, table, i * chunk_size,
                       min(chunk_size, total_rows - i * chunk_size))
                      for i in range(self.num_processes)
                      if i * chunk_size < total_rows]

        with Pool(self.num_processes) as pool:
            parsed_chunks = pool.map(_parse_sqlite_chunk, chunks)

        return _rows_to_dataframe(parsed_chunks, columns)



# Execução rápida para teste

if __name__ == "__main__":
    csv_handler = CSVHandler(num_processes=4)
    df_csv = csv_handler.extract_data("../mock_data_db.csv")
    print("DataFrame do CSV:")
    df_csv.print()
    print("Shape:", df_csv.shape)     # shape é atributo, não método

    print("=" * 60)

    json_handler = JSONHandler(num_processes=4)
    df_json = json_handler.extract_data("../mock_data_pedidos_novos.json")
    print("DataFrame do JSON:")
    df_json.print()
    print("Shape:", df_json.shape)

    print("=" * 60)

    sqlite_handler = SQLiteHandler(num_processes=4)
    df_sql = sqlite_handler.extract_data("ecommerce.db", "pedidos")
    print("DataFrame do SQLite:")
    df_sql.print()
    print("Shape:", df_sql.shape)
