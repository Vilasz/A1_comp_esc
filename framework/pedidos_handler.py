import csv
import json
import sqlite3
from multiprocessing import Pool, cpu_count
from dataframe import DataFrame


def parse_csv_chunk(args):
    lines_chunk, columns = args
    rows = []
    for line in lines_chunk:
        produto = line[0]
        quantidade = int(line[1]) 
        centro = line[2]
        rows.append([produto, quantidade, centro])
    return rows


def parse_json_chunk(args):
    pedidos_chunk, columns = args
    rows = []
    for pedido in pedidos_chunk:
        produto = pedido["produto"]
        quantidade = int(pedido["quantidade"])
        centro = pedido["centro_logistico_mais_proximo"]
        rows.append([produto, quantidade, centro])
    return rows

def parse_sqlite_chunk(args):
    database, table, offset, limit = args
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {table} LIMIT {limit} OFFSET {offset}")
    rows = cursor.fetchall()

    conn.close()
    return rows


def merge_dataframes(list_of_rows, columns):
    all_rows = []
    for chunk_rows in list_of_rows:
        all_rows.extend(chunk_rows) 
    return DataFrame(columns, all_rows)


class CSVHandler:
    def __init__(self, num_processes=None):
        """
        :param num_processes: Número de processos a serem usados.
                              Por padrão (None), usa cpu_count().
        """
        self.num_processes = num_processes or cpu_count()

    def extract_data(self, filename):
        with open(filename, mode="r", encoding="utf-8") as f:
            reader = csv.reader(f)
            all_lines = list(reader)

        columns = all_lines[0]
        data_lines = all_lines[1:]
        chunk_size = (len(data_lines) + self.num_processes - 1) // self.num_processes
        chunks = []
        start = 0
        while start < len(data_lines):
            chunk = data_lines[start:start + chunk_size]
            chunks.append((chunk, columns))
            start += chunk_size

        with Pool(processes=self.num_processes) as pool:
            list_of_rows = pool.map(parse_csv_chunk, chunks)

        return merge_dataframes(list_of_rows, columns)


class JSONHandler:
    def __init__(self, num_processes=None):
        """
        :param num_processes: Número de processos a serem usados.
                              Por padrão (None), usa cpu_count().
        """
        self.num_processes = num_processes or cpu_count()

    def extract_data(self, filename):
        with open(filename, mode="r", encoding="utf-8") as f:
            data = json.load(f)

        pedidos = data["pedidos"]
        columns = ["produto", "quantidade", "centro_logistico_mais_proximo"]

        chunk_size = (len(pedidos) + self.num_processes - 1) // self.num_processes
        chunks = []
        start = 0
        while start < len(pedidos):
            chunk = pedidos[start:start + chunk_size]
            chunks.append((chunk, columns))
            start += chunk_size

        with Pool(processes=self.num_processes) as pool:
            list_of_rows = pool.map(parse_json_chunk, chunks)

        return merge_dataframes(list_of_rows, columns)

class SQLiteHandler:
    def __init__(self, num_processes=None):
        self.num_processes = num_processes or cpu_count()

    def extract_data(self, database, table):
        conn = sqlite3.connect(database)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total = cursor.fetchone()[0]

        cursor.execute(f"PRAGMA table_info({table})")
        columns_info = cursor.fetchall()
        columns = [col[1] for col in columns_info]

        chunk_size = (total + self.num_processes - 1) // self.num_processes
        chunks = []
        for i in range(self.num_processes):
            offset = i * chunk_size
            limit = min(chunk_size, total - offset)
            if limit > 0:
                chunks.append((database, table, offset, limit))

        with Pool(processes=self.num_processes) as pool:
            list_of_rows = pool.map(parse_sqlite_chunk, chunks)

        return merge_dataframes(list_of_rows, columns)



if __name__ == "__main__":
    csv_handler = CSVHandler(num_processes=4)
    df_csv = csv_handler.extract_data("../mock_data_db.csv")
    print("DataFrame extraído do CSV:")
    print(df_csv)
    print("Shape:", df_csv.shape())

    print("==========")

    json_handler = JSONHandler(num_processes=4)
    df_json = json_handler.extract_data("../mock_data_pedidos_novos.json")
    print("DataFrame extraído do JSON:")
    print(df_json)
    print("Shape:", df_json.shape())

    sqlite3_handler = SQLiteHandler(num_processes=4)
    df_sql = sqlite3_handler.extract_data("ecommerce.db", "pedidos")
    print("Dataframe extraído do SQLite:")
    print(df_sql)
    print("Shape:", df_sql.shape())
