import csv
import json
import math
from multiprocessing import Pool, cpu_count
from dataframe import DataFrame


def parse_csv_chunk(args):
    """
    Função auxiliar que recebe um chunk de linhas CSV e retorna
    uma lista de 'rows' para o DataFrame.
    """
    lines_chunk, columns = args
    rows = []
    for line in lines_chunk:
        produto = line[0]
        quantidade = int(line[1])  # converte para int
        centro = line[2]
        rows.append([produto, quantidade, centro])
    return rows


def parse_json_chunk(args):
    """
    Função auxiliar que recebe um chunk de pedidos JSON e retorna
    uma lista de 'rows' para o DataFrame.
    """
    pedidos_chunk, columns = args
    rows = []
    for pedido in pedidos_chunk:
        produto = pedido["produto"]
        quantidade = int(pedido["quantidade"])
        centro = pedido["centro_logistico_mais_proximo"]
        rows.append([produto, quantidade, centro])
    return rows


def merge_dataframes(list_of_rows, columns):
    """
    Recebe várias listas de rows (cada uma retornada por um 'chunk')
    e as mescla em um único DataFrame.
    """
    all_rows = []
    for chunk_rows in list_of_rows:
        all_rows.extend(chunk_rows)  # concatena todas as rows
    return DataFrame(columns, all_rows)


class CSVHandler:
    """
    Handler robusto para extrair dados de um CSV em paralelo,
    aproveitando vários processos (multiprocessing).
    """
    def __init__(self, num_processes=None):
        """
        :param num_processes: Número de processos a serem usados.
                              Por padrão (None), usa cpu_count().
        """
        self.num_processes = num_processes or cpu_count()

    def extract_data(self, filename):
        """
        Lê um CSV e retorna um DataFrame final, processando as linhas em paralelo.
        """
        with open(filename, mode="r", encoding="utf-8") as f:
            reader = csv.reader(f)
            all_lines = list(reader)

        # Primeira linha é o header (colunas)
        columns = all_lines[0]
        data_lines = all_lines[1:]

        # Divide as linhas em chunks com base no número de processos
        chunk_size = math.ceil(len(data_lines) / self.num_processes)
        chunks = []
        start = 0
        while start < len(data_lines):
            chunk = data_lines[start:start + chunk_size]
            chunks.append((chunk, columns))
            start += chunk_size

        # Cria pool de processos e processa cada chunk
        with Pool(processes=self.num_processes) as pool:
            list_of_rows = pool.map(parse_csv_chunk, chunks)

        # Junta todos os resultados em um único DataFrame
        return merge_dataframes(list_of_rows, columns)


class JSONHandler:
    """
    Handler robusto para extrair dados de um JSON em paralelo,
    aproveitando vários processos (multiprocessing).
    """
    def __init__(self, num_processes=None):
        """
        :param num_processes: Número de processos a serem usados.
                              Por padrão (None), usa cpu_count().
        """
        self.num_processes = num_processes or cpu_count()

    def extract_data(self, filename):
        """
        Lê um JSON no formato {"pedidos": [...]} e retorna um DataFrame final,
        processando os pedidos em paralelo.
        """
        with open(filename, mode="r", encoding="utf-8") as f:
            data = json.load(f)

        pedidos = data["pedidos"]
        columns = ["produto", "quantidade", "centro_logistico_mais_proximo"]

        # Divide a lista de pedidos em chunks com base no número de processos
        chunk_size = math.ceil(len(pedidos) / self.num_processes)
        chunks = []
        start = 0
        while start < len(pedidos):
            chunk = pedidos[start:start + chunk_size]
            chunks.append((chunk, columns))
            start += chunk_size

        # Cria pool de processos e processa cada chunk
        with Pool(processes=self.num_processes) as pool:
            list_of_rows = pool.map(parse_json_chunk, chunks)

        # Junta todos os resultados em um único DataFrame
        return merge_dataframes(list_of_rows, columns)


if __name__ == "__main__":
    # Exemplo de uso:
    csv_handler = CSVHandler(num_processes=4)
    df_csv = csv_handler.extract_data("mock_data_db.csv")
    print("DataFrame extraído do CSV:")
    print(df_csv)
    print("Shape:", df_csv.shape())

    print("==========")

    json_handler = JSONHandler(num_processes=4)
    df_json = json_handler.extract_data("mock_data_pedidos_novos.json")
    print("DataFrame extraído do JSON:")
    print(df_json)
    print("Shape:", df_json.shape())
