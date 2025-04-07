# from concurrent_pipeline import ConcurrentPipeline
from concurrent_pipeline import ConcurrentPipeline
from dataframe import DataFrame
from pedidos_handler import CSVHandler, JSONHandler
from pathlib import Path
import time

ROOT_DIR = Path(__file__).resolve().parent.parent
import sys
sys.path.append(str(ROOT_DIR))
from mock.gera_csv_pedidos import gerar_pedidos_csv
from mock.gera_json_pedidos import gerar_pedidos

# Modified worker functions to match the expected signature (single parameter)
def agrupa_dados(data: tuple[DataFrame, DataFrame]) -> DataFrame:
    df_orders_history, df_new_orders = data
    
    df_orders_history.vstack(df_new_orders)    

    return df_orders_history

def agrupa_por_produto(df: DataFrame) -> DataFrame:
    """
    Retorna um novo DataFrame com a soma das quantidades de cada produto.
    """
    coluna_produto = "produto"
    coluna_quantidade = "quantidade"
    df_agrupado = df.group_by(coluna_produto, sum, [coluna_quantidade])
    return df_agrupado

def pega_top_5(df: DataFrame) -> DataFrame:
    """
    Retorna um novo DataFrame com os 5 produtos mais vendidos,
    baseado na soma da coluna de quantidade.
    """
    coluna_produto = "produto"
    coluna_quantidade = "quantidade"
    df_ordenado = df.sort_by(f"sum({coluna_quantidade})", ascending=False)
    top_5 = df_ordenado.rows[:5]
    return DataFrame(df_ordenado.columns, top_5)

def print_top_5(df: DataFrame):
    """
    Imprime no terminal um ranking dos 5 produtos mais vendidos.
    
    Entradas:
        df (DataFrame): DataFrame com os top 5 produtos e suas quantidades vendidas.
    """
    coluna_produto = "produto"
    coluna_quantidade = "sum(quantidade)"
    dados = df.to_dicts()
    
    print("\n📊 TOP 5 PRODUTOS MAIS VENDIDOS\n")
    print(f"{'RANK':<5} | {'PRODUTO':<15} | {'QUANTIDADE':>10}")
    print("-" * 40)
    
    for i, item in enumerate(dados, start=1):
        produto = item[coluna_produto]
        quantidade = item[coluna_quantidade]
        print(f"{i:<5} | {produto:<15} | {quantidade:>10}")
    print("-" * 40)
    
    # Since this is the last stage, it's okay to return None
    # But for consistency, you could return the data as is
    return df

if __name__ == "__main__":
    num_workers = [2, 4, 8, 16]

    gerar_pedidos_csv(100_000)
    gerar_pedidos(10000)
    
    dict_final = {}

    csv_handler = CSVHandler()
    df_orders_history = csv_handler.extract_data("mock_data_db.csv")
    
    json_handler = JSONHandler()
    df_new_orders = json_handler.extract_data("mock_data_pedidos_novos.json")
    
    data = (df_new_orders, df_orders_history)
    for num in num_workers:
        data_list = []
        print(f"Num worker: {num}")
        pipeline = ConcurrentPipeline(max_buffer_size=100, num_workers=num, verbose=False)
        
        pipeline.add_stage('Handler: agrupa dados', agrupa_dados)
        pipeline.add_stage('Handler: agrupa por produto', agrupa_por_produto)
        pipeline.add_stage('Handler: seleciona top 5', pega_top_5)
        pipeline.add_stage('Handler: exibe top 5', print_top_5)
        
        print(f"Starting pipeline")
        pipeline.start()
        start_time = time.time()
        # print(30 * "=")
        # print("Adicionando dados")
        for i in range(16):
            data_list.append(data)

        pipeline.add_batch(data_list)

        print(f"Shutting down pipeline")
        pipeline.end()
        end_time = time.time()
        elapsed_time = end_time - start_time
        dict_final[num] = elapsed_time

    print(dict_final)