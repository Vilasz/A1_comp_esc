from dataframe import DataFrame
from pedidos_handler import CSVHandler, JSONHandler

def agrupa_por_produto(df: DataFrame, coluna_produto: str = "produto", coluna_quantidade: str = "quantidade") -> DataFrame:
    """
    Retorna um novo DataFrame com a soma das quantidades de cada produto.
    """
    df_agrupado = df.group_by(coluna_produto, sum, [coluna_quantidade])

    return df_agrupado

def pega_top_5(df: DataFrame, coluna_produto: str = "produto", coluna_quantidade: str = "quantidade") -> DataFrame:
    """
    Retorna um novo DataFrame com os 5 produtos mais vendidos,
    baseado na soma da coluna de quantidade.
    """
    df_ordenado = df.sort_by(f"sum({coluna_quantidade})", ascending=False)
    top_5 = df_ordenado.rows[:5]
    
    return DataFrame(df_ordenado.columns, top_5)

def print_top_5(df: DataFrame, coluna_produto: str = "produto", coluna_quantidade: str = "sum(quantidade)"):
    """
    Imprime no terminal um ranking dos 5 produtos mais vendidos.

    Entradas:
        df (DataFrame): DataFrame com os top 5 produtos e suas quantidades vendidas.
        coluna_produto (str): Nome da coluna dos produtos.
        coluna_quantidade (str): Nome da coluna com os totais vendidos.
    """
    dados = df.to_dicts()

    print("\n📊 TOP 5 PRODUTOS MAIS VENDIDOS\n")
    print(f"{'RANK':<5} | {'PRODUTO':<15} | {'QUANTIDADE':>10}")
    print("-" * 40)

    for i, item in enumerate(dados, start=1):
        produto = item[coluna_produto]
        quantidade = item[coluna_quantidade]
        print(f"{i:<5} | {produto:<15} | {quantidade:>10}")
    print("-" * 40)
    
def agrega_json_com_csv(orders_history:str, new_orders:str):
    """
    Atualiza o histórico de dados para que contenha os novos pedidos.
    A função realiza a atualização diretamente ao dataframe de histórico de pedidos.

    Args:
        orders_history (str): Caminho para o CSV que contém o histórico de pedidos
        new_orders (str): Caminho para o JSON que contém os novos pedidos.

    Returns:
        df.Dataframe: Dataframe referente ao histórico de pedidos atualizado.
    """
    csv_handler = CSVHandler()
    df_orders_history = csv_handler.extract_data(orders_history)

    json_handler = JSONHandler()
    df_new_orders = json_handler.extract_data(new_orders)

    df_orders_history.vstack(df_new_orders)
    return df_orders_history
# if __name__ == "__main__":
#     csv_handler = CSVHandler()
#     df_orders_history = csv_handler.extract_data("mock_data_db.csv")
#     print("Size of orders history before update: ", df_orders_history.shape())

#     top_vendidos = top_5_mais_vendidos(df_orders_history)
#     print("Top 5 produtos mais vendidos:")
#     print(top_vendidos)