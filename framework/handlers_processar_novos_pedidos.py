from dataframe import DataFrame
from pedidos_handler import CSVHandler, JSONHandler

def update_data(orders_history:str, new_orders:str):
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

if __name__ == "__main__":
    csv_handler = CSVHandler()
    df_orders_history = csv_handler.extract_data("mock_data_db.csv")
    print("Size of orders history before update: ", df_orders_history.shape())
          
    json_handler = JSONHandler()
    df_new_orders = json_handler.extract_data("mock_data_pedidos_novos.json")
    print("Size of orders history after update: ", df_new_orders.shape())

    print("Size of orders history after update: ", update_data("mock_data_db.csv", "mock_data_pedidos_novos.json").shape())
