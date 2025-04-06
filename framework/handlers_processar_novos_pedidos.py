import dataframe as df
import handlers_worker_pools_buffer as wpb

import json

def json_to_list(json_file:str):
    """
    Lê um arquivo JSON e converte os valores de um dicionário interno em uma lista de listas
    O Arquivo JSON deve conter somente um dicionário no topo, cuja chave será uma lista de
    dicionários. A função extrairá os valores de cada dicionário e construirá uma lista de listas.

    Args:
        json_file (str): Caminho para o arquivo JSON a ser lido.

    Returns:
        list: Lista de listas contendo os valores de cada dicionário do JSON.

    Exemplo:
        Se o JSON for:
        {
            "dados":[
            {"a": 1, "b": 2}
            {"a": 3, "b": 4}
            ]
        }

        A função retornará: [[1, 2], [3, 4]]
    """
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    treatment = data[list(data.keys())[0]]
    result = []
    for i in treatment:
        result.append(list(i.values()))
    return result

def get_csv_data(csv_file:str):
    """
    Obtém dados de um arquivo CSV e os converte para a estrutura de Dataframe

    Args:
        csv_file (str): Caminho para um arquivo CSV
    
    Returns:
        df.Dataframe: Dataframe contendo os dados correspondentes ao CSV
    """
    with open(csv_file, "r", encoding="utf-8") as f:
        data = f.readlines()

        data_columns = data[0].rstrip("\n").split(",")
        data_rows = [row.rstrip("\n").split(",") for row in data[1:]]

        df_historico_pedidos = df.DataFrame(data_columns, data_rows)

    return df_historico_pedidos


def update_data(orders_history:df.DataFrame, new_orders:list):
    """
    Atualiza o histórico de dados para que contenha os novos pedidos.
    A função realiza a atualização diretamente ao histórico de pedidos, não tendo, portanto, um retorno.

    Args:
        orders_history (df.Dataframe): Dataframe que contém o histórico de pedidos.
        new_orders (list): Lista de listas contendo as informações dos novos pedidos a serem incluidas ao histórico.

    Returns:
        None
    """
    for i in new_orders:
        orders_history.add_row(i)
    return
