import pedidos_handler as ph
import dataframe
import datetime as dt
import random

def process_new_orders():
    json_extractor = ph.JSONHandler()
    new_orders = json_extractor.extract_data("../mock/mock_data_pedidos_novos.json")

    sqlite_extractor = ph.SQLiteHandler()
    orders_history = sqlite_extractor.extract_data("ecommerce.db", "pedidos")
    client_table = sqlite_extractor.extract_data("ecommerce.db", "clientes")

    for order in new_orders.rows:
        id = orders_history.rows[-1][0] + 1
        cliente_id = order[0]
        data_pedido = dt.datetime.now()
        status = "novo"
        centro_logistico_id = order[3]
        valor_total = round(random.uniform(50, 5000), 2)
        endereco = client_table.rows[cliente_id][6]

        orders_history.add_row([id, cliente_id, data_pedido, status, centro_logistico_id, valor_total, endereco])

    return orders_history


def update_stock():
    json_extractor = ph.JSONHandler()
    new_orders = json_extractor.extract_data("../mock/mock_data_pedidos_novos.json")

    sqlite_extractor = ph.SQLiteHandler()
    produtos = sqlite_extractor.extract_data("ecommerce.db", "produtos")

    for order in new_orders.rows:
        produtos.rows[order[1]-1][4] -= order[2]

    return produtos


if __name__ == "__main__":
    orders_history = process_new_orders()
    print(orders_history)
    produtos = update_stock()
    print(produtos)

