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
    itens_pedido = sqlite_extractor.extract_data("ecommerce.db", "itens_pedido")
    produtos = sqlite_extractor.extract_data("ecommerce.db", "produtos")


    for order in new_orders.rows:
        id = orders_history.rows[-1][0] + 1
        cliente_id = order[0]
        data_pedido = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = "novo"
        centro_logistico_id = order[3]
        valor_total = round(random.uniform(50, 5000), 2)
        endereco = client_table.rows[cliente_id][6]

        orders_history.add_row([id, cliente_id, data_pedido, status, centro_logistico_id, valor_total, endereco])

        itens_id = itens_pedido.rows[-1][0] + 1
        produto_id = order[1]
        quantidade = order[2]
        preco_unitario = produtos.rows[produto_id-1][3]

        itens_pedido.add_row([itens_id, id, produto_id, quantidade, preco_unitario])

    return orders_history, itens_pedido


def update_stock():
    json_extractor = ph.JSONHandler()
    new_orders = json_extractor.extract_data("../mock/mock_data_pedidos_novos.json")

    sqlite_extractor = ph.SQLiteHandler()
    produtos = sqlite_extractor.extract_data("ecommerce.db", "produtos")

    for order in new_orders.rows:
        produtos.rows[order[1]-1][4] -= order[2]

    return produtos

def top5_mais_vendidos(itens_pedido):
    quant_vendidas = {i: 0 for i in range(1, 30)}
    for i in itens_pedido.rows:
        quant_vendidas[i[2]] += 1

    sorted_quant_vendidas = dict(sorted(quant_vendidas.items(), key=lambda x: x[1]))

    return dict(list(sorted_quant_vendidas.items())[-5:])

def valor_total_vendas(orders_history):
    total = 0
    for i in orders_history.rows:
        total += i[5]
    return total

def vendas_por_cat_produto(produtos, itens_pedido):
    result = {}

    for i in itens_pedido.rows:
        produto = i[2]
        quantidade = i[3]
        category = produtos.rows[produto-1][5]

        if category in result:
            result[category] += quantidade
        else:
            result[category] = quantidade

    return result


def centros_mais_requisitados(orders_history):
    result = {1:0, 2:0, 3:0, 4:0, 5:0, 6:0, 7:0, 8:0, 9:0, 10:0}
    for i in orders_history.rows:
        result[i[4]] += 1

    return dict(sorted(result.items(), key=lambda x: x[1]))

def alerta_reposição(produtos):
    produtos_em_alerta = []
    for i in produtos.rows:
        if i[4] <= 10:
            produtos_em_alerta.append(i[1])
    return produtos_em_alerta

if __name__ == "__main__":
    orders_history, itens_pedidos = process_new_orders()
    print(orders_history)
    print(centros_mais_requisitados(orders_history))
    print(valor_total_vendas(orders_history))
    print(update_stock())
    print(top5_mais_vendidos(itens_pedidos))
    print(vendas_por_cat_produto(update_stock(), itens_pedidos))
