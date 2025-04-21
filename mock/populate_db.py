import sqlite3
import random
import json
from faker import Faker
from datetime import timedelta

fk = Faker("pt_BR")
conn = sqlite3.connect("../ecommerce.db")
cursor = conn.cursor()

def populate_clientes(num_clientes):
    for i in range(num_clientes):
        nome = f"indivíduo{i}"
        email = nome + "@gmail.com"
        cpf = random.randint(10000000000, 99999999999)
        telefone = fk.phone_number()
        endereco = {
            "rua": fk.street_name(),
            "numero": fk.building_number(),
            "bairro": fk.bairro(),
            "cidade": fk.city(),
            "estado": fk.estado_sigla(),
            "cep": fk.postcode()
        }
        endereco_json = json.dumps(endereco)

        cursor.execute("""
                       INSERT INTO clientes (nome, email, cpf, telefone, endereco_json)
                       VALUES (?, ?, ?, ?, ?)
                       """, (nome, email, cpf, telefone, endereco_json))

    conn.commit()

def populate_produtos():
    produtos_disponiveis = [
        "Notebook", "Mouse", "Teclado", "Smartphone", "Fone de Ouvido",
        "Monitor", "Cadeira Gamer", "Mesa para Computador", "Impressora",
        "Webcam", "HD Externo", "SSD", "Placa de Vídeo", "Memória RAM",
        "Fonte ATX", "Placa-mãe", "Roteador Wi-Fi", "Leitor de Cartão SD",
        "Grampeador", "Luminária de Mesa", "Estabilizador", "Suporte para Notebook",
        "Mousepad Gamer", "Caixa de Som Bluetooth", "Power Bank", "Scanner",
        "Projetor", "Filtro de Linha", "Cabo USB-C"
    ]

    precos_produtos = {
        "Notebook": 3499.90, "Mouse": 79.90, "Teclado": 129.90, "Smartphone": 2499.00, "Fone de Ouvido": 199.90,
        "Monitor": 899.00, "Cadeira Gamer": 1399.00, "Mesa para Computador": 649.90, "Impressora": 599.90,
        "Webcam": 149.90, "HD Externo": 399.90, "SSD": 299.90, "Placa de Vídeo": 2599.00, "Memória RAM": 229.90,
        "Fonte ATX": 349.90, "Placa-mãe": 699.90, "Roteador Wi-Fi": 249.90, "Leitor de Cartão SD": 59.90,
        "Grampeador": 24.90, "Luminária de Mesa": 89.90, "Estabilizador": 189.90, "Suporte para Notebook": 119.90,
        "Mousepad Gamer": 49.90, "Caixa de Som Bluetooth": 159.90, "Power Bank": 139.90, "Scanner": 549.90,
        "Projetor": 1849.00, "Filtro de Linha": 49.90, "Cabo USB-C": 29.90
    }

    for i in produtos_disponiveis:
        nome = i
        descricao = i
        preco = precos_produtos[i]
        estoque = random.randint(5, 100)
        categoria_id = random.randint(1, 5)

        cursor.execute("""
                       INSERT INTO produtos (nome, descricao, preco, estoque, categoria_id)
                       VALUES (?, ?, ?, ?, ?)
                       """, (nome, descricao, preco, estoque, categoria_id))
    conn.commit()

def populate_categorias(num_categorias):
    for i in range(num_categorias):
        nome = f"Categoia {i}"
        description = f"Descrição {i}"

        cursor.execute("""
                       INSERT INTO categorias (nome, descricao)
                       VALUES (?, ?)
                       """, (nome, description))

    conn.commit()

def populate_centros():
    centros_logisticos = [
        "São Paulo", "Rio de Janeiro", "Belo Horizonte",
        "Curitiba", "Porto Alegre", "Salvador", "Manaus",
        "Brasília", "Fortaleza", "Cuiabá"
    ]
    for i in centros_logisticos:
        nome = i
        endereco = {
            "rua": fk.street_name(),
            "numero": fk.building_number(),
            "bairro": fk.bairro(),
            "cidade": i
        }
        endereco_json = json.dumps(endereco)
        capacidade = 20000
        capacidade_utilizada = random.randint(1, capacidade)
        ativo = True

        cursor.execute("""
                        INSERT INTO centros_logisticos (nome, endereco_json, capacidade, capacidade_utilizada, ativo)
                        VALUES (?, ?, ?, ?, ?)
                        """, (nome, endereco_json, capacidade, capacidade_utilizada, ativo))

    conn.commit()

def populate_pedidos(num_pedidos, num_clientes):
    cursor.execute("SELECT * FROM clientes")
    clientes = cursor.fetchall()

    for i in range(num_pedidos):
        cliente = random.choice(clientes)
        cliente_id = cliente[0]
        status = random.choice(["novo", "processando", "enviado", "entregue", "cancelado"])
        centro_logistico_id = random.randint(1, 10)
        valor_total = round(random.uniform(50, 5000), 2)
        endereco_entrega_json = cliente[6]

        cursor.execute("""
            INSERT INTO pedidos (cliente_id, status, centro_logistico_id, valor_total, endereco_entrega_json)
            VALUES (?, ?, ?, ?, ?)
            """, (cliente_id, status, centro_logistico_id, valor_total, endereco_entrega_json))

    conn.commit()

def populate_itens_pedido(num_items):
    cursor.execute("SELECT * FROM pedidos")
    pedidos = cursor.fetchall()
    cursor.execute("SELECT * FROM produtos")
    produtos = cursor.fetchall()

    for pedido in pedidos:
        produto = random.choice(produtos)
        pedido_id = pedido[0]
        produto_id = produto[0]
        quantidade = random.randint(1, 5)
        preco_unitario = produto[3]

        cursor.execute("""
            INSERT INTO itens_pedido (pedido_id, produto_id, quantidade, preco_unitario)
            VALUES (?, ?, ?, ?)
        """, (pedido_id, produto_id, quantidade, preco_unitario))

def populate_entregas():
    cursor.execute("SELECT id FROM pedidos")
    pedidos = [row[0] for row in cursor.fetchall()]

    for pedido_id in pedidos:
        data_envio = fk.date_time_between(start_date='-365d', end_date='now')
        data_prevista = data_envio + timedelta(days=random.randint(3, 7))

        if random.random() < 0.8:
            data_entrega = data_envio + timedelta(days=random.randint(2, 8))
            status = 'entregue'
        else:
            data_entrega = None
            status = 'pendente'

        transportadora = random.choice(["Correios", "Jadlog", "FedEx", "Total Express", "Loggi"])
        codigo_rastreio = fk.bothify(text='??########BR').upper()

        cursor.execute("""
            INSERT INTO entregas (pedido_id, data_envio, data_prevista, data_entrega, status, transportadora, codigo_rastreio)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (pedido_id, data_envio, data_prevista, data_entrega, status, transportadora, codigo_rastreio
        ))

    conn.commit()

def populate_database(num_clientes, num_categorias, num_pedidos):
    populate_clientes(num_clientes)
    populate_produtos()
    populate_categorias(num_categorias)
    populate_centros()
    populate_pedidos(num_pedidos, num_clientes)
    populate_itens_pedido(num_pedidos*3)
    populate_entregas()

if __name__ == '__main__':
    populate_database(3000, 5, 9000)
    conn.close()
