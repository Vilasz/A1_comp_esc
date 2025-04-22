# populate_ecommerce_db.py
"""Popula um banco SQLite *ecommerce.db* com dados realistas de clientes,
produtos, pedidos etc.  O script se auto‑contém: cria as tabelas se ainda não
existirem e utiliza *Faker* para dados plausíveis em pt‑BR.

```
pip install faker
python populate_ecommerce_db.py --clientes 3000 --pedidos 9000 --db ./ecommerce.db
```
"""
from __future__ import annotations

import argparse
import json
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from faker import Faker

fk = Faker("pt_BR")


# Configuração

PRODUCTS: List[str] = [
    "Notebook", "Mouse", "Teclado", "Smartphone", "Fone de Ouvido",
    "Monitor", "Cadeira Gamer", "Mesa para Computador", "Impressora",
    "Webcam", "HD Externo", "SSD", "Placa de Vídeo", "Memória RAM",
    "Fonte ATX", "Placa‑mãe", "Roteador Wi‑Fi", "Leitor de Cartão SD",
    "Grampeador", "Luminária de Mesa", "Estabilizador", "Suporte para Notebook",
    "Mousepad Gamer", "Caixa de Som Bluetooth", "Power Bank", "Scanner",
    "Projetor", "Filtro de Linha", "Cabo USB‑C",
]

PRICES: Dict[str, float] = {
    "Notebook": 3499.90, "Mouse": 79.90, "Teclado": 129.90, "Smartphone": 2499.00,
    "Fone de Ouvido": 199.90, "Monitor": 899.00, "Cadeira Gamer": 1399.00,
    "Mesa para Computador": 649.90, "Impressora": 599.90, "Webcam": 149.90,
    "HD Externo": 399.90, "SSD": 299.90, "Placa de Vídeo": 2599.00,
    "Memória RAM": 229.90, "Fonte ATX": 349.90, "Placa‑mãe": 699.90,
    "Roteador Wi‑Fi": 249.90, "Leitor de Cartão SD": 59.90, "Grampeador": 24.90,
    "Luminária de Mesa": 89.90, "Estabilizador": 189.90, "Suporte para Notebook": 119.90,
    "Mousepad Gamer": 49.90, "Caixa de Som Bluetooth": 159.90, "Power Bank": 139.90,
    "Scanner": 549.90, "Projetor": 1849.00, "Filtro de Linha": 49.90, "Cabo USB‑C": 29.90,
}

CENTERS: List[str] = [
    "São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba",
    "Porto Alegre", "Salvador", "Manaus", "Brasília", "Fortaleza", "Cuiabá",
]


# DDL – cria tabelas se não existirem

DDL = [
    """CREATE TABLE IF NOT EXISTS clientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT, email TEXT, cpf TEXT,
            telefone TEXT, endereco_json TEXT
        );""",
    """CREATE TABLE IF NOT EXISTS categorias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT, descricao TEXT
        );""",
    """CREATE TABLE IF NOT EXISTS produtos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT, descricao TEXT, preco REAL,
            estoque INTEGER, categoria_id INTEGER REFERENCES categorias(id)
        );""",
    """CREATE TABLE IF NOT EXISTS centros_logisticos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT, endereco_json TEXT,
            capacidade INTEGER, capacidade_utilizada INTEGER, ativo BOOLEAN
        );""",
    """CREATE TABLE IF NOT EXISTS pedidos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id INTEGER REFERENCES clientes(id),
            status TEXT, centro_logistico_id INTEGER REFERENCES centros_logisticos(id),
            valor_total REAL, endereco_entrega_json TEXT
        );""",
    """CREATE TABLE IF NOT EXISTS itens_pedido (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pedido_id INTEGER REFERENCES pedidos(id),
            produto_id INTEGER REFERENCES produtos(id),
            quantidade INTEGER, preco_unitario REAL
        );""",
    """CREATE TABLE IF NOT EXISTS entregas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pedido_id INTEGER REFERENCES pedidos(id),
            data_envio TEXT, data_prevista TEXT,
            data_entrega TEXT, status TEXT,
            transportadora TEXT, codigo_rastreio TEXT
        );""",
]


# Funções helpers


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)



# Populadores individuais


def populate_clientes(cur: sqlite3.Cursor, n: int) -> None:
    for i in range(1, n + 1):
        nome = f"cliente{i}"
        email = f"{nome}@exemplo.com"
        cpf = f"{random.randint(10**10, 10**11-1)}"
        telefone = fk.phone_number()
        endereco = {
            "rua": fk.street_name(),
            "numero": fk.building_number(),
            "bairro": fk.bairro(),
            "cidade": fk.city(),
            "estado": fk.estado_sigla(),
            "cep": fk.postcode(),
        }
        cur.execute(
            """INSERT INTO clientes (nome,email,cpf,telefone,endereco_json)
               VALUES (?,?,?,?,?)""",
            (nome, email, cpf, telefone, _json(endereco)),
        )


def populate_categorias(cur: sqlite3.Cursor, n: int) -> None:
    for i in range(1, n + 1):
        cur.execute(
            "INSERT INTO categorias (nome, descricao) VALUES (?, ?)",
            (f"Categoria {i}", f"Descrição {i}"),
        )


def populate_produtos(cur: sqlite3.Cursor) -> None:
    for nome in PRODUCTS:
        cur.execute(
            """INSERT INTO produtos
                (nome, descricao, preco, estoque, categoria_id)
               VALUES (?,?,?,?,?)""",
            (
                nome,
                nome,
                PRICES[nome],
                random.randint(5, 100),
                random.randint(1, 5),
            ),
        )


def populate_centros(cur: sqlite3.Cursor) -> None:
    for nome in CENTERS:
        endereco = {
            "rua": fk.street_name(),
            "numero": fk.building_number(),
            "bairro": fk.bairro(),
            "cidade": nome,
        }
        capacidade = 20000
        cur.execute(
            """INSERT INTO centros_logisticos
                (nome,endereco_json,capacidade,capacidade_utilizada,ativo)
               VALUES (?,?,?,?,?)""",
            (
                nome,
                _json(endereco),
                capacidade,
                random.randint(1, capacidade),
                True,
            ),
        )


def populate_pedidos(cur: sqlite3.Cursor, n: int) -> None:
    cur.execute("SELECT id,endereco_json FROM clientes")
    clientes = cur.fetchall()
    for _ in range(n):
        cliente_id, end_json = random.choice(clientes)
        cur.execute(
            """INSERT INTO pedidos
                (cliente_id,status,centro_logistico_id,valor_total,endereco_entrega_json)
               VALUES (?,?,?,?,?)""",
            (
                cliente_id,
                random.choice(["novo", "processando", "enviado", "entregue", "cancelado"]),
                random.randint(1, len(CENTERS)),
                round(random.uniform(50, 5000), 2),
                end_json,
            ),
        )


def populate_itens(cur: sqlite3.Cursor) -> None:
    cur.execute("SELECT id FROM pedidos")
    pedidos = [row[0] for row in cur.fetchall()]
    cur.execute("SELECT id, preco FROM produtos")
    produtos = cur.fetchall()
    for pedido_id in pedidos:
        for _ in range(random.randint(1, 5)):
            produto_id, preco = random.choice(produtos)
            quantidade = random.randint(1, 5)
            cur.execute(
                """INSERT INTO itens_pedido (pedido_id,produto_id,quantidade,preco_unitario)
                   VALUES (?,?,?,?)""",
                (pedido_id, produto_id, quantidade, preco),
            )


def populate_entregas(cur: sqlite3.Cursor) -> None:
    cur.execute("SELECT id FROM pedidos")
    for (pedido_id,) in cur.fetchall():
        data_envio = fk.date_time_between(start_date="-365d", end_date="now")
        data_prevista = data_envio + timedelta(days=random.randint(3, 7))
        if random.random() < 0.8:
            data_entrega = data_envio + timedelta(days=random.randint(2, 8))
            status = "entregue"
        else:
            data_entrega = None
            status = "pendente"
        cur.execute(
            """INSERT INTO entregas (pedido_id,data_envio,data_prevista,data_entrega,status,transportadora,codigo_rastreio)
               VALUES (?,?,?,?,?,?,?)""",
            (
                pedido_id,
                data_envio.isoformat(sep=" ", timespec="seconds"),
                data_prevista.isoformat(sep=" ", timespec="seconds"),
                data_entrega.isoformat(sep=" ", timespec="seconds") if data_entrega else None,
                status,
                random.choice(["Correios", "Jadlog", "FedEx", "Total Express", "Loggi"]),
                fk.bothify(text="??########BR").upper(),
            ),
        )


# Função principal


def populate_database(db_path: Path, n_clientes: int, n_categorias: int, n_pedidos: int) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Cria tabelas
    for ddl in DDL:
        cur.execute(ddl)
    conn.commit()

    # Popula
    populate_clientes(cur, n_clientes)
    populate_categorias(cur, n_categorias)
    populate_produtos(cur)
    populate_centros(cur)
    populate_pedidos(cur, n_pedidos)
    populate_itens(cur)
    populate_entregas(cur)

    conn.commit()
    conn.close()
    print("Base populada com sucesso →", db_path.resolve())


# CLI


def _parse() -> argparse.Namespace:  # pragma: no cover
    p = argparse.ArgumentParser(description="Popula banco ecommerce.db com dados fictícios")
    p.add_argument("--db", default="ecommerce.db", help="Caminho para o SQLite")
    p.add_argument("--clientes", type=int, default=3000)
    p.add_argument("--categorias", type=int, default=5)
    p.add_argument("--pedidos", type=int, default=9000)
    return p.parse_args()


def main() -> None:  # pragma: no cover
    args = _parse()
    populate_database(Path(args.db), args.clientes, args.categorias, args.pedidos)


if __name__ == "__main__":  # pragma: no cover
    main()
