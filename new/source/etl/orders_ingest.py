# orders_ingest.py  –  compatível com pedidos_handler v2.0
from __future__ import annotations

import datetime as dt
import random
from pathlib import Path
from typing import Dict, List, Tuple

# NOVA API ---------------------------------------------------------------
from new.source.etl.pedidos_handler import JSONExtractor, PedidoService

DB_FILE = Path("ecommerce.db")
MOCK_JSON = Path("../mock/mock_data_pedidos_novos.json")


# 1. INGESTÃO de pedidos + itens_pedido + atualização de estoque

def process_new_orders() -> Tuple[PedidoService.Table, PedidoService.Table]:
    """
    • Lê mock_data_pedidos_novos.json
    • Gera linhas em 'pedidos'   e  'itens_pedido'
    • Atualiza estoque em 'produtos'
    • devolve DataFrames‑like (PedidoService.Table) já atualizados
    """
    # extrai JSON -----------------------------------------------------------------
    orders_raw = JSONExtractor().extract(MOCK_JSON)

    # abre conexão SQLite via PedidoService ---------------------------------------
    svc = PedidoService(DB_FILE)

    pedidos_tbl      = svc.table("pedidos")
    itens_pedido_tbl = svc.table("itens_pedido")
    clientes_tbl     = svc.table("clientes")
    produtos_tbl     = svc.table("produtos")

    # gera chaves únicas uma única vez --------------------------------------------
    next_pedido_id  = pedidos_tbl.last_pk(default=0) + 1
    next_item_id    = itens_pedido_tbl.last_pk(default=0) + 1

    # insere pedidos / itens e debita estoque -------------------------------------
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for cli_id, prod_id, qtd, centro_id in orders_raw:
        # pedido
        valor_total = round(random.uniform(50, 5_000), 2)
        endereco    = clientes_tbl.get(cli_id)["endereco_json"]

        pedidos_tbl.insert(
            id                   = next_pedido_id,
            cliente_id           = cli_id,
            data_pedido          = now,
            status               = "novo",
            centro_logistico_id  = centro_id,
            valor_total          = valor_total,
            endereco_entrega_json= endereco,
        )

        # item
        preco_unitario = produtos_tbl.get(prod_id)["preco"]
        itens_pedido_tbl.insert(
            id             = next_item_id,
            pedido_id      = next_pedido_id,
            produto_id     = prod_id,
            quantidade     = qtd,
            preco_unitario = preco_unitario,
        )

        # estoque
        produtos_tbl.update_pk(prod_id, estoque = "estoque - ?", params=(qtd,))

        next_pedido_id += 1
        next_item_id   += 1

    svc.commit()                # grava tudo de uma vez
    return pedidos_tbl, itens_pedido_tbl



# 2. CÁLCULOS auxiliares (inalterados, mas sobre novas tabelas)

def top5_mais_vendidos(itens_pedido: PedidoService.Table) -> Dict[int, int]:
    vendas: Dict[int, int] = {}
    for row in itens_pedido.rows:
        vendas[row["produto_id"]] = vendas.get(row["produto_id"], 0) + row["quantidade"]
    return dict(sorted(vendas.items(), key=lambda kv: kv[1])[-5:])

def valor_total_vendas(pedidos: PedidoService.Table) -> float:
    return sum(r["valor_total"] for r in pedidos.rows)

def vendas_por_cat_produto(produtos: PedidoService.Table,
                           itens_pedido: PedidoService.Table) -> Dict[int, int]:
    result: Dict[int, int] = {}
    for row in itens_pedido.rows:
        cat = produtos.get(row["produto_id"])["categoria_id"]
        result[cat] = result.get(cat, 0) + row["quantidade"]
    return result

def centros_mais_requisitados(pedidos: PedidoService.Table) -> Dict[int, int]:
    freq: Dict[int, int] = {}
    for r in pedidos.rows:
        freq[r["centro_logistico_id"]] = freq.get(r["centro_logistico_id"], 0) + 1
    return dict(sorted(freq.items(), key=lambda kv: kv[1]))

def alerta_reposicao(produtos: PedidoService.Table) -> List[str]:
    return [r["nome"] for r in produtos.rows if r["estoque"] <= 10]




if __name__ == "__main__":
    pedidos, itens = process_new_orders()

    print("Top‑5 produtos:",           top5_mais_vendidos(itens))
    print("Valor total vendas:",       f"R$ {valor_total_vendas(pedidos):,.2f}")
    print("Centros mais usados:",      centros_mais_requisitados(pedidos))
    print("Alertas de estoque:",       alerta_reposicao(PedidoService(DB_FILE).table('produtos')))
