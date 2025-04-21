# data_generators.py  (v3 – dados enriquecidos para dashboards)
"""
Gerador *mock* de pedidos (CSV / JSON) com colunas ricas.

Colunas geradas
---------------
cliente_id | produto_id | categoria_id | produto | quantidade | preco_unitario
valor_total | data_pedido | hora_pedido | mes | ano | canal_venda
centro_logistico_mais_proximo | cidade_cliente | estado_cliente | dias_para_entrega

Uso rápido (CLI)
----------------
python data_generators.py csv 1_000_000 pedidos.csv
python data_generators.py json 400_000 pedidos.json
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Sequence

from faker import Faker

fk = Faker("pt_BR")

# ---------------------------------------------------------------------------
# Constantes (podem ser ajustadas para cenários >12 CPUs sem impacto)
# ---------------------------------------------------------------------------
PRODUCTS: List[str] = [
    "Notebook", "Mouse", "Teclado", "Smartphone", "Fone de Ouvido",
    "Monitor", "Cadeira Gamer", "Mesa para Computador", "Impressora",
    "Webcam", "HD Externo", "SSD", "Placa de Vídeo", "Memória RAM",
    "Fonte ATX", "Placa‑mãe", "Roteador Wi‑Fi", "Leitor de Cartão SD",
    "Grampeador", "Luminária de Mesa", "Estabilizador", "Suporte p/ Notebook",
    "Mousepad Gamer", "Caixa de Som Bluetooth", "Power Bank", "Scanner",
    "Projetor", "Filtro de Linha", "Cabo USB‑C",
]

PRICES = {p: round(random.uniform(20, 4000), 2) for p in PRODUCTS}

CENTERS: List[str] = [
    "São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba",
    "Porto Alegre", "Salvador", "Manaus", "Brasília", "Fortaleza", "Cuiabá",
]

CHANNELS = ["site", "app", "telefone", "loja"]

CSV_WEIGHTS = [10, 9, 8, 6, 5, 3, 2, 1, 1, 1]
JSON_WEIGHTS = [12, 10, 6, 4, 3, 2, 2, 1, 1, 1]

# ---------------------------------------------------------------------------


def _rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    sec = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=sec)


# ---------------------------------------------------------------------------
# Funções públicas
# ---------------------------------------------------------------------------
def generate_csv(
    n_rows: int,
    out_path: str | Path = "mock_data_db.csv",
    *,
    start: str = "2023-01-01",
    end: str = "2024-01-01",
    seed: int | None = None,
    qty_weights: Sequence[int] = CSV_WEIGHTS,
) -> Path:
    """
    Gera CSV com todas as colunas descritas em topo de arquivo.
    """
    if seed is not None:
        random.seed(seed)
    start_dt, end_dt = datetime.fromisoformat(start), datetime.fromisoformat(end)

    out_path = Path(out_path)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "cliente_id",
                "produto_id",
                "categoria_id",
                "produto",
                "quantidade",
                "preco_unitario",
                "valor_total",
                "data_pedido",
                "hora_pedido",
                "mes",
                "ano",
                "canal_venda",
                "centro_logistico_mais_proximo",
                "cidade_cliente",
                "estado_cliente",
                "dias_para_entrega",
            ]
        )
        for _ in range(n_rows):
            prod_idx = random.randrange(len(PRODUCTS))
            produto = PRODUCTS[prod_idx]
            quantidade = random.choices(range(1, 11), weights=qty_weights, k=1)[0]
            preco = PRICES[produto]
            total = round(preco * quantidade, 2)
            dt = _rand_date(start_dt, end_dt)
            w.writerow(
                [
                    random.randint(1, 500_000),          # cliente_id
                    prod_idx + 1,                       # produto_id
                    random.randint(1, 20),              # categoria_id
                    produto,
                    quantidade,
                    preco,
                    total,
                    dt.date().isoformat(),
                    dt.time().isoformat(timespec="seconds"),
                    dt.month,
                    dt.year,
                    random.choice(CHANNELS),
                    random.choice(CENTERS),
                    fk.city(),
                    fk.estado_sigla(),
                    random.randint(1, 10),              # dias_para_entrega
                ]
            )
    print(f"CSV gerado: {out_path.resolve()}  ({n_rows:,} linhas)")
    return out_path


def generate_json(
    n_rows: int,
    out_path: str | Path = "mock_data_pedidos_novos.json",
    *,
    start: str = "2023-01-01",
    end: str = "2024-01-01",
    seed: int | None = None,
    qty_weights: Sequence[int] = JSON_WEIGHTS,
) -> Path:
    """
    Gera JSON com a mesma estrutura (lista em `pedidos`).
    """
    if seed is not None:
        random.seed(seed)
    start_dt, end_dt = datetime.fromisoformat(start), datetime.fromisoformat(end)

    pedidos = []
    for _ in range(n_rows):
        prod_idx = random.randrange(len(PRODUCTS))
        produto = PRODUCTS[prod_idx]
        quantidade = random.choices(range(1, 11), weights=qty_weights, k=1)[0]
        preco = PRICES[produto]
        dt = _rand_date(start_dt, end_dt)
        pedidos.append(
            {
                "cliente_id": random.randint(1, 500_000),
                "produto_id": prod_idx + 1,
                "categoria_id": random.randint(1, 20),
                "produto": produto,
                "quantidade": quantidade,
                "preco_unitario": preco,
                "valor_total": round(preco * quantidade, 2),
                "data_pedido": dt.isoformat(sep=" ", timespec="seconds"),
                "canal_venda": random.choice(CHANNELS),
                "centro_logistico_mais_proximo": random.choice(CENTERS),
                "cidade_cliente": fk.city(),
                "estado_cliente": fk.estado_sigla(),
                "dias_para_entrega": random.randint(1, 10),
            }
        )

    out_path = Path(out_path)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump({"pedidos": pedidos}, f, ensure_ascii=False, indent=2)
    print(f"JSON gerado: {out_path.resolve()}  ({n_rows:,} pedidos)")
    return out_path


# ---------------------------------------------------------------------------
# CLI simples
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:  # pragma: no cover
    p = argparse.ArgumentParser(description="Gerador de pedidos mock avançado")
    p.add_argument("format", choices=["csv", "json"], help="Formato de saída")
    p.add_argument("n", type=int, help="Quantidade de linhas/pedidos a gerar")
    p.add_argument("path", help="Arquivo de saída")
    p.add_argument("--start", default="2023-01-01")
    p.add_argument("--end", default="2024-01-01")
    p.add_argument("--seed", type=int)
    return p.parse_args()


def main() -> None:  # pragma: no cover
    args = _parse_args()
    if args.format == "csv":
        generate_csv(args.n, args.path, start=args.start, end=args.end, seed=args.seed)
    else:
        generate_json(args.n, args.path, start=args.start, end=args.end, seed=args.seed)


if __name__ == "__main__":  # pragma: no cover
    main()
