# data_generators.py (v3.1 – com parâmetros adicionais para flexibilidade)
"""
Gerador *mock* de pedidos (CSV / JSON).
... (o resto da sua docstring original pode permanecer) ...
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Sequence
import logging # Adicionado para consistência e boas práticas

from faker import Faker

logger = logging.getLogger(__name__) # Adicionado
fk = Faker("pt_BR")

# Constantes (podem ser ajustadas para cenários >12 CPUs sem impacto)
PRODUCTS: List[str] = [
    "Notebook", "Mouse", "Teclado", "Smartphone", "Fone de Ouvido",
    "Monitor", "Cadeira Gamer", "Mesa para Computador", "Impressora",
    "Webcam", "HD Externo", "SSD", "Placa de Vídeo", "Memória RAM",
    "Fonte ATX", "Placa‐mãe", "Roteador Wi‑Fi", "Leitor de Cartão SD",
    "Grampeador", "Luminária de Mesa", "Estabilizador", "Suporte p/ Notebook",
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

def _rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    sec = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=sec)

# Funções públicas
def generate_csv(
    n_rows: int,
    out_path: str | Path = "mock_data_db.csv",
    *,
    start: str = "2023-01-01",
    end: str = "2024-01-01",
    seed: int | None = None,
    qty_weights: Sequence[int] = CSV_WEIGHTS,
    # <<< ADICIONADO: Novos parâmetros com valores padrão >>>
    delimiter: str = ',',
    include_header: bool = True
) -> Path:
    """
    Gera CSV com todas as colunas descritas em topo de arquivo.
    """
    logger.info(f"Gerando CSV: {Path(out_path).name} ({n_rows:,} linhas, delim='{delimiter}', header={include_header})")
    if seed is not None:
        random.seed(seed)
    start_dt, end_dt = datetime.fromisoformat(start), datetime.fromisoformat(end)

    out_path_obj = Path(out_path)
    out_path_obj.parent.mkdir(parents=True, exist_ok=True) # Garante que o diretório pai exista

    with out_path_obj.open("w", newline="", encoding="utf-8") as f:
        # <<< MODIFICADO: Usa o delimiter fornecido >>>
        w = csv.writer(f, delimiter=delimiter)
        
        headers = [
            "cliente_id", "produto_id", "categoria_id", "produto",
            "quantidade", "preco_unitario", "valor_total", "data_pedido",
            "hora_pedido", "mes", "ano", "canal_venda",
            "centro_logistico_mais_proximo", "cidade_cliente",
            "estado_cliente", "dias_para_entrega",
        ]
        
        # <<< MODIFICADO: Escreve o cabeçalho condicionalmente >>>
        if include_header:
            w.writerow(headers)
            
        for _ in range(n_rows):
            prod_idx = random.randrange(len(PRODUCTS))
            produto = PRODUCTS[prod_idx]
            quantidade = random.choices(range(1, 11), weights=qty_weights, k=1)[0]
            preco = PRICES[produto]
            total = round(preco * quantidade, 2)
            dt = _rand_date(start_dt, end_dt)
            w.writerow([
                random.randint(1, 500_000),
                prod_idx + 1,
                random.randint(1, 20),
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
                random.randint(1, 10),
            ])
    logger.info(f"CSV gerado: {out_path_obj.resolve()} ({n_rows:,} linhas)")
    return out_path_obj


def generate_json(
    n_rows: int,
    out_path: str | Path = "mock_data_pedidos_novos.json",
    *,
    start: str = "2023-01-01",
    end: str = "2024-01-01",
    seed: int | None = None,
    qty_weights: Sequence[int] = JSON_WEIGHTS,
    # <<< ADICIONADO: Novo parâmetro com valor padrão >>>
    records_key_name: str = "pedidos" 
) -> Path:
    """
    Gera JSON com a mesma estrutura (lista em `pedidos` ou `records_key_name`).
    """
    logger.info(f"Gerando JSON: {Path(out_path).name} ({n_rows:,} pedidos, root_key='{records_key_name}')")
    if seed is not None:
        random.seed(seed)
    start_dt, end_dt = datetime.fromisoformat(start), datetime.fromisoformat(end)

    pedidos_data = [] # Renomeado para evitar conflito com o nome da chave
    for _ in range(n_rows):
        prod_idx = random.randrange(len(PRODUCTS))
        produto = PRODUCTS[prod_idx]
        quantidade = random.choices(range(1, 11), weights=qty_weights, k=1)[0]
        preco = PRICES[produto]
        dt = _rand_date(start_dt, end_dt)
        pedidos_data.append({
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
        })

    out_path_obj = Path(out_path)
    out_path_obj.parent.mkdir(parents=True, exist_ok=True) # Garante que o diretório pai exista
    
    # <<< MODIFICADO: Usa o records_key_name fornecido >>>
    output_dict = {records_key_name: pedidos_data}
    
    with out_path_obj.open("w", encoding="utf-8") as f:
        json.dump(output_dict, f, ensure_ascii=False, indent=2)
    logger.info(f"JSON gerado: {out_path_obj.resolve()} ({n_rows:,} registros em '{records_key_name}')")
    return out_path_obj

# CLI simples
def _parse_args() -> argparse.Namespace:  # pragma: no cover
    p = argparse.ArgumentParser(description="Gerador de pedidos mock avançado")
    p.add_argument("format", choices=["csv", "json"], help="Formato de saída")
    p.add_argument("n", type=int, help="Quantidade de linhas/pedidos a gerar")
    p.add_argument("path", help="Arquivo de saída")
    p.add_argument("--start", default="2023-01-01")
    p.add_argument("--end", default="2024-01-01")
    p.add_argument("--seed", type=int)
    # <<< NOTA: A CLI não foi atualizada para usar os novos parâmetros,
    # mas isso não afeta o script de benchmark, que importa as funções diretamente. >>>
    return p.parse_args()

def main() -> None:  # pragma: no cover
    args = _parse_args()
    if args.format == "csv":
        # Se você quisesse usar os novos params via CLI, teria que adicioná-los a _parse_args
        # e passá-los aqui, e.g., delimiter=args.delimiter
        generate_csv(args.n, args.path, start=args.start, end=args.end, seed=args.seed)
    else:
        generate_json(args.n, args.path, start=args.start, end=args.end, seed=args.seed)

if __name__ == "__main__":  # pragma: no cover
    main()