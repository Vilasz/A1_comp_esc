# mvp_pipeline.py  (v5 — heavy‑load, métricas ricas e pool híbrido)
"""
ETL paralelizado ultra‑robusto para datasets gigantes gerados por data_generators.py.

Evoluções v5
------------
* **Suporte às 17 colunas novas** (preco_unitario, valor_total, canal_venda,
  estado_cliente, …).
* Calcula **três agregados** simultâneos no worker:
    1. (produto, centro) → (valor_total, quantidade)              ‑ original
    2. canal_venda        → valor_total
    3. estado_cliente     → quantidade
* Usa **HybridPool** do `miniframework` — nunca passa de 12 CPUs.
* Métricas histogram/contador publicadas no relatório.
"""

from __future__ import annotations
from concurrent.futures import as_completed

import argparse
import csv
import hashlib
import json
import os
import random
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

from tqdm import tqdm

from data_generators import CENTERS, CHANNELS, PRODUCTS, generate_csv, generate_json
from miniframework import HybridPool, METRICS, CPU_LIMIT

###############################################################################
# CONSTANTES / CONFIG #########################################################
###############################################################################
DEFAULT_CSV = Path("mock_data_db.csv")
DEFAULT_JSON = Path("mock_data_pedidos_novos.json")
DEFAULT_DB = Path("ecommerce.db")
SUMMARY_TABLE = "orders_summary"

###############################################################################
# EXTRAÇÃO ####################################################################
###############################################################################


def _load_csv(path: Path) -> List[Tuple[str, int, str, float, str, str]]:
    with path.open("r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        return [
            (
                row["produto"],
                int(row["quantidade"]),
                row["centro_logistico_mais_proximo"],
                float(row["preco_unitario"]),
                row["canal_venda"],
                row["estado_cliente"],
            )
            for row in rdr
        ]


def _load_json(path: Path) -> List[Tuple[str, int, str, float, str, str]]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)["pedidos"]
    return [
        (
            p["produto"],
            int(p["quantidade"]),
            p["centro_logistico_mais_proximo"],
            float(p["preco_unitario"]),
            p["canal_venda"],
            p["estado_cliente"],
        )
        for p in data
    ]

###############################################################################
# WORKER ######################################################################
###############################################################################


def _process_chunk(
    chunk: Sequence[Tuple[str, int, str, float, str, str]],
    loops: int,
) -> Tuple[
    Dict[Tuple[str, str], Tuple[float, int]],
    Dict[str, float],
    Dict[str, int],
]:
    rng = random.Random()
    sha = hashlib.sha256

    prod_center: Dict[Tuple[str, str], Tuple[float, int]] = {}
    canal_tot: Dict[str, float] = {}
    estado_qtd: Dict[str, int] = {}

    for prod, qtd, centro, preco, canal, uf in chunk:
        preco = preco * rng.uniform(0.95, 1.05)  # ligeira variação
        # 1) produto+centro
        v, c = prod_center.get((prod, centro), (0.0, 0))
        prod_center[(prod, centro)] = (v + preco * qtd, c + qtd)
        # 2) canal
        canal_tot[canal] = canal_tot.get(canal, 0.0) + preco * qtd
        # 3) estado
        estado_qtd[uf] = estado_qtd.get(uf, 0) + qtd

        # payload CPU‑bound
        payload = f"{prod}{qtd}{centro}{preco}".encode()
        for _ in range(loops):
            payload = sha(payload).digest()

    METRICS.counter("records_processed").inc(len(chunk))
    return prod_center, canal_tot, estado_qtd

###############################################################################
# MERGE TOOLS #################################################################
###############################################################################


def _merge_pair(
    dest: Dict[Tuple[str, str], Tuple[float, int]],
    src: Dict[Tuple[str, str], Tuple[float, int]],
):
    for k, (v, q) in src.items():
        dv, dq = dest.get(k, (0.0, 0))
        dest[k] = (dv + v, dq + q)


def _merge_num(dest: Dict[str, float], src: Dict[str, float]):
    for k, v in src.items():
        dest[k] = dest.get(k, 0.0) + v


def _merge_int(dest: Dict[str, int], src: Dict[str, int]):
    for k, v in src.items():
        dest[k] = dest.get(k, 0) + v

###############################################################################
# SQLITE ######################################################################
###############################################################################


def _ensure_table(conn: sqlite3.Connection):
    conn.executescript(
        f"""
        CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
            produto TEXT,
            centro  TEXT,
            total_valor REAL,
            total_qtd  INTEGER,
            PRIMARY KEY (produto, centro)
        );
        CREATE TABLE IF NOT EXISTS canal_summary (
            canal TEXT PRIMARY KEY,
            total_valor REAL
        );
        CREATE TABLE IF NOT EXISTS estado_summary (
            estado TEXT PRIMARY KEY,
            total_qtd INTEGER
        );
        """
    )
    conn.commit()


def _upsert(
    conn: sqlite3.Connection,
    pc: Dict[Tuple[str, str], Tuple[float, int]],
    canal: Dict[str, float],
    estado: Dict[str, int],
):
    cur = conn.cursor()
    cur.executemany(
        f"""INSERT INTO {SUMMARY_TABLE}
              (produto,centro,total_valor,total_qtd)
            VALUES (?,?,?,?)
            ON CONFLICT(produto,centro) DO UPDATE SET
              total_valor=total_valor+excluded.total_valor,
              total_qtd  =total_qtd  +excluded.total_qtd""",
        [(p, c, v, q) for (p, c), (v, q) in pc.items()],
    )
    cur.executemany(
        """INSERT INTO canal_summary (canal,total_valor)
           VALUES (?,?)
           ON CONFLICT(canal) DO UPDATE SET total_valor=total_valor+excluded.total_valor""",
        list(canal.items()),
    )
    cur.executemany(
        """INSERT INTO estado_summary (estado,total_qtd)
           VALUES (?,?)
           ON CONFLICT(estado) DO UPDATE SET total_qtd=total_qtd+excluded.total_qtd""",
        list(estado.items()),
    )
    conn.commit()

###############################################################################
# MAIN PIPELINE ###############################################################
###############################################################################


def run_pipeline(
    *,
    csv_path: Path,
    json_path: Path,
    db_path: Path,
    csv_size: int,
    json_size: int,
    workers: int,
    loops: int,
    chunksize: int | None,
    regenerate: bool,
):
    # -------- (re)gera dados --------------------------------------------
    if regenerate or not csv_path.exists():
        generate_csv(csv_size, csv_path)
    if regenerate or not json_path.exists():
        generate_json(json_size, json_path)

    # -------- carrega ----------------------------------------------------
    records = _load_csv(csv_path) + _load_json(json_path)
    total = len(records)

    # -------- partição em chunks ----------------------------------------
    if not chunksize:
        chunksize = max(int(2_000_000 / 120), 20_000)  # 120 ≈ bytes/registro
    chunks = [records[i : i + chunksize] for i in range(0, total, chunksize)]

    # -------- processamento paralelo ------------------------------------
    wall_start = time.perf_counter()

    glob_pc: Dict[Tuple[str, str], Tuple[float, int]] = {}
    glob_canal: Dict[str, float] = {}
    glob_estado: Dict[str, int] = {}

    pool = HybridPool(limit=workers)
    futs = [pool.submit(_process_chunk, ch, loops) for ch in chunks]

    for fut in tqdm(as_completed(futs), total=len(futs), desc="Chunks"):
        pc, canal, estado = fut.result()
        _merge_pair(glob_pc, pc)
        _merge_num(glob_canal, canal)
        _merge_int(glob_estado, estado)

    pool.shutdown()
    wall_dur = time.perf_counter() - wall_start

    # -------- grava SQLite ----------------------------------------------
    conn = sqlite3.connect(db_path)
    _ensure_table(conn)
    _upsert(conn, glob_pc, glob_canal, glob_estado)
    conn.close()

    # -------- relatório --------------------------------------------------
    thr = total / wall_dur
    print("\n=========== REPORT ===========")
    print(f"Workers (hybrid) : {workers}  (máx CPU {CPU_LIMIT})")
    print(f"Dados            : {total:,} registros  CSV {csv_size:,} | JSON {json_size:,}")
    print(f"Loops/registro   : {loops}")
    print(f"Chunks           : {len(chunks)}  (≈{chunksize} regs)")
    print(f"Tempo wall       : {wall_dur:.1f}s   Throughput {thr:,.0f} r/s")

    top_canal = sorted(glob_canal.items(), key=lambda x: -x[1])[:5]
    top_estado = sorted(glob_estado.items(), key=lambda x: -x[1])[:5]
    print("\nTOP canais por receita:")
    for k, v in top_canal:
        print(f"  {k:<10} R$ {v:,.0f}")
    print("\nTOP estados por quantidade:")
    for k, v in top_estado:
        print(f"  {k:<2} {v:,}")

    print("\n--- Métricas internas ---\n", METRICS.report())
    print("==============================")

###############################################################################
# CLI #########################################################################
###############################################################################


def _parse() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Heavy‑load ETL benchmark (v5)")
    p.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    p.add_argument("--json", type=Path, default=DEFAULT_JSON)
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--csv-size", type=int, default=1_000_000)
    p.add_argument("--json-size", type=int, default=400_000)
    p.add_argument("--workers", type=int, default=min(os.cpu_count() or 4, CPU_LIMIT))
    p.add_argument("--loops", type=int, default=800)
    p.add_argument("--chunksize", type=int)
    p.add_argument("--regenerate", action="store_true")
    return p.parse_args()


def main() -> None:
    args = _parse()
    run_pipeline(
        csv_path=args.csv,
        json_path=args.json,
        db_path=args.db,
        csv_size=args.csv_size,
        json_size=args.json_size,
        workers=args.workers,
        loops=args.loops,
        chunksize=args.chunksize,
        regenerate=args.regenerate,
    )


if __name__ == "__main__":
    main()
