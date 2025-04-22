# mvp_pipeline.py  (v5 — heavy‑load, métricas ricas e pool híbrido)
"""
ETL paralelizado  para datasets gigantes gerados por data_generators.py.

"""

from __future__ import annotations
from concurrent.futures import as_completed

import argparse
import os
import random
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

from tqdm import tqdm

from new.source.etl.data_generators import CENTERS, CHANNELS, PRODUCTS, generate_csv, generate_json
from new.source.utils.loaders import DEFAULT_JSON, DEFAULT_CSV, load_csv, load_json
from new.source.etl.database import DEFAULT_DB, ensure_table, upsert
from new.source.framework.hybrid_pool import HybridPool, CPU_LIMIT
from new.source.utils.metrics import METRICS
from new.source.framework.worker import process_chunk, merge_int, merge_num, merge_pair



# MAIN PIPELINE 



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
    #  (re)gera dados 
    if regenerate or not csv_path.exists():
        generate_csv(csv_size, csv_path)
    if regenerate or not json_path.exists():
        generate_json(json_size, json_path)

    #  carrega 
    records = load_csv(csv_path) + load_json(json_path)
    total = len(records)

    #  partição em chunks 
    if not chunksize:
        chunksize = max(int(2_000_000 / 120), 20_000)  # 120 ≈ bytes/registro
    chunks = [records[i : i + chunksize] for i in range(0, total, chunksize)]

    #  processamento paralelo 
    wall_start = time.perf_counter()

    glob_pc: Dict[Tuple[str, str], Tuple[float, int]] = {}
    glob_canal: Dict[str, float] = {}
    glob_estado: Dict[str, int] = {}

    pool = HybridPool(limit=workers)
    futs = [pool.submit(process_chunk, ch, loops) for ch in chunks]

    for fut in tqdm(as_completed(futs), total=len(futs), desc="Chunks"):
        pc, canal, estado = fut.result()
        merge_pair(glob_pc, pc)
        merge_num(glob_canal, canal)
        merge_int(glob_estado, estado)

    pool.shutdown()
    wall_dur = time.perf_counter() - wall_start

    #  grava SQLite 
    conn = sqlite3.connect(db_path)
    ensure_table(conn)
    upsert(conn, glob_pc, glob_canal, glob_estado)
    conn.close()

    #  relatório 
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



def _parse() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Heavy‑load ETL benchmark (v5)")
    p.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    p.add_argument("--json", type=Path, default=DEFAULT_JSON)
    p.add_argument("--db", type=Path, default=DEFAULT_DB)
    p.add_argument("--csv-size", type=int, default=1_00)
    p.add_argument("--json-size", type=int, default=40)
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
