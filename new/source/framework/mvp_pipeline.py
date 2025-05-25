# new/source/framework/mvp_pipeline.py
from __future__ import annotations

import argparse
import os
import sqlite3 # For direct DB connection
import time
from pathlib import Path
from typing import Dict, Tuple, List, Sequence # Added List, Sequence
from concurrent.futures import as_completed, Future
from tqdm import tqdm # For progress bar

# --- Project-specific imports ---
# Assuming these are correctly located relative to this file or in PYTHONPATH
from new.source.etl.data_generators import generate_csv, generate_json # Re-use data generators
from new.source.utils.loaders import load_csv, load_json # Re-use basic loaders
from new.source.etl.database import DEFAULT_DB as DEFAULT_MVP_DB, ensure_table, upsert # DB utilities
from new.source.framework.hybrid_pool import HybridPool, CPU_LIMIT
from new.source.utils.metrics import METRICS # Assuming a global METRICS object exists
from new.source.framework.worker import process_chunk, merge_int, merge_num, merge_pair # Core worker

import logging
from typing import Optional # For optional type hints

# --- Configuration specific to this MVP/Benchmark ---
# These can be different from the generic ETL's PATH_CONFIG
# It's good practice to make paths configurable via argparse or a dedicated config.
MVP_DEFAULT_CSV = Path("mvp_mock_data.csv")
MVP_DEFAULT_JSON = Path("mvp_mock_data_new.json")


# --- Main MVP Pipeline Logic ---
def run_mvp_benchmark_pipeline(
    *,
    csv_path: Path,
    json_path: Path,
    db_path: Path,
    csv_size: int,
    json_size: int,
    num_workers: int, # Renamed from 'workers' for clarity
    loops_per_record: int, # Renamed from 'loops'
    processing_chunk_size: Optional[int], # Renamed from 'chunksize'
    regenerate_data: bool, # Renamed from 'regenerate'
):
    logger = logging.getLogger(__name__) # Local logger for this module
    logger.info("Starting MVP Benchmark Pipeline...")

    # 1. (Re)generate mock data
    # This part is specific to the benchmark's setup
    if regenerate_data or not csv_path.exists():
        logger.info(f"Generating {csv_size:,} CSV records into {csv_path}...")
        generate_csv(csv_size, csv_path) # generate_csv from data_generators.py
    if regenerate_data or not json_path.exists():
        logger.info(f"Generating {json_size:,} JSON records into {json_path}...")
        generate_json(json_size, json_path) # generate_json from data_generators.py

    # 2. Load raw records from files (using basic loaders)
    # This step loads all data into memory first. For truly massive datasets,
    # this might need to be integrated with a streaming/chunked read from disk.
    logger.info(f"Loading records from {csv_path} and {json_path}...")
    # load_csv and load_json return List[Tuple[str, int, str, float, str, str]]
    # which is the expected input for process_chunk
    try:
        raw_records: List[Tuple[str, int, str, float, str, str]] = load_csv(csv_path) + load_json(json_path)
    except FileNotFoundError:
        logger.error(f"Data files not found. CSV: {csv_path}, JSON: {json_path}. Please generate them or check paths.")
        return
    except Exception as e:
        logger.error(f"Error loading initial data: {e}", exc_info=True)
        return
        
    total_records = len(raw_records)
    if total_records == 0:
        logger.warning("No records loaded. Exiting benchmark.")
        return
    logger.info(f"Loaded {total_records:,} total records.")

    # 3. Partition records into chunks for parallel processing
    actual_processing_chunk_size = processing_chunk_size
    if not actual_processing_chunk_size:
        # Heuristic from original: 120 bytes/record approx.
        # Aim for chunks that are neither too small (overhead) nor too large (memory, load imbalance)
        actual_processing_chunk_size = max(int(total_records / (num_workers * 4)), 10000) # e.g. 4 chunks per worker
        actual_processing_chunk_size = min(actual_processing_chunk_size, 100_000) # Max chunk size
    logger.info(f"Processing chunk size: {actual_processing_chunk_size:,} records.")
    
    record_chunks: List[Sequence[Tuple[str, int, str, float, str, str]]] = [
        raw_records[i : i + actual_processing_chunk_size]
        for i in range(0, total_records, actual_processing_chunk_size)
    ]
    num_chunks = len(record_chunks)
    logger.info(f"Split into {num_chunks} chunks for processing.")

    # 4. Parallel processing using HybridPool and `process_chunk` worker
    logger.info(f"Starting parallel processing with {num_workers} workers (HybridPool limit)...")
    wall_start_time = time.perf_counter()

    # Global accumulators for aggregated results
    # Types match the return type of process_chunk and input of merge_ helpers
    global_prod_center_agg: Dict[Tuple[str, str], Tuple[float, int]] = {}
    global_canal_revenue_agg: Dict[str, float] = {}
    global_estado_quantity_agg: Dict[str, int] = {}

    # Ensure METRICS is cleared or scoped if this can be run multiple times in one session
    # For simplicity, assuming METRICS is a global singleton that accumulates.
    # If running benchmarks repeatedly, you might want MetricsManager.new_run() or similar.

    with HybridPool(limit=num_workers) as pool: # HybridPool manages its own shutdown
        futures: List[Future[Tuple[Dict, Dict, Dict]]] = [
            pool.submit(process_chunk, chunk, loops_per_record) for chunk in record_chunks
        ]

        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Chunks"):
            try:
                # process_chunk returns: prod_center, canal_tot, estado_qtd
                chunk_pc_agg, chunk_canal_agg, chunk_estado_agg = future.result()
                
                # Merge results from this chunk into global accumulators
                merge_pair(global_prod_center_agg, chunk_pc_agg)
                merge_num(global_canal_revenue_agg, chunk_canal_agg)
                merge_int(global_estado_quantity_agg, chunk_estado_agg)
            except Exception as e:
                logger.error(f"Error processing a chunk: {e}", exc_info=True)
                # Decide if one chunk error should halt the whole benchmark

    processing_duration_sec = time.perf_counter() - wall_start_time
    logger.info(f"Parallel processing finished in {processing_duration_sec:.2f} seconds.")

    # 5. Store aggregated results into SQLite
    logger.info(f"Storing aggregated results into database: {db_path}")
    try:
        with sqlite3.connect(db_path) as conn: # Context manager handles close
            ensure_table(conn) # ensure_table from etl.database
            upsert(conn, global_prod_center_agg, global_canal_revenue_agg, global_estado_quantity_agg) # upsert from etl.database
        logger.info("Aggregated results stored successfully.")
    except Exception as e:
        logger.error(f"Error storing results to database {db_path}: {e}", exc_info=True)

    # 6. Report benchmark results
    throughput_rps = total_records / processing_duration_sec if processing_duration_sec > 0 else 0
    print("\n=========== MVP BENCHMARK REPORT ===========")
    print(f"Target Database      : {db_path}")
    print(f"Hybrid Pool Workers  : {num_workers} (Configured CPU Limit: {CPU_LIMIT})")
    print(f"Input Data Sources   : CSV ({csv_size:,} requested), JSON ({json_size:,} requested)")
    print(f"Total Records Loaded : {total_records:,}")
    print(f"CPU Loops per Record : {loops_per_record}")
    print(f"Processing Chunks    : {num_chunks} (approx. {actual_processing_chunk_size:,} records each)")
    print(f"Processing Wall Time : {processing_duration_sec:.2f}s")
    print(f"Overall Throughput   : {throughput_rps:,.0f} records/sec")

    if global_canal_revenue_agg:
        top_canals = sorted(global_canal_revenue_agg.items(), key=lambda x: x[1], reverse=True)[:5]
        print("\nTOP Canals by Revenue (Aggregated):")
        for canal, revenue in top_canals:
            print(f"  {canal:<15} R$ {revenue:,.2f}")

    if global_estado_quantity_agg:
        top_estados = sorted(global_estado_quantity_agg.items(), key=lambda x: x[1], reverse=True)[:5]
        print("\nTOP Estados by Quantity (Aggregated):")
        for estado, quantity in top_estados:
            print(f"  {estado:<5} {quantity:,} items")
    
    # Assuming METRICS is imported and accumulates globally.
    # If METRICS needs to be scoped to a run, it should be instantiated/reset.
    print("\n--- Internal Framework Metrics ---")
    # Ensure METRICS object is available and has a report method
    if hasattr(METRICS, 'report') and callable(METRICS.report):
        print(METRICS.report())
    else:
        print("METRICS object not configured or does not have a report method.")
    print("==========================================")

    return processing_duration_sec


# --- Argument Parsing and Main Execution for MVP ---
def _parse_mvp_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MVP Heavy-Load ETL Benchmark (Refactored)")
    parser.add_argument("--csv-path", type=Path, default=MVP_DEFAULT_CSV, help="Path to input/output CSV file.")
    parser.add_argument("--json-path", type=Path, default=MVP_DEFAULT_JSON, help="Path to input/output JSON file.")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_MVP_DB, help="Path to SQLite database file for results.")
    
    parser.add_argument("--csv-size", type=int, default=100_000, help="Number of records to generate for CSV.")
    parser.add_argument("--json-size", type=int, default=40_000, help="Number of records to generate for JSON.")
    
    parser.add_argument("--workers", type=int, default=min(os.cpu_count() or 4, CPU_LIMIT), help="Number of parallel workers for HybridPool.")
    parser.add_argument("--loops", type=int, default=100, help="Number of CPU-bound loops per record in process_chunk.")
    parser.add_argument("--chunksize", type=int, help="Number of records per processing chunk. Auto-calculated if not set.")
    
    parser.add_argument("--regenerate", action="store_true", help="Regenerate mock data files even if they exist.")
    return parser.parse_args()

def main_mvp() -> None:
    args = _parse_mvp_args()
    processing_time = run_mvp_benchmark_pipeline(
        csv_path=args.csv_path,
        json_path=args.json_path,
        db_path=args.db_path,
        csv_size=args.csv_size,
        json_size=args.json_size,
        num_workers=args.workers,
        loops_per_record=args.loops,
        processing_chunk_size=args.chunksize,
        regenerate_data=args.regenerate,
    )

if __name__ == "__main__":
    # Basic logging config for the script if run directly
    # The ETL.py might have its own more specific config if it's the entry point
    if not logging.getLogger().handlers: # Configure only if no handlers are set
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)8s] [%(processName)-12s %(threadName)s] %(message)s")
    main_mvp()