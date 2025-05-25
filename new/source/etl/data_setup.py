import logging
from pathlib import Path

# Assuming these generators are in new.source.etl.data_generators
# Adjust the import path if they are located elsewhere.
try:
    from .data_generators import generate_csv as etl_generate_csv
    from .data_generators import generate_json as etl_generate_json
except ImportError:
    # Fallback if they are in a different location relative to this file
    # This might happen if data_generators.py is not in the same 'etl' package.
    # For example, if it's directly in new/source/etl/
    try:
        from new.source.etl.data_generators import generate_csv as etl_generate_csv
        from new.source.etl.data_generators import generate_json as etl_generate_json
        logging.info("Imported data_generators from new.source.etl.data_generators")
    except ImportError as e:
        logging.error(f"Could not import data generators: {e}. Data setup will fail if they are needed.")
        # Define dummy functions to avoid NameError if generation is attempted
        def etl_generate_csv(*args, **kwargs):
            raise NotImplementedError("generate_csv from data_generators not found.")
        def etl_generate_json(*args, **kwargs):
            raise NotImplementedError("generate_json from data_generators not found.")


logger = logging.getLogger(__name__)

def setup_etl_benchmark_data(sim_root: Path, regenerate: bool = False):
    logger.info(f"Setting up benchmark data in {sim_root}")
    sim_root.mkdir(parents=True, exist_ok=True)

    # CSV example
    etl_csv_path = sim_root / "contaverde" / "products_benchmark.txt"
    etl_csv_path.parent.mkdir(parents=True, exist_ok=True)
    if regenerate or not etl_csv_path.exists():
        logger.info(f"Generating CSV for ETL benchmark: {etl_csv_path}")
        try:
            etl_generate_csv(n_rows=1000000, out_path=etl_csv_path)
        except Exception as e:
            logger.error(f"Failed to generate {etl_csv_path}: {e}")


    # JSON example
    etl_json_path = sim_root / "orders_benchmark.json" # Ensure this path is used consistently
    etl_json_path.parent.mkdir(parents=True, exist_ok=True) # In case sim_root itself is orders_benchmark.json's parent
    if regenerate or not etl_json_path.exists():
        logger.info(f"Generating JSON for ETL benchmark: {etl_json_path}")
        try:
            # The original JSON generator creates {"pedidos": [...]}
            # Ensure your task 'orders_benchmark' expects "pedidos" as json_records_path
            etl_generate_json(n_rows=500000, out_path=etl_json_path, records_key_name="pedidos")
        except Exception as e:
            logger.error(f"Failed to generate {etl_json_path}: {e}")
        
    # TXT example
    etl_txt_path = sim_root / "cadeanalytics" / "cade_benchmark.txt"
    etl_txt_path.parent.mkdir(parents=True, exist_ok=True)
    if regenerate or not etl_txt_path.exists():
        logger.info(f"Generating TXT (as CSV) for ETL benchmark: {etl_txt_path}")
        try:
            etl_generate_csv(n_rows=300000, out_path=etl_txt_path, delimiter=",") # Original was comma-delimited
        except Exception as e:
            logger.error(f"Failed to generate {etl_txt_path}: {e}")


    # DataCat example
    datacat_dir = sim_root / "datacat_benchmark" / "behaviour"
    datacat_dir.mkdir(parents=True, exist_ok=True)
    if regenerate or not list(datacat_dir.glob("*.txt")): # Check if any .txt files exist
        logger.info(f"Generating DataCat files for ETL benchmark in {datacat_dir}")
        try:
            for i in range(5):
                etl_generate_csv(
                    n_rows=100000, # Adjust as needed
                    out_path=datacat_dir / f"events_log_bench_{i:02d}.txt", # Lexicographical sort
                    # DataCat files in example were user|event|detail, no header
                    include_header=False, 
                    delimiter="|" 
                )
        except Exception as e:
            logger.error(f"Failed to generate DataCat files in {datacat_dir}: {e}")

    logger.info(f"Benchmark data setup attempt finished for {sim_root}.")