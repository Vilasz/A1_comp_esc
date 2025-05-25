import logging
from pathlib import Path
import os

# --- Logging Configuration ---
LOG_FORMAT = "%(asctime)s [%(levelname)8s] [%(threadName)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__) # For any config-specific logging

# --- Path Configuration ---
try:
    _PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent # Adjust if structure is deeper/shallower
    PATH_CONFIG = {
        "SIMULATOR_DATA_ROOT": _PROJECT_ROOT / "simulator_data",
        "DEFAULT_SQLITE_OUTPUT_DB_PATH": _PROJECT_ROOT / "output_data" / "etl_sqlite_output.db",
        "TIMES_FILE_PATH": _PROJECT_ROOT / "output_data" / "etl_times.txt", # For LoadStage timing
    }
    if "DEFAULT_SQLITE_OUTPUT_DB_PATH" in PATH_CONFIG:
         PATH_CONFIG["DEFAULT_SQLITE_OUTPUT_DB_PATH"].parent.mkdir(parents=True, exist_ok=True)
    if "TIMES_FILE_PATH" in PATH_CONFIG:
         PATH_CONFIG["TIMES_FILE_PATH"].parent.mkdir(parents=True, exist_ok=True)

except Exception as e:
    logger.warning(f"Could not determine project paths based on __file__: {e}. Paths may need manual configuration.")
    PATH_CONFIG = {} # Fallback to empty, expect errors downstream or manual setup


# --- Database Configuration ---
ETL_POSTGRES_DB_CONFIG = {
    "host": "localhost",
    "database": "my_etl_db",
    "user": "my_etl_user",
    "password": "your_secure_password" # CHANGEME
}

# --- Concurrency/Resource Limits ---
# CPU_LIMIT is often defined near HybridPool, but if it's a general config, it can be here.
# For now, assume it's imported from hybrid_pool where needed.
# Or define it here if it's a top-level configuration:
# CPU_LIMIT = min(32, (os.cpu_count() or 1) + 4) # Example definition

# --- Default Values for ETL Stages (can be overridden) ---
DEFAULT_CHUNK_SIZE_LINES_EXTRACT = 100000
DEFAULT_CHUNK_SIZE_ROWS_TRANSFORM = 50000
DEFAULT_CHUNK_SIZE_ROWS_ORDERS = 10000
DEFAULT_PG_EVENT_BATCH_SIZE = 100