import subprocess
import time
import pandas as pd
import matplotlib.pyplot as plt
import os
from pathlib import Path
import sqlite3 # Import for creating dummy DB

# --- Configuration ---
# Use a smaller set for quicker testing, expand as needed
WORKER_COUNTS = [1, 2, os.cpu_count() // 2 if os.cpu_count() else 2, os.cpu_count() if os.cpu_count() else 4, 8]

# --- Module paths FOR -m FLAG (relative to project root) ---
MVP_MODULE_PATH = "new.source.framework.mvp_pipeline"
# <<< CHANGE 1: Update the module path to point to run_etl >>>
ETL_MODULE_PATH = "new.source.etl.run_etl" 

# MVP Benchmark Config
MVP_CSV_SIZE = 100000
MVP_JSON_SIZE = 600000
MVP_LOOPS = 100

# <<< CHANGE 2: Define paths for benchmark databases >>>
BENCHMARK_OUTPUT_DB = "output_data/benchmark_etl_output.db"
BENCHMARK_ECOMMERCE_DB = "output_data/benchmark_ecommerce.db"

# --- Function to setup Dummy E-commerce DB ---
def setup_dummy_ecommerce_db(db_path: Path):
    """Creates an empty SQLite DB with necessary tables if it doesn't exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if not db_path.exists():
        print(f"Creating dummy e-commerce DB at: {db_path}")
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            # Create minimal tables needed for get_last_pk to work
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS pedidos (
                    pedido_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cliente_id TEXT,
                    data_pedido TEXT,
                    status TEXT,
                    total REAL
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS itens_pedido (
                    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pedido_id INTEGER,
                    produto_id TEXT,
                    quantidade INTEGER,
                    preco_unitario REAL,
                    FOREIGN KEY (pedido_id) REFERENCES pedidos (pedido_id)
                )
            ''')
            # Add one dummy row so get_last_pk returns 1 (or 0 depending on impl.)
            # If get_last_pk returns 0 for empty, this isn't needed.
            # If it expects at least one row, add it. Let's assume it can handle 0.
            # cursor.execute("INSERT INTO pedidos (cliente_id) VALUES ('dummy')")
            # cursor.execute("INSERT INTO itens_pedido (pedido_id) VALUES (1)")
            conn.commit()
            conn.close()
            print("Dummy e-commerce DB created.")
        except Exception as e:
            print(f"ERROR: Could not create dummy e-commerce DB: {e}")
            # Decide if you want to exit or continue without it
            # exit(1)

# --- Updated run_command_and_get_output ---
def run_command_and_get_output(command_args: list[str], cwd: str | None = None) -> tuple[float, str]:
    print(f"Running: {' '.join(command_args)} (in CWD: {cwd or os.getcwd()})")
    start_time = time.perf_counter()
    process = subprocess.Popen(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=cwd)
    stdout, stderr = process.communicate()
    end_time = time.perf_counter()
    run_duration = end_time - start_time

    processing_time_from_stdout = None
    if stdout:
        for line in stdout.splitlines():
            # Check for MVP output (remains the same)
            if "Processing Wall Time" in line and MVP_MODULE_PATH in " ".join(command_args):
                try:
                    processing_time_from_stdout = float(line.split(":")[1].strip().replace("s", ""))
                    break
                except (ValueError, IndexError): pass
            # <<< CHANGE 3: Update parsing for the new ETL output >>>
            elif "ETL Pipeline execution time:" in line and ETL_MODULE_PATH in " ".join(command_args):
                try:
                    # Extracts the float value
                    processing_time_from_stdout = float(line.split(":")[1].strip().split(" ")[0])
                    break
                except (ValueError, IndexError): pass
                
    if stderr and process.returncode != 0:
        print(f"STDERR (return code {process.returncode}):")
        print(stderr)
    
    actual_processing_time = processing_time_from_stdout if processing_time_from_stdout is not None else run_duration
    
    if process.returncode != 0:
        print(f"STDOUT:\n{stdout}") # Print stdout on error too
        raise subprocess.CalledProcessError(process.returncode, command_args, output=stdout, stderr=stderr)

    return actual_processing_time, stdout + "\n" + stderr

# --- Main Benchmark Function ---
def main():
    results = []
    project_root_dir = Path(__file__).resolve().parent.parent.parent

    # --- Initial Data Generation ---
    print("--- Generating Data for MVP Benchmark ---")
    cmd_mvp_setup = [
        "python", "-m", MVP_MODULE_PATH, "--regenerate",
        f"--csv-size={MVP_CSV_SIZE}", f"--json-size={MVP_JSON_SIZE}", f"--loops={MVP_LOOPS}"
    ]
    subprocess.run(cmd_mvp_setup, check=True, cwd=str(project_root_dir))

    print("\n--- Generating Data for ETL Benchmark ---")
    # <<< CHANGE 4: Update ETL setup command >>>
    # Use the new module path and the --setup-data flag (if your run_etl.py has it)
    # If run_etl.py doesn't have --setup-data, you might need to call data_setup.py directly
    # Assuming run_etl.py *was* modified to include --setup-data:
    cmd_etl_setup = ["python", "-m", ETL_MODULE_PATH, "--setup-data"]
    try:
        subprocess.run(cmd_etl_setup, check=True, cwd=str(project_root_dir))
    except subprocess.CalledProcessError as e:
         print(f"ETL Data setup failed. Make sure '{ETL_MODULE_PATH}' supports '--setup-data'. Error: {e}")
         # If setup fails, maybe exit or try manual setup. For now, we'll try to continue.
         # exit(1) 

    # <<< CHANGE 5: Setup the dummy E-commerce DB >>>
    setup_dummy_ecommerce_db(project_root_dir / BENCHMARK_ECOMMERCE_DB)

    # --- Run MVP Benchmark Trial ---
    print("\n--- Starting MVP Benchmark Trial ---")
    for workers in WORKER_COUNTS:
        print(f"Running MVP Benchmark with {workers} workers...")
        cmd = [
            "python", "-m", MVP_MODULE_PATH,
            f"--csv-size={MVP_CSV_SIZE}", f"--json-size={MVP_JSON_SIZE}", f"--loops={MVP_LOOPS}",
            f"--workers={workers}"
        ]
        processing_time, output = run_command_and_get_output(cmd, cwd=str(project_root_dir))
        results.append({"pipeline": "MVP", "workers": workers, "time": processing_time})
        print(f"MVP with {workers} workers took {processing_time:.2f}s")

    # --- Run ETL Benchmark Trial ---
    print("\n--- Starting ETL Benchmark Trial ---")
    for workers in WORKER_COUNTS:
        print(f"Running ETL Benchmark with {workers} workers...")
        # <<< CHANGE 6: Update ETL run command with new args >>>
        cmd = [
            "python", "-m", ETL_MODULE_PATH, 
            f"--workers={workers}",
            f"--db-path={BENCHMARK_OUTPUT_DB}",
            f"--ecommerce-db={BENCHMARK_ECOMMERCE_DB}",
            "--if-exists=replace" # Use replace for consistent benchmarks
        ]
        processing_time, output = run_command_and_get_output(cmd, cwd=str(project_root_dir))
        results.append({"pipeline": "ETL", "workers": workers, "time": processing_time}) # Renamed to "ETL"
        print(f"ETL with {workers} workers took {processing_time:.2f}s")

    # --- Process and Plot Results ---
    df_results = pd.DataFrame(results)
    print("\n--- Benchmark Results ---")
    print(df_results)

    plt.figure(figsize=(12, 7))
    for pipeline_name, group in df_results.groupby("pipeline"):
        plt.plot(group["workers"], group["time"], marker='o', linestyle='-', label=pipeline_name)

    plt.title('Pipeline Performance vs. Number of Workers')
    plt.xlabel('Number of Workers')
    plt.ylabel('Processing Time (seconds)')
    plt.xticks(WORKER_COUNTS) 
    plt.legend()
    plt.grid(True)
    
    for i, row in df_results.iterrows():
        plt.text(row['workers'] + 0.1, row['time'], f"{row['time']:.2f}s", fontsize=9)

    graph_path = project_root_dir / "benchmark_performance_graph.png"
    plt.savefig(graph_path)
    print(f"\nGraph saved to {graph_path}")

if __name__ == "__main__":
    main()