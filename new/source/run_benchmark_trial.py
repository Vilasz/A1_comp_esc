# In A1_comp_esc/new/source/run_benchmark_trial.py

import subprocess
import time
import pandas as pd
import matplotlib.pyplot as plt
import os
from pathlib import Path # Keep this for general path handling if needed

# --- Configuration ---
WORKER_COUNTS = [1, 2, 4, os.cpu_count() // 2 if os.cpu_count() else 2, 8, 10, os.cpu_count() if os.cpu_count() else 4]

# --- Module paths FOR -m FLAG (relative to project root) ---
# These are not file paths, but Python module paths
MVP_MODULE_PATH = "new.source.framework.mvp_pipeline"
ETL_NEW_MODULE_PATH = "new.source.etl.ETL_new" # Assuming ETL_new.py is directly in new/source

# MVP Benchmark Config (these are just parameters for the script)
MVP_CSV_SIZE =100000
MVP_JSON_SIZE = 600000
MVP_LOOPS = 100

# ... (run_command_and_get_output function remains the same) ...
# ... (Ensure ETL_new.py is modified to accept --workers CLI argument as discussed previously) ...

def main():
    results = []
    project_root_dir = Path(__file__).resolve().parent.parent.parent # This gets A1_comp_esc

    # --- Initial Data Generation ---
    print("--- Generating Data for MVP Benchmark ---")
    cmd_mvp_setup = [
        "python", "-m", MVP_MODULE_PATH, "--regenerate",
        f"--csv-size={MVP_CSV_SIZE}", f"--json-size={MVP_JSON_SIZE}", f"--loops={MVP_LOOPS}"
    ]
    # When using -m, Python needs to run from a directory where it can find 'new'
    # So, we set the `cwd` (current working directory) for the subprocess.
    subprocess.run(cmd_mvp_setup, check=True, cwd=str(project_root_dir))


    print("\n--- Generating Data for ETL_new Benchmark ---")
    # Assuming ETL_new.py has the --setup-data-etl flag
    cmd_etl_setup = ["python", "-m", ETL_NEW_MODULE_PATH, "--setup-data-etl"]
    subprocess.run(cmd_etl_setup, check=True, cwd=str(project_root_dir))


    # --- Run MVP Benchmark Trial ---
    print("\n--- Starting MVP Benchmark Trial ---")
    for workers in WORKER_COUNTS:
        print(f"Running MVP Benchmark with {workers} workers...")
        cmd = [
            "python", "-m", MVP_MODULE_PATH,
            f"--csv-size={MVP_CSV_SIZE}", f"--json-size={MVP_JSON_SIZE}", f"--loops={MVP_LOOPS}",
            f"--workers={workers}"
        ]
        processing_time, output = run_command_and_get_output(cmd, cwd=str(project_root_dir)) # Add cwd
        results.append({"pipeline": "MVP", "workers": workers, "time": processing_time})
        print(f"MVP with {workers} workers took {processing_time:.2f}s")

    # --- Run ETL_new Benchmark Trial ---
    print("\n--- Starting ETL_new Benchmark Trial ---")
    for workers in WORKER_COUNTS:
        print(f"Running ETL_new Benchmark with {workers} workers...")
        cmd = ["python", "-m", ETL_NEW_MODULE_PATH, f"--workers={workers}"]
        processing_time, output = run_command_and_get_output(cmd, cwd=str(project_root_dir)) # Add cwd
        results.append({"pipeline": "ETL_new", "workers": workers, "time": processing_time})
        print(f"ETL_new with {workers} workers took {processing_time:.2f}s")

    # ... (Process and Plot Results - no change here) ...
    # ...
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

    plt.savefig("benchmark_performance_graph.png") # Will be saved in project_root_dir
    print(f"\nGraph saved to {project_root_dir / 'benchmark_performance_graph.png'}")
    # plt.show() # Uncomment if you want to see it immediately

# Modify run_command_and_get_output to accept cwd
def run_command_and_get_output(command_args: list[str], cwd: str | None = None) -> tuple[float, str]:
    print(f"Running: {' '.join(command_args)} (in CWD: {cwd or os.getcwd()})")
    start_time = time.perf_counter()
    # Pass cwd to Popen
    process = subprocess.Popen(command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=cwd)
    stdout, stderr = process.communicate()
    end_time = time.perf_counter()
    run_duration = end_time - start_time

    processing_time_from_stdout = None
    if stdout:
        for line in stdout.splitlines():
            # Using module path in checks for clarity
            if "Processing Wall Time" in line and MVP_MODULE_PATH in " ".join(command_args) :
                try:
                    processing_time_from_stdout = float(line.split(":")[1].strip().replace("s", ""))
                    break
                except (ValueError, IndexError): pass
            elif "Generic ETL Pipeline finished in" in line and ETL_NEW_MODULE_PATH in " ".join(command_args):
                try:
                    processing_time_from_stdout = float(line.split("finished in")[1].strip().replace("seconds.", "").replace("s",""))
                    break
                except (ValueError, IndexError): pass
    if stderr and process.returncode != 0 : # Only print stderr if there was an error for cleaner output
        print(f"STDERR (return code {process.returncode}):")
        print(stderr)
    
    actual_processing_time = processing_time_from_stdout if processing_time_from_stdout is not None else run_duration
    if process.returncode != 0:
        # Raise an error if the subprocess failed, to make it obvious
        raise subprocess.CalledProcessError(process.returncode, command_args, output=stdout, stderr=stderr)

    return actual_processing_time, stdout + "\n" + stderr


if __name__ == "__main__":
    main()