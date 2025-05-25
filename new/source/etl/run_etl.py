import logging
import queue
import time
import os
import argparse
import json # For dummy file generation
from pathlib import Path
from typing import List, Callable, Dict, Any, Union, Optional, Tuple, cast

# Framework and Utilities
from new.source.utils.dataframe import DataFrame
from new.source.etl.ETL_utils import MapMutex, RepoData
from new.source.framework.hybrid_pool import HybridPool, CPU_LIMIT
from new.source.framework.using_threads import ThreadWrapper

# ETL Components (relative imports from within the 'etl' package)
# Ensure these paths match your project structure.
try:
    from .config import PATH_CONFIG, ETL_POSTGRES_DB_CONFIG
    from .etl_types import (
        ExtractionTaskItem, DataFrameQueueItem, ExtractionTaskPayload,
        ExtractedDataQueueItem, ProcessedDataQueueItem, LoadStageInputItem
    )
    from .ETL_stages import ExtractStage, TransformStage, OrderProcessingStage, LoadStage
    from .data_setup import setup_etl_benchmark_data
    # Check if OrderProcessingStage can be used (imports worked)
    from .ETL_stages.order_processing_stage import ORDERS_LOGIC_AVAILABLE
except ImportError as e:
    logging.error(f"Failed to import ETL components: {e}. Check your package structure and dependencies.")
    # You might want to exit here or handle this more gracefully.
    exit(1)

logger = logging.getLogger(__name__)

# Define Sentinel Type for Queues
Sentinel = type(None)

# --- The New Dispatcher Stage ---
class DispatcherStage(ThreadWrapper[str, Any]):
    """
    Reads items from ExtractStage and routes them to the appropriate
    downstream queue (Orders, Transform, or Load).
    """
    def __init__(
        self,
        in_queue: queue.Queue[Union[DataFrameQueueItem, ExtractedDataQueueItem, None]],
        orders_queue: Optional[queue.Queue[ExtractedDataQueueItem]],
        transform_queue: Optional[queue.Queue[DataFrameQueueItem]],
        load_queue: queue.Queue[LoadStageInputItem], # Always needs load queue
        has_transform_stage: bool,
        name: str = "DispatcherStage",
    ):
        super().__init__(in_queue, out_queue=None, name=name) # No single out_queue
        self.orders_queue = orders_queue
        self.transform_queue = transform_queue
        self.load_queue = load_queue
        self.has_transform_stage = has_transform_stage
        self.consumers_count = sum(1 for q in [orders_queue, transform_queue if has_transform_stage else load_queue] if q is not None)


    def handle_item(self, task_id: str, item_value: Any) -> None:
        """
        Inspects the item and puts it onto the correct downstream queue.
        ThreadWrapper passes (key, value). We need to handle both possible
        formats from ExtractStage:
        1. DataFrameQueueItem -> (task_id, DataFrame)
        2. ExtractedDataQueueItem -> (task_id, (DataFrame, ExtractionTaskPayload))
        """

        df: Optional[DataFrame] = None
        payload: Optional[ExtractionTaskPayload] = None

        # Unpack based on the two possible structures
        if isinstance(item_value, DataFrame):
            # This is DataFrameQueueItem's value part
            df = item_value
            # We don't have the payload, assume it's not for orders
            payload = {"processing_type": "generic"}
        elif isinstance(item_value, tuple) and len(item_value) == 2 and isinstance(item_value[0], DataFrame) and isinstance(item_value[1], dict):
             # This is ExtractedDataQueueItem's value part
            df, payload = item_value
        else:
            logger.error(f"{self.name}: Received unknown item format for task '{task_id}'. Type: {type(item_value)}. Skipping.")
            return

        processing_type = payload.get("processing_type")

        if processing_type == "ecommerce_new_orders" and self.orders_queue:
            logger.debug(f"{self.name}: Routing task '{task_id}' to OrderProcessingStage.")
            # We need to send (task_id, df, payload) - ThreadWrapper will only put 'value'
            # So, we need to reconstruct the full ExtractedDataQueueItem
            self.orders_queue.put((task_id, df, payload)) # Put the full item
        elif self.has_transform_stage and self.transform_queue:
            logger.debug(f"{self.name}: Routing task '{task_id}' to TransformStage.")
            self.transform_queue.put((task_id, df)) # Send only (task_id, df)
        else:
            logger.debug(f"{self.name}: Routing task '{task_id}' directly to LoadStage.")
            self.load_queue.put((task_id, df)) # Send only (task_id, df)

        return None # No direct output from handle_item in ThreadWrapper

    def run(self):
        """Override run to handle sentinel and multiple output queues."""
        logger.info(f"{self.name}: Starting...")
        while self._running or not self.in_queue.empty():
            try:
                item = self.in_queue.get(timeout=0.5)
                if item is None: # Sentinel received
                    logger.info(f"{self.name}: Sentinel received. Draining queue...")
                    self._running = False # Signal to stop trying to get more
                    continue # Go back to check in_queue.empty()

                task_id, item_value = item # Unpack the item
                self.handle_item(task_id, item_value)
                self.in_queue.task_done() # Mark task as processed

            except queue.Empty:
                if not self._running:
                    break # Exit loop if stopped and queue is empty
                continue # If running, just wait for more items
            except Exception as e:
                logger.error(f"{self.name}: Error processing item: {e}", exc_info=True)
                # Decide if you should mark task_done or stop
                # For now, we try to continue, but mark as done to avoid join issues
                self.in_queue.task_done()

        logger.info(f"{self.name}: Sending sentinels downstream...")
        # Send sentinels to all connected downstream queues
        if self.orders_queue: self.orders_queue.put(None)
        if self.transform_queue: self.transform_queue.put(None)
        # If transform exists, it sends to loader. If not, we send directly.
        # This logic is tricky. It's better to always send to loader *if*
        # transform doesn't exist. Let's assume LoadStage can handle multiple Nones.
        # A safer way: send one None *per input source* to LoadStage.
        # The Dispatcher is one source (if no transform).
        if not self.has_transform_stage:
             self.load_queue.put(None) # Send one sentinel if we feed loader directly

        logger.info(f"{self.name}: Stopped.")

# --- Main ETL Orchestration ---
def run_pipeline_orchestrator(
    tasks: List[ExtractionTaskItem],
    transform_function: Optional[Callable[[DataFrame, Any], DataFrame]] = None,
    transform_params: Optional[Dict[str, Any]] = None,
    db_target_sqlite: Union[str, Path] = PATH_CONFIG.get("DEFAULT_SQLITE_OUTPUT_DB_PATH", "etl_output.db"),
    db_table_prefix: str = "output_",
    db_if_exists: str = "append",
    ecommerce_db_path_sqlite: Optional[Path] = None,
    num_workers_hybrid_pool: int = min(CPU_LIMIT, (os.cpu_count() or 1)),
    queue_capacity: int = 100,
):
    logger.info("Starting ETL Pipeline Orchestrator...")
    pipeline_start_time = time.perf_counter()
    logger.info(f"Path Config used: {PATH_CONFIG}")
    logger.info(f"Hybrid Pool Workers: {num_workers_hybrid_pool} (CPU_LIMIT: {CPU_LIMIT})")
    logger.info(f"Target SQLite DB: {db_target_sqlite}")

    # --- Check if Order Processing is possible and needed ---
    has_order_tasks = any(t[1].get("processing_type") == "ecommerce_new_orders" for t in tasks)
    use_order_stage = has_order_tasks and ORDERS_LOGIC_AVAILABLE and ecommerce_db_path_sqlite is not None

    if has_order_tasks and not use_order_stage:
        logger.warning("ETL includes order tasks, but OrderProcessingStage cannot be used (logic unavailable or DB path missing). These tasks will be skipped or misrouted!")
    elif use_order_stage:
        logger.info(f"Order Processing Stage will be used. E-commerce DB: {ecommerce_db_path_sqlite}")
        if not Path(ecommerce_db_path_sqlite).exists():
             logger.warning(f"E-commerce DB '{ecommerce_db_path_sqlite}' does not exist. `get_last_pk` might fail. Ensure it's set up.")


    # --- Initialize Queues (using JoinableQueue for better shutdown) ---
    extract_tasks_q = queue.Queue[ExtractionTaskItem](maxsize=queue_capacity)
    dispatcher_in_q = queue.Queue[Union[DataFrameQueueItem, ExtractedDataQueueItem, Sentinel]](maxsize=queue_capacity)
    orders_in_q = queue.Queue[Union[ExtractedDataQueueItem, Sentinel]](maxsize=queue_capacity) if use_order_stage else None
    transform_in_q = queue.Queue[Union[DataFrameQueueItem, Sentinel]](maxsize=queue_capacity) if transform_function else None
    loader_in_q = queue.Queue[Union[LoadStageInputItem, Sentinel]](maxsize=queue_capacity)

    # --- Initialize Shared Resources ---
    map_mutex = MapMutex[str]()
    hybrid_pool = HybridPool(limit=num_workers_hybrid_pool)

    # --- Initialize Pipeline Stages (Threads) ---
    all_stages: List[ThreadWrapper] = []

    extractor = ExtractStage(
        extract_tasks_q,
        dispatcher_in_q,
        map_mutex,
        hybrid_pool
    )
    all_stages.append(extractor)

    dispatcher = DispatcherStage(
        dispatcher_in_q,
        orders_in_q,
        transform_in_q,
        loader_in_q,
        has_transform_stage=transform_function is not None
    )
    all_stages.append(dispatcher)

    order_processor: Optional[OrderProcessingStage] = None
    if use_order_stage:
        order_processor = OrderProcessingStage(
            orders_in_q,
            loader_in_q, # Output directly to loader
            hybrid_pool,
            ecommerce_db_path_sqlite # Must be provided
        )
        all_stages.append(order_processor)

    transformer: Optional[TransformStage] = None
    if transform_function:
        transformer = TransformStage(
            transform_in_q,
            loader_in_q, # Output directly to loader
            hybrid_pool,
            user_transform_function=transform_function,
            user_transform_params=transform_params
        )
        all_stages.append(transformer)

    loader = LoadStage(
        loader_in_q,
        db_path_or_conn=db_target_sqlite,
        target_table_prefix=db_table_prefix,
        if_exists_mode=db_if_exists,
        # Tell LoadStage how many upstream stages feed into it (how many sentinels to expect)
        num_inputs=sum([
            1 if use_order_stage else 0, # From Orders
            1 if transform_function else 0, # From Transform
            0 if transform_function else (1 if not use_order_stage else (1 if any(not t[1].get("processing_type") == "ecommerce_new_orders" for t in tasks) else 0) ) # Directly from Dispatcher if no transform and generic tasks exist
        ])
    )
    # Refined LoadStage input count:
    # 1. If OrderStage exists -> 1 input
    # 2. If TransformStage exists -> 1 input
    # 3. If TransformStage *doesn't* exist AND there are generic tasks -> 1 input (from Dispatcher)
    loader_input_count = 0
    if use_order_stage: loader_input_count += 1
    has_generic_tasks = any(t[1].get("processing_type") != "ecommerce_new_orders" for t in tasks)
    if transform_function:
        if has_generic_tasks: loader_input_count += 1
    else: # No transform
        if has_generic_tasks: loader_input_count += 1
    # Ensure at least 1 if any tasks exist, but this count should be accurate
    if loader_input_count == 0 and tasks: loader_input_count = 1 # Fallback, should not be needed.
    logger.info(f"Loader expects {loader_input_count} sentinel(s).")
    loader.num_inputs = loader_input_count # We need to modify LoadStage to use this.
    all_stages.append(loader)


    # --- Start Threads ---
    logger.info(f"Starting {len(all_stages)} pipeline stages...")
    for stage in all_stages:
        stage.start()

    # --- Enqueue Initial Tasks ---
    logger.info(f"Enqueuing {len(tasks)} tasks...")
    for task_id, task_payload in tasks:
        logger.debug(f"Enqueuing task: {task_id}")
        extract_tasks_q.put((task_id, task_payload))

    # --- Signal End of Tasks & Wait for Completion ---
    logger.info("All tasks enqueued. Signaling end of input...")
    # Send sentinel to the first stage
    extract_tasks_q.put(None)

    # --- Wait for Pipeline Completion ---
    logger.info("Waiting for pipeline stages to finish...")
    for stage in all_stages:
        stage.join() # Wait for each thread to complete its run method

    # --- Shutdown the hybrid pool ---
    logger.info("Shutting down Hybrid Pool...")
    hybrid_pool.shutdown()

    pipeline_end_time = time.perf_counter()
    pipeline_time_duration = pipeline_end_time - pipeline_start_time
    logger.info("ETL Pipeline execution time: %.2f seconds", pipeline_time_duration)
    logger.info("ETL Pipeline Orchestrator finished.")

    return pipeline_time_duration


def setup_dummy_data(sim_data_root: Path):
    """Creates minimal dummy files if benchmark data doesn't exist."""
    if not sim_data_root.is_dir():
        logger.warning(f"SIMULATOR_DATA_ROOT '{sim_data_root}' not found. Creating dummy structure.")
        try:
            (sim_data_root / "contaverde").mkdir(parents=True, exist_ok=True)
            (sim_data_root / "cadeanalytics").mkdir(parents=True, exist_ok=True)
            datacat_dir = sim_data_root / "datacat" / "behaviour"
            datacat_dir.mkdir(parents=True, exist_ok=True)

            products_path = sim_data_root / "contaverde" / "products.txt"
            if not products_path.exists():
                with open(products_path, "w", encoding="utf-8") as f:
                    f.write("product_id,product_name,category,price\n")
                    f.write("P001,Laptop,Electronics,1200.00\n")

            cade_path = sim_data_root / "cadeanalytics" / "cade_analytics.txt"
            if not cade_path.exists():
                with open(cade_path, "w", encoding="utf-8") as f:
                    f.write("company_id|segment|market_share\n")
                    f.write("C100|Tech|0.35\n")

            orders_path = sim_data_root / "orders_new.json"
            if not orders_path.exists():
                with open(orders_path, "w", encoding="utf-8") as f:
                    # Make this suitable for OrderProcessingStage
                    json.dump({"pedidos": [
                        {"data": "2025-05-25 12:00:00", "cliente_id": "CLI001", "produto_id": "PROD_X", "quantidade": 2, "preco_unitario": 10.50},
                        {"data": "2025-05-25 12:05:00", "cliente_id": "CLI002", "produto_id": "PROD_Y", "quantidade": 1, "preco_unitario": 55.00}
                    ]}, f, indent=2)

            if not list(datacat_dir.glob("*.txt")):
                with open(datacat_dir / "events_log_20230101_00.txt", "w", encoding="utf-8") as f:
                    f.write("userA|click|button1\n")

        except Exception as e_setup:
            logger.error(f"Failed to set up dummy files: {e_setup}")


if __name__ == "__main__":
    cli_parser = argparse.ArgumentParser(description="Run ETL Pipeline.")
    cli_parser.add_argument("--workers", type=int,
                            default=min(CPU_LIMIT, (os.cpu_count() or 1)),
                            help="Number of workers for the HybridPool.")
    cli_parser.add_argument("--setup-data", action="store_true",
                            help="Run data generation for benchmark and exit.")
    cli_parser.add_argument("--db-path", type=str,
                            default=PATH_CONFIG.get("DEFAULT_SQLITE_OUTPUT_DB_PATH"),
                            help="Path to the output SQLite database.")
    cli_parser.add_argument("--ecommerce-db", type=str,
                            default=None, # User MUST provide this if order processing is needed
                            help="Path to the e-commerce SQLite database for order processing.")
    cli_parser.add_argument("--if-exists", type=str, default="replace", choices=["append", "replace", "fail"],
                             help="Behavior if target DB tables exist.")

    cli_args = cli_parser.parse_args()

    sim_data_root = Path(PATH_CONFIG.get("SIMULATOR_DATA_ROOT", "simulator_data"))

    if cli_args.setup_data:
        print("Setting up benchmark data...")
        setup_etl_benchmark_data(sim_data_root, regenerate=True)
        print("Benchmark data setup complete. Exiting.")
        exit()

    # Ensure some data exists, even if dummy
    setup_dummy_data(sim_data_root)

    # --- Define Example ETL Tasks ---
    example_tasks: List[ExtractionTaskItem] = [
        ("products", {
            "kind": RepoData.Kind.CSV,
            "path": "contaverde/products.txt",
            "csv_has_header": True, "chunkable": True,
            "processing_type": "generic" # Explicitly generic
        }),
        ("cade_data", {
            "kind": RepoData.Kind.TXT,
            "path": "cadeanalytics/cade_analytics.txt",
            "txt_delimiter": "|", "txt_has_header": True, "chunkable": True,
            "processing_type": "generic" # Explicitly generic
        }),
        ("new_orders", {
            "kind": RepoData.Kind.JSON,
            "path": "orders_new.json",
            "json_records_path": "pedidos", # Matches dummy data
            "chunkable": False,
            # This task MUST go to OrderProcessingStage
            "processing_type": "ecommerce_new_orders"
        }),
        ("datacat_events", {
            "kind": "datacat_loader",
            "path": "datacat/behaviour",
            "txt_delimiter": "|", "txt_has_header": False,
            "processing_type": "generic" # Explicitly generic
        }),
        # Add benchmark tasks if they exist and you want to run them
        # ("products_benchmark", { ... }),
    ]

    # --- Define an example transformation function ---
    def example_transform_function(df: DataFrame, extra_param: str = "default") -> DataFrame:
        logger.info(f"Applying example_transform_function ({extra_param}) to DF shape {df.shape}")
        if not df.empty and "transformed_col" not in df.columns:
            df.add_column("transformed_col", [f"T_{i}" for i in range(df.shape[0])])
        return df

    # --- Run the pipeline ---
    run_pipeline_orchestrator(
        tasks=example_tasks,
        transform_function=example_transform_function, # Set to None to skip transform
        transform_params={"extra_param": "ETL_RUN"},
        db_target_sqlite=Path(cli_args.db_path),
        db_if_exists=cli_args.if_exists,
        ecommerce_db_path_sqlite=Path(cli_args.ecommerce_db) if cli_args.ecommerce_db else None,
        num_workers_hybrid_pool=cli_args.workers
    )