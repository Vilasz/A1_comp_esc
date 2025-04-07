import concurrent.futures
from multiprocessing import JoinableQueue, cpu_count
import threading
import traceback
import os
import time
import queue
import uuid
from typing import List, Any, Tuple, Optional, Callable

from tqdm import tqdm


class ConcurrentPipeline:
    """
    A concurrent pipeline processor that handles multi-stage data processing
    with configurable stages and worker pools.

    Each stage runs in its own process pool with a dispatcher thread managing the flow.
    """

    # Sentinel object to signal the end of data
    SENTINEL = None

    def __init__(
        self,
        max_workers_per_stage: Optional[int] = None,
        max_buffer_size: int = 10,
        show_progress: bool = True,
        verbose: bool = True
    ):
        """
        Initialize the pipeline processor.

        Args:
            max_workers_per_stage: Maximum number of worker processes per stage.
                                   Defaults to half of CPU count.
            max_buffer_size: Maximum size of the buffer queues between stages.
            show_progress: Whether to show progress bars during processing.
            verbose: Whether to print detailed logs.
        """
        self.max_workers_per_stage = max_workers_per_stage or max(1, (cpu_count() or 4) // 2)
        self.max_buffer_size = max_buffer_size
        self.show_progress = show_progress
        self.verbose = verbose

        # Pipeline components
        self.stages = []
        self.queues = []
        self.pools = []
        self.dispatchers = []

        # Tracking
        self.is_running = False
        self.start_time = None
        self.processed_items = 0

        if self.verbose:
            print(f"Initialized pipeline - Workers per stage: {self.max_workers_per_stage}")

    def add_stage(self, name: str, worker_function: Callable, max_workers: Optional[int] = None) -> None:
        """
        Add a processing stage to the pipeline.

        Args:
            name: Name of the stage for logging
            worker_function: Function that processes data. Must accept a tuple (data, id)
                           and return a tuple (processed_data, id)
            max_workers: Optional override for number of workers for this specific stage
        """
        stage = {
            'name': name,
            'worker_function': worker_function,
            'max_workers': max_workers or self.max_workers_per_stage
        }
        self.stages.append(stage)

        if self.verbose:
            print(f"Added stage: '{name}' with {stage['max_workers']} workers")

    def _setup_pipeline(self) -> None:
        """
        Set up the pipeline components: queues, pools, and dispatchers.
        """
        if not self.stages:
            raise ValueError("No stages have been added to the pipeline")

        # Create queues (one per stage plus one for input)
        self.queues = []
        for i in range(len(self.stages) + 1):
            # The +1 is because we need an input queue for the first stage
            # and then one output queue per stage (which is input for the next stage)
            self.queues.append(JoinableQueue(maxsize=self.max_buffer_size))

        # Create process pools (one per stage)
        self.pools = []
        for stage in self.stages:
            pool = concurrent.futures.ProcessPoolExecutor(max_workers=stage['max_workers'])
            self.pools.append(pool)

        # Create dispatcher threads (one per stage)
        self.dispatchers = []
        for i, stage in enumerate(self.stages):
            input_queue = self.queues[i]
            # Last stage might not have an output queue
            output_queue = self.queues[i + 1] if i < len(self.stages) - 1 else None

            dispatcher = threading.Thread(
                target=self._dispatcher_thread_runner,
                args=(
                    f"Dispatcher-{stage['name']}",
                    input_queue,
                    output_queue,
                    stage['worker_function'],
                    self.pools[i]
                ),
                daemon=True
            )
            self.dispatchers.append(dispatcher)

    def _dispatcher_thread_runner(
        self,
        handler_name: str,
        input_queue: JoinableQueue,
        output_queue: Optional[JoinableQueue],
        processing_function: Callable,
        worker_pool: concurrent.futures.Executor,
    ) -> None:
        """
        Monitors input queue, submits tasks to pool, handles results and task completion.
        Runs until SENTINEL is received and all tasks are processed.
        """
        current_thread_id = threading.get_native_id()
        if self.verbose:
            print(f"{handler_name} Dispatcher {os.getpid()}/{current_thread_id} Started.")

        sentinel_received = False
        # active_futures = set()  # Track active futures

        while True:
            try:
                # Using blocking get with timeout
                item_package = input_queue.get(timeout=1.0)
                
                # --- Process the retrieved item ---
                if item_package is self.SENTINEL:
                    print(f"[{handler_name} Dispatcher] GET result: SENTINEL received")
                    sentinel_received = True # Set the flag
                    input_queue.task_done()

                elif item_package:
                    # Process actual data item
                    data, original_id = item_package
                    print(f"[{handler_name} Dispatcher] Processing Item ID {original_id}")

                    # Submitting to worker pool
                    future = worker_pool.submit(processing_function, item_package)
                    future.original_id = original_id
                    future.add_done_callback(
                        lambda f: self._handle_result_and_signal(f, output_queue, input_queue)
                    )

                else:
                    # Handle unexpected None case
                    print(f"[{handler_name} Dispatcher] GET result: Unexpected None or Empty")
            
            except queue.Empty:
                # If we've received the sentinel and the queue is now empty,
                # we can be confident all work is complete
                if sentinel_received and input_queue.empty():
                    break
                continue

            except Exception as e:
                print(f"!!! [{handler_name} Dispatcher] Unexpected error: {e}")
                traceback.print_exc() # Print full traceback for the error
                
                # Call task_done if got an item but failed to process it
                if 'item_package' in locals() and item_package is not self.SENTINEL:
                    input_queue.task_done()
                
                time.sleep(0.5) # Avoid tight loop on persistent error

        # Propagate SENTINEL after all processing is done
        if output_queue is not None:
            if self.verbose:
                print(f"[{handler_name}] Propagating SENTINEL")
            output_queue.put(self.SENTINEL)

        if self.verbose:
            print(f"[{handler_name} Dispatcher {os.getpid()}/{current_thread_id}] Finished.")

    def _handle_result_and_signal(
        self,
        future: concurrent.futures.Future,
        output_queue: Optional[JoinableQueue],
        input_queue_ref: JoinableQueue
    ) -> None:
        """
        Callback helper to handle result tuple, put to next queue, call task_done
        """
        original_id = "UNKNOWN"
        try:
            if hasattr(future, 'original_id'):
                original_id = future.original_id
            else:
                if self.verbose:
                    print(f"!!! Callback Warning: Future missing original_id attribute!")

            # No output unless the task succeeds
            result_for_output = None

            if future.cancelled():
                if self.verbose:
                    print(f"Task cancelled (ID: {original_id})")
            elif future.exception():
                exc = future.exception()
                print(f"!!! Task failed (ID: {original_id}) with exception: {exc}")
            else:
                # Task succeeded, get the result package (data, id)
                worker_result_package = future.result()
                # Only put the output if there *is* an output queue
                if output_queue is not None:
                    # Defining the result to be put in the output
                    result_for_output = worker_result_package
                    self.processed_items += 1

            if output_queue is not None and result_for_output is not None:
                try:
                    output_queue.put(result_for_output)
                except Exception as q_put_error:
                    print(f"Error putting result to output queue (ID: {original_id}), {q_put_error}")
        except Exception as callback_err:
            print(f"Error within callback logic (ID: {original_id}), {callback_err}")
            traceback.print_exc()

        finally:
            # --- Input queue signaling ---
            # Should crucially call task_done regardless of success/failure/outputting
            try:
                input_queue_ref.task_done()
            except ValueError:
                print(f"WARNING: task_done() called inappropriately (ID: {original_id}).")
            except Exception as td_error:
                print(f"Error calling task_done in callback (ID: {original_id}) {td_error}")

    def feed_data(self, data: Any, item_id: Optional[str] = None) -> None:
        """
        Feed a single data item into the pipeline.

        Args:
            data: The data to process
            item_id: Optional identifier for the data item. If not provided, a UUID will be generated.
        """
        if not self.is_running:
            raise RuntimeError("Pipeline is not running. Call start() first.")

        if item_id is None:
            item_id = str(uuid.uuid4())

        self.queues[0].put((data, item_id))
        if self.verbose:
            print(f"[Pipeline] Item (ID: {item_id}) put into first stage.")

    def start(self) -> None:
        """
        Start the pipeline processing.
        """
        if self.is_running:
            raise RuntimeError("Pipeline is already running")

        self._setup_pipeline()

        # Start all dispatcher threads
        if self.verbose:
            print("Starting dispatcher threads...")

        for dispatcher in self.dispatchers:
            dispatcher.start()

        self.is_running = True
        self.start_time = time.time()
        self.processed_items = 0

        if self.verbose:
            print("Pipeline started.")

    def add_batch(self, data_items: List[Tuple[Any, str]]) -> None:
        """
        Add a batch of data to the pipeline for processing.

        Args:
            data_items: List of (data, id) tuples to process
        """
        if not self.is_running:
            raise RuntimeError("Pipeline is not running. Call start() first.")

        for data, item_id in data_items:
            self.feed_data(data, item_id)

    def end(self) -> None:
        """
        Signal the end of the input data stream and wait for completion.
        """
        if not self.is_running:
            raise RuntimeError("Pipeline is not running")

        if self.verbose:
            print("[Pipeline] Sending SENTINEL to first stage...")

        self.queues[0].put(self.SENTINEL)

        self.wait_completion()

        self.shutdown()

    def wait_completion(self, timeout: Optional[float] = None) -> bool:
        """
        Wait for the pipeline to complete processing all items.
        """
        if not self.is_running:
            raise RuntimeError("Pipeline is not running")

        start_wait = time.time()

        try:
            if self.verbose:
                print("Waiting for all stages to complete processing...")

            # Join all queues to ensure processing is complete
            for i, q in enumerate(self.queues):
                if self.show_progress:
                    stage_name = self.stages[i]['name'] if i < len(self.stages) else "Final"
                    print(f"Waiting for {stage_name} stage to complete...")

                # Calculate remaining timeout if specified
                if timeout is not None:
                    elapsed = time.time() - start_wait
                    if elapsed >= timeout:
                        return False
                    remaining = timeout - elapsed
                else:
                    remaining = None

                q.join()

        except KeyboardInterrupt:
            print("Pipeline completion wait interrupted by user")
            return False

        if self.verbose:
            print("All queue stages joined. Proceeding to shutdown.")

        return True

    def shutdown(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        Shutdown the pipeline gracefully.
        """
        if not self.is_running:
            return

        print("\nShutting down pipeline components...")

        # First, wait for all pending tasks if requested
        if wait:
            self.wait_completion(timeout)

        # Then shut down pools to prevent new tasks
        if self.show_progress:
            pool_iter = tqdm(enumerate(self.pools), total=len(self.pools), desc="Shutting down worker pools")
        else:
            pool_iter = enumerate(self.pools)

        for i, p in pool_iter:
            try:
                p.shutdown(wait=False)  # Non-blocking
            except Exception as e:
                print(f"Error shutting down pool {i + 1}: {e}")

        # Wait for dispatchers to finish
        if self.show_progress:
            disp_iter = tqdm(enumerate(self.dispatchers), total=len(self.dispatchers), desc="Waiting for dispatchers")
        else:
            disp_iter = enumerate(self.dispatchers)

        for i, d in disp_iter:
            stage_name = self.stages[i]['name'] if i < len(self.stages) else "Final"
            join_timeout = timeout or 3  # Use provided timeout or default to 3 seconds

            d.join(timeout=join_timeout)
            if d.is_alive() and self.verbose:
                print(f"Note: {stage_name} dispatcher is completing tasks in the background")

        self.is_running = False
        end_time = time.time()
        elapsed = end_time - (self.start_time or end_time)

        print(f"\n--- Pipeline Shutdown Complete ---")
        print(f"Processed {self.processed_items} items in {elapsed:.2f} seconds")

if __name__ == "__main__":
    # Functions for simulating data pipeline
    def generate_random_numbers(min_val: int, max_val: int, size: int):
        """Generate a list of random numbers"""
        random_list = random.choices(range(min_val, max_val + 1), k=size)
        return random_list

    def filter_worker(data_tuple):
        """Filter even numbers from the list"""
        data, original_id = data_tuple
        print(f"[Filter Worker {os.getpid()}] Processing ID: {original_id} (Size: {len(data)})")
        filtered_data = [number for number in data if number % 2 == 0]
        print(f"[Filter Worker {os.getpid()}] Done ID: {original_id} (Even Size: {len(filtered_data)})")
        return filtered_data, original_id

    def sort_worker(data_tuple):
        """Sort the filtered list"""
        data, original_id = data_tuple
        print(f"[Sort Worker {os.getpid()}] Processing ID: {original_id} (Size: {len(data)})")
        sorted_data = sorted(data)
        print(f"[Sort Worker {os.getpid()}] Done ID: {original_id} (Sorted Size: {len(sorted_data)})")
        return sorted_data, original_id

    def print_worker(data_tuple):
        """Print the top 5 values from the sorted list"""
        data, original_id = data_tuple
        print(f"[Print Worker {os.getpid()}] Processing ID: {original_id} (Size: {len(data)})")
        top_5 = data[-1:-6:-1] if len(data) >= 5 else data[-1::-1]
        print(f"[ID: {original_id}] Top 5 numbers: {top_5}")
        return None, original_id

    # Configuration
    MIN_VALUE = 1
    MAX_VALUE = 1_000_000
    LIST_SIZE = 300_000
    NUM_RUNS = 5
    NUM_LISTS = 10
    
    print("--- Starting Pipeline Demo ---")
    
    # Create the pipeline with 3 stages
    print("Creating pipeline class")
    pipeline = ConcurrentPipeline(max_workers_per_stage=2, max_buffer_size=30, show_progress=True, verbose=True)
    
    # Add processing stages
    print("Adding stages")
    pipeline.add_stage("Filter", filter_worker)
    print("filter stage added")
    pipeline.add_stage("Sort", sort_worker)
    print("sort stage added")
    pipeline.add_stage("Print", print_worker)
    print("print stage added")
    
    print("--- Starting pipeline ---")
    time.sleep(1)
    start_time = time.time()
    
    # Starts the pipeline
    pipeline.start()

    # Prepare data
    for j in range(NUM_RUNS):
        print(50 * "=")
        print(f"Run {j + 1}/{NUM_RUNS}")
        for i in range(NUM_LISTS):
            data_items = []
            list_id = f'List-{i+1}'
            data = generate_random_numbers(MIN_VALUE, MAX_VALUE, LIST_SIZE)
            data_items.append((data, list_id))
            # pipeline.run_batch(data_items)
            print(f"Adding {i},{j} data batch")
            
            # Adds data to be processed
            pipeline.add_batch(data_items)
    
    # Run the pipeline with all data
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("--- Ending pipeline ---")
    
    # Ends the pipeline
    pipeline.end()
    
    print(f"Elapsed time: {elapsed_time}")
    
    print("Demo completed!")