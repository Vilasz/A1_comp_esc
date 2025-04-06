import concurrent.futures
from multiprocessing import JoinableQueue, cpu_count, Process
from threading import Thread

import os
import time
import queue
import random
import uuid

from typing import List, Any, Tuple, Optional, Callable

from tqdm import tqdm

# Module setup
MIN_VALUE = 1
MAX_VALUE = 1_000_000
LIST_SIZE = 300_000
NUM_LISTS_PER_RUN = 3
NUM_RUNS = 3
TOTAL_LISTS = NUM_LISTS_PER_RUN * NUM_RUNS
MAX_BUFFER_SIZE = 10

NUM_WORKERS = cpu_count() or 1

# Sentinel object to signal the end of data
# TODO: refine sentinel object
SENTINEL = None

def generate_random_numbers(min_val: int, max_val: int, size: int):
    random_list = random.choices(range(min_val, max_val + 1), k=size)
    return random_list

def filter_even_numbers(data: List[int]) -> List[int]:
    even_numbers = [number for number in data if number % 2 == 0]
    return even_numbers

def sort_list(data: List[int]) -> List[int]:
    sorted_data = sorted(data)
    return sorted_data

def print_top_5(sorted_data: List[int]) -> None:
    top_5 = sorted_data[-1:-6:-1]
    print(f"Top 5 numbers: {top_5}")

def pipeline():
    random_list = generate_random_numbers(min_value, max_value, list_size)
    print("Generated list")
    print(random_list)
    even_numbers = filter_even_numbers(random_list)
    print("Even numbers")
    print(even_numbers)
    sorted_list = sort_list(even_numbers)
    print("sorted list")
    print(sorted_list)
    print_top_5(sorted_list)

# pipeline()

def cpu_bound_handler_task(item):
    print(30 * "=")
    print("Simulating intensive work")
    # Simulate CPU-intensive work
    result = 0
    for i in tqdm(range(item * 10**6)): # Scale work with item value
        result += i
    # print(f"Worker {os.getpid()} processed {item}")
    return item * item

# --- Stage 1: Filter Worker ---
# The function gets the data in the id, makes the transformation and
# returns with the id
def filter_worker(data_tuple: Tuple[List[int], str]) -> Tuple[List[int], str]:
    """
    Worker function for the filtering stage
    """
    # Decoupling data and id
    data, original_id = data_tuple
    print(f"[Filter Worker {os.getpid()}] Processing ID: {original_id} (Size: {len(data)})")
    filtered_data = filter_even_numbers(data)
    print(f"[Filter Worker {os.getpid()}] Done ID: {original_id} (Even Size: {len(filtered_data)})")

    return filtered_data, original_id

# --- Stage 2: Sort Worker ---
def sort_worker(data_tuple: Tuple[List[int], str]) -> Tuple[List[int], str]:
    """
    Worker function for the sorting stage
    """
    # Decoupling data and id
    data, original_id = data_tuple
    print(f"[Sort Worker {os.getpid()}] Processing ID: {original_id} (Size: {len(data)})")
    sorted_data = sort_list(data)
    print(f"[Sort Worker {os.getpid()}] Done ID: {original_id} (Even Size: {len(sorted_data)})")

    return sorted_data, original_id

# --- Stage 3: Print Worker ---
def print_worker(data_tuple: Tuple[List[int], str]) -> Tuple[None, str]:
    """
    Worker function for the printing stage
    """
    
    # Decoupling data and id
    data, original_id = data_tuple
    print(f"[Print Worker {os.getpid()}] Processing ID: {original_id} (Size: {len(data)})")
    print_top_5(data)
    return None, original_id

# --- Callback Helper ---
# This handles the result tuple (data, id) and signals task done
def handle_result_and_signal(
    future: concurrent.futures.Future,
    output_queue: Optional[JoinableQueue],
    input_queue_ref: JoinableQueue
):
    """
    Callback helper to handle result tuple, put to next queue, call task_done
    """
    original_id = "UNKNOWN"
    try:
        # Retrieve the original ID stored when submitting the task
        original_id = future.original_id
        # No output unless the task succeeds
        result_for_output = None

        if future.cancelled():
            print(f"Task cancelled (ID: {original_id})")
        elif future.exception():
            print(f"!!! Task failed (ID: {original_id}) with exception: {future.exception()}")
        else:
            # Task succeeded, get the result package (data, id)
            worker_result_package = future.result()
            print(f"Task completed (ID: {original_id})")

            # Only put the output if there *is* an output queue
            if output_queue is not None:
                # Defining the result to be put in the output
                result_for_output = worker_result_package
        
        if output_queue is not None and result_for_output is not None:
            try:
                output_queue.put(result_for_output)
            except Exception as q_put_error:
                print(f"Error putting result to output queue (ID: {original_id}), {q_put_error}")
    except Exception as callback_err:
        print(f"Error within callback logic (ID: {original_id}), {callback_err}")

    finally:
        # --- Input queue signaling ---
        # Should crucially call task_done regardless of success/failure/outputting
        # Because an item was originally taken from the input queue
        try:
            input_queue_ref.task_done()
            print(f"Callback called task_done for ID {original_id}")
        except ValueError:
            print(40 * "=")
            print(f"WARNING: task_done() called inappropriately (ID: {original_id}). This may indicate the queue logic doesn't match future handling")
            pass
        except Exception as td_error:
            print(f"Error calling task_done in callback (ID: {original_id}) {td_error}")

# def task_done_callback(output_queue, input_queue_ref):
#     def callback(future):
#         # Keeping track if we should put some somethign
#         result_for_output = None
#         try:
#             if future.cancelled():
#                 print("Task was cancelled")
#             elif future.exception():
#                 print(f"Task failed with exception: {future.exception()}")
#             else:
#                 result = future.result()
#                 result_for_output = result
#                 print(f"Task completed successfully with result: {result}")
#         except Exception as e:
#             print(f"Error retrievign results/status in callback: {e}")
#             print("Exiting!")
#             exit

#         # --- Output queue handling
#         # Put result in the output queue only if task succeeded and there's the output queue
#         if result_for_output is not None and output_queue is not None:
#             # Putting result in the output queue
#             try:
#                 output_queue.put(result_for_output)
#             except Exception as e:
#                 print(f"Error putting result to output queue: {e}")
#                 print("Exiting!")
#                 exit

#         # --- Input Queue Signaling ---
#         # Should crucially call task_done regardless of success/failure/outputting
#         # Because an item was originally taken from the input queue
#         try:
#             input_queue_ref.task_done()
#         except ValueError:
#             # We might get here if task_done is called more times than put
#             print(40 * "=")
#             print("WARNING: task_done() called inappropriately. This may indicate the queue logic doesn't match future handling")
#             print("Exiting!")
#             exit
#         except Exception as e:
#             print(f"Error calling task_done in callback: {e}")
#             print("Exiting!")
#             exit


#     return callback

# --- Dispatcher logic ---
def dispatcher_thread_runner(
    handler_name: str,
    input_queue: JoinableQueue,
    output_queue: Optional[JoinableQueue],
    processing_function: Callable, # The handler function
    worker_pool: concurrent.futures.Executor
):
    """
    Monitors input queue, submit tasks to pool, uses callback for results/task_done.
    Runs until SENTINEL is received
    """
    print(20* "=")
    print(f"{handler_name} Dispatcher {os.getpid()}/{Thread.native_id} Started.")
    active_task_count = 0
    
    while True:
        try: 
            # Blocking get - wait for items indefinitely until SENTINEL
            print("Removing item from buffer")
            item_package = input_queue.get()
            if item_package is SENTINEL: # Using None as SENTINEL
                print(f"[Dispacther {os.getpid()}] SENTINEL received, exiting.")
                break

            # Unpacking data and ID
            data, original_id = item_package
            print(f"[{handler_name} Dispatcher] Submitting ID: {original_id}")
            
            try:
                print("Submitting task to worker pool")
                future = worker_pool.submit(processing_function, item_package)
                active_task_count += 1

                # Store original_id with the future for the callback to 
                # retrieve
                future.original_id = original_id
                
                # Create and add the callback for the specific task
                # the_callback = callback_factory(output_queue, input_queue)
                future.add_done_callback(
                    lambda f: handle_result_and_signal(f, output_queue, input_queue)
                )
            except Exception as submit_err:
                print(f"[Dispatcher {os.getpid()}] Error submitting task {item}: {submit_err}")
                # We got the item but failed to submit.
                # We MUST call task_done here otherwise join() will hang.
                input_queue.task_done()
            
        except (EOFError, BrokenPipeError):
            print(f"!!! [Dispatcher {os.getpid()}] Queue communication error, exiting.")
            break # Exit loop on queue errors
        except Exception as e:
            print(f"!!! [Dispatcher {os.getpid()}] Unexpected error: {e}")
            # Depending on error, might need to break or continue
            time.sleep(1) # Avoid tight loop on continuous error

    # --- Sentinel received ---
    print(f"[{handler_name} Dispatcher] Waiting for tasks associated with this stage to complete...")
    # Propagate SENTINEL to the next stage if applicable
    if output_queue is not None:
        print(f"[{handler_name} Dispatcher] Propagating SENTINEL to next stage")
        output_queue.put(SENTINEL)

    print(f"[{handler_name} Dispatcher {os.getpid()}/{Thread.native_id}] Finished.")


# --- Orchestration Logic (Simplified) ---
def manage_pool_b():
    
    print(30 * "=")
    print("Creating buffers")
    handler_b_input_queue = JoinableQueue(maxsize=20) # Input buffer for Handler B
    handler_b_output_queue = JoinableQueue() # Output buffer for Handler B

    MAX_WORKERS_B = os.cpu_count() or 2
    total_tasks_to_add = 0
    num_batches = 2
    tasks_per_batch = 16

    print(f"Creating ProcessPoolExecutor with {MAX_WORKERS_B} workers")
    # Pool is created in the main process
    pool_b = concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS_B)

    # Start the dispatcher in a separate Thread
    # Pass queues, pool, and the callback factory
    print(f"Starting dispatcher thread...")
    dispatcher_thread = Thread(target=dispatcher, args=(
        handler_b_input_queue,
        handler_b_output_queue,
        pool_b,
        task_done_callback # Pass the factory itself
    ), daemon=True) # Daemon allows main program to exit even if this hangs
    dispatcher_thread.start()
    print(f"Dispatcher thread started!")

    # --- Simulating producer ---
    # Simulate Producer (Handler A) putting data
    print("Simulating handler A work")
    # 2 iterações com A adicionando trabalho no buffer
    for i in range(num_batches):
        print(f"Batch {i+1}/{num_batches}: Adding {tasks_per_batch} tasks")
        # Adding 4 tasks each time
        for j in range(tasks_per_batch):
            task_value = j % 5 + 1
            handler_b_input_queue.put(task_value) # Add items with varying intensity
            total_tasks_to_add += 1
        
    print(f"Total {total_tasks_to_add} tasks added to input queue")

    #  --- Signal end of input ---
    print("Adding SENTIEL (None) to input queue")
    handler_b_input_queue.put(None) # Signal to dispatcher to stop

    # --- Wait for completion ---
    print("Waiting for input queue to be fully processed (join())")
    handler_b_input_queue.join()
    print("Input queue joined. All tasks submitted and processed.")

    # --- Check Output Queue ---
    print("Checking output queue for results...")
    results_received = 0
    while results_received < total_tasks_to_add:
        try:
            # Use timeout to avoid blocking forever if something went wrong
            res = handler_b_output_queue.get(timeout=5.0)
            print(f"Result received: {res}")
            results_received += 1
        except queue.Empty:
            print("Output queue empty after waiting. Assuming complete (or error).")
            if results_received != total_tasks_to_add:
                print(f"!!! WARNING: Expected {total_tasks_to_add} results, got {results_received}")
            break # Stop checking output

    # --- Shutdown ---
    print("Waiting for dispatcher thread to finish...")
    dispatcher_thread.join(timeout=10) # Wait for dispatcher to exit cleanly
    if dispatcher_thread.is_alive():
        # Note: You can't forcefully terminate a thread like a process.
        # If it hangs, it usually indicates a bug in the dispatcher loop
        # or waiting logic (e.g., not handling sentinel correctly).
        print("!!! WARNING: Dispatcher thread did not exit cleanly")

    print("Shutting down worker pool...")
    # shutdown should ideally be called after we're sure no more tasks
    # will be submitted and after dispatcher is done.
    pool_b.shutdown(wait=True)
    print("Pool B finished.")

# --- Guard for multiprocessing ---
if __name__ == "__main__":
    # manage_pool_b()
    print("--- Starting Pipeline Simulation ---")
    print(f"CPU Count: {os.cpu_count()}, Workers per stage: {NUM_WORKERS}")
    print(f"Lists: {TOTAL_LISTS}, Size per list: {LIST_SIZE:,}")

    # Creating buffers for the handlers
    queue_a_in = JoinableQueue(maxsize = MAX_BUFFER_SIZE)
    queue_b_in = JoinableQueue(maxsize = MAX_BUFFER_SIZE)
    queue_c_in = JoinableQueue(maxsize = MAX_BUFFER_SIZE)

    # --- Creating worker pools ---
    # One pool per stage
    # Keeping pools in a list for easier shutdown of the pools
    pools = []
    pool_a = concurrent.futures.ProcessPoolExecutor(max_workers=NUM_WORKERS)
    pools.append(pool_a)
    pool_b = concurrent.futures.ProcessPoolExecutor(max_workers=NUM_WORKERS)
    pools.append(pool_b)
    pool_c = concurrent.futures.ProcessPoolExecutor(max_workers=NUM_WORKERS)
    pools.append(pool_c)

    # --- Start Dispatchers threads ---
    # List for dispatcher for easy management
    dispatchers = []
    print("Starting dispatcher threads...")
    # Dispatchers run in thread within the main process 
    # threads are even better, because the dispatchers are I/O bound.
    dispatcher_a = Thread(target=dispatcher_thread_runner, args=(
        'Dispatcher Handler A (Filter)', queue_a_in, queue_b_in, filter_worker, pool_a), daemon= True)
    dispatchers.append(dispatcher_a)
    dispatcher_b = Thread(target=dispatcher_thread_runner, args=(
        'Dispatcher Handler B (Sort)', queue_b_in, queue_c_in, sort_worker, pool_b), daemon= True)
    dispatchers.append(dispatcher_b)
    # Handler C's dispatcher hsa None for output_queue
    dispatcher_c = Thread(target=dispatcher_thread_runner, args=(
        'Dispatcher Handler C (Print)', queue_c_in, None, print_worker, pool_c), daemon= True)
    dispatchers.append(dispatcher_c)

    for d in dispatchers:
        d.start()
    print("Dispatcher threads started")

    # --- Simulate extractor ---
    start_time = time.time()
    print("[Extractor] Starting data generation and queuing... ")
    list_ids_generated = []
    for run in range(NUM_RUNS):
        print(f"[Extractor] Run {run + 1}/{NUM_RUNS}")

        for i in range(NUM_LISTS_PER_RUN):
            list_id = f'Run{run + 1}-List{i + 1}'
            list_ids_generated.append(list_id)
            print(f"[Extractor] generating list: {list_id}")

            # Generate data
            data = generate_random_numbers(MIN_VALUE, MAX_VALUE, LIST_SIZE)
            # Put in the first handler queue
            queue_a_in.put((data, list_id))
            print(f"[Extractor] List (ID: {list_id}) put into queue A.")
            time.sleep(0.05) # Optional simulate slower extraction

    print(f"[Extractor] Finished queuing {len(list_ids_generated)} lists.")

    # --- Signal End of Data ---
    print("[Extractor] Sending SENTINEL to Handler A...")
    queue_a_in.put(SENTINEL)

    # --- Wait for Pipeline Completion (by joining dispatcher threads) ---
    print("Waiting for dispatcher threads to complete...")
    for i, d in enumerate(dispatchers):
        handler_name = d.name or f'Dispatcher {i + 1}'
        d.join(timeout=5)

        if d.is_alive():
            print(f"!!! {handler_name} thread did not exit cleanly after SENTINEL propagation!")
        else:
            print(f"{handler_name} thread finished.")
    print("All dispatcher threads have joined.")

    # --- Shutdown Pools ---
    print("Shutting down worker pools...")
    for i, p in enumerate(pools):
        try:
            p.shutdown(wait=True)
            print(f"Pool {i + 1} shut down")
        except Exception as pool_shutdown_error:
            print(f"!!! Error shutting down pool {i + 1}: {pool_shutdown_error}")
    
    end_time = time.time()
    elapsed_time = end_time - start_time

    print("\n--- Pipeline Finished ---")
    print(f"Processed {len(list_ids_generated)} lists.")
    print(f"Total time: {elapsed_time:.2f} seconds")

    print("Main thread exiting.")