import concurrent.futures
from multiprocessing import JoinableQueue, cpu_count, Process
from threading import Thread
import threading
import traceback

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
LIST_SIZE = 300_00
NUM_LISTS_PER_RUN = 10
NUM_RUNS = 3
TOTAL_LISTS = NUM_LISTS_PER_RUN * NUM_RUNS
MAX_BUFFER_SIZE = 10

NUM_WORKERS = max((cpu_count() or 4) // 2, 1)

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
        # original_id = future.original_id
        if hasattr(future, 'original_id'):
            original_id = future.original_id
        else:
            print(f"!!! Callback Warning: Future missing original_id attribute!")

        # No output unless the task succeeds
        result_for_output = None

        if future.cancelled():
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
        # Because an item was originally taken from the input queue
        try:
            input_queue_ref.task_done()
            
        except ValueError:
            print(40 * "=")
            print(f"WARNING: task_done() called inappropriately (ID: {original_id}). This may indicate the queue logic doesn't match future handling")
        except Exception as td_error:
            print(f"Error calling task_done in callback (ID: {original_id}) {td_error}")

# --- Dispatcher logic ---
def dispatcher_thread_runner(
    handler_name: str,
    input_queue: JoinableQueue,
    output_queue: Optional[JoinableQueue],
    processing_function: Callable, # The handler function
    worker_pool: concurrent.futures.Executor,
):
    """
    Monitors input queue, submit tasks to pool, uses callback for results/task_done.
    Runs until SENTINEL is received
    """
    current_thread_id = threading.get_native_id()
    print(20* "=")
    print(f"{handler_name} Dispatcher {os.getpid()}/{current_thread_id} Started.")
    
    sentinel_received = False

    while True:
        try:
            # Using blocking get with timeout
            item_package = input_queue.get(timeout=1.0)
            
            # --- Process the retrieved item ---
            if item_package is SENTINEL:
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
                    lambda f: handle_result_and_signal(f, output_queue, input_queue)
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
            if 'item_package' in locals() and item_package is not SENTINEL:
                input_queue.task_done()
            
            time.sleep(0.5) # Avoid tight loop on persistent error

    # Only propagating SENTINEL after ALL items have been processed
    if output_queue is not None:
        print(f"[{handler_name} Dispatcher] Propagating SENTINEL to next stage")
        output_queue.put(SENTINEL)

    print(f"[{handler_name} Dispatcher {os.getpid()}/{current_thread_id}] Finished.")

# --- Guard for multiprocessing ---
if __name__ == "__main__":
    print("--- Starting Pipeline Simulation ---")
    print(f"CPU Count: {os.cpu_count()}, Workers per stage: {NUM_WORKERS}")
    print(f"Lists: {TOTAL_LISTS}, Size per list: {LIST_SIZE:,}")

    # Creating buffers for the handlers
    print("Creating joinable queues")
    queue_a_in = JoinableQueue(maxsize = MAX_BUFFER_SIZE)
    queue_b_in = JoinableQueue(maxsize = MAX_BUFFER_SIZE)
    queue_c_in = JoinableQueue(maxsize = MAX_BUFFER_SIZE)
    print("Queues created")

    # --- Creating worker pools ---
    # One pool per stage
    # Keeping pools in a list for easier shutdown of the pools
    print("Created ProcessPoolExecutors...")
    pools = []
    # pool_everything = concurrent.futures.ProcessPoolExecutor(max_workers=8)
    # pools.append(pool_everything)
    
    # This commented section creates dedicated worker pools
    pool_a = concurrent.futures.ProcessPoolExecutor(max_workers=NUM_WORKERS)
    pools.append(pool_a)
    pool_b = concurrent.futures.ProcessPoolExecutor(max_workers=NUM_WORKERS)
    pools.append(pool_b)
    pool_c = concurrent.futures.ProcessPoolExecutor(max_workers=NUM_WORKERS)
    pools.append(pool_c)
    
    print("Pools created")

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
            # time.sleep(0.05) # Optional simulate slower extraction

    print(f"[Extractor] Finished queuing {len(list_ids_generated)} lists.")

    # --- Signal End of Data ---
    print("[Extractor] Sending SENTINEL to Handler A...")
    queue_a_in.put(SENTINEL)

    # --- Wait for Pipeline Stages using Queue Joins ---
    # This is now the primary synchronization mechanism.
    print("Waiting for Handler A processing (queue_a_in.join())...")
    queue_a_in.join() # Waits for N data task_done + 1 sentinel task_done
    print("Handler A finished processing all items.")

    print("Waiting for Handler B processing (queue_b_in.join())...")
    queue_b_in.join() # Waits for N data task_done + 1 sentinel task_done
    print("Handler B finished processing all items.")

    print("Waiting for Handler C processing (queue_c_in.join())...")
    queue_c_in.join() # Waits for N data task_done + 1 sentinel task_done
    print("Handler C finished processing all items.")

    print("All queue stages joined. Proceeding to shutdown.")

    print("Time count stopped")
    end_time = time.time()
    elapsed_time = end_time - start_time

    # --- Wait for Pipeline Completion (by joining dispatcher threads) ---
    print("*============================================================*")
    print("|Shutting down pipeline components. This may take a moment...|")
    print("*============================================================*")
    for i, d in enumerate(dispatchers):
        handler_name = d.name or f'Dispatcher {i + 1}'
        # d.join(timeout=5)
        d.join()

        if d.is_alive():
            print(f"!!! {handler_name} thread did not exit cleanly after SENTINEL propagation!")
        else:
            print(f"{handler_name} thread finished.")
    print("All dispatcher threads have joined.")

    # --- Shutdown Pools ---
    print("Shutting down worker pools...")
    for i, p in enumerate(pools):
        try:
            print(f"calling shutdown for pool {i + 1}")
            p.shutdown(wait=True)
            print(f"Pool {i + 1} shutdown")
        except Exception as pool_shutdown_error:
            print(f"!!! Error shutting down pool {i + 1}: {pool_shutdown_error}")

    print("\n--- Pipeline Finished ---")
    print(f"Processed {len(list_ids_generated)} lists.")
    print(f"Total time: {elapsed_time:.2f} seconds")

    print("Main thread exiting.")