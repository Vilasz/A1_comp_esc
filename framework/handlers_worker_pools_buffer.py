import concurrent.futures
from multiprocessing import JoinableQueue, cpu_count, Process
from threading import Thread

import os
import time
import queue
import random
import uuid

from typing import List

from tqdm import tqdm

# Module setup

MIN_VALUE = 1
MAX_VALUE = 1_000_000
LIST_SIZE = 1_000_000
NUM_LISTS_PER_RUN = 3
NUM_RUNS = 3
TOTAL_LISTS = NUM_LISTS_PER_RUN * NUM_RUNS

NUM_WORKERS = os.cpu_count or 2

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

def task_done_callback(output_queue, input_queue_ref):
    def callback(future):
        # Keeping track if we should put some somethign
        result_for_output = None
        try:
            if future.cancelled():
                print("Task was cancelled")
            elif future.exception():
                print(f"Task failed with exception: {future.exception()}")
            else:
                result = future.result()
                result_for_output = result
                print(f"Task completed successfully with result: {result}")
        except Exception as e:
            print(f"Error retrievign results/status in callback: {e}")
            print("Exiting!")
            exit

        # --- Output queue handling
        # Put result in the output queue only if task succeeded and there's the output queue
        if result_for_output is not None and output_queue is not None:
            # Putting result in the output queue
            try:
                output_queue.put(result_for_output)
            except Exception as e:
                print(f"Error putting result to output queue: {e}")
                print("Exiting!")
                exit

        # --- Input Queue Signaling ---
        # Should crucially call task_done regardless of success/failure/outputting
        # Because an item was originally taken from the input queue
        try:
            input_queue_ref.task_done()
        except ValueError:
            # We might get here if task_done is called more times than put
            print(40 * "=")
            print("WARNING: task_done() called inappropriately. This may indicate the queue logic doesn't match future handling")
            print("Exiting!")
            exit
        except Exception as e:
            print(f"Error calling task_done in callback: {e}")
            print("Exiting!")
            exit


    return callback

# --- Dispatcher logic ---
def dispatcher(input_queue, output_queue, pool, callback_factory):
    """
    Continuously gets tasks and submits them with callbacks
    """
    print(20* "=")
    print(f"[Dispatcher {os.getpid()}] Started.")
    while True:
        try: 
            # Blocking get - wait for items indefinitely until SENTINEL
            print("Removing item from buffer")
            item = input_queue.get()
            if item is None: # Using None as SENTINEL
                print(f"[Dispacther {os.getpid()}] SENTINEL received, exiting.")
                break

            try:
                print("Submitting task to worker pool")
                future = pool.submit(cpu_bound_handler_task, item)
                # Create and add the callback for the specific task
                the_callback = callback_factory(output_queue, input_queue)
                future.add_done_callback(
                    the_callback
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

    print(f"[Dispatcher {os.getpid()}] Stopped.")
    

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
    manage_pool_b()