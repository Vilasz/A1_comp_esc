import concurrent.futures
from multiprocessing import Queue, Manager

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

def manage_pool_b():
    # --- Orchestration Logic (Simplified) ---
    
    print(30 * "=")
    print("Creating buffers")
    handler_b_input_queue = queue.Queue(maxsize=20) # Input buffer for Handler B
    handler_b_output_queue = queue.Queue() # Output buffer for Handler B

    MAX_WORKERS_B = 8 # Max potential workers for Handler B pool

    # Using ProcessPoolExecutor for CPU-bound task
    # Note: Standard executor resizing isn't direct.
    # Control happens via managing submitted tasks relative to max_workers.
    print("Creating worker pool")
    pool_b = concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS_B)
    active_tasks = []
    
    # --- Dispatcher / Pool Management Logic (Simplified Example) ---
    def check_and_dispatch():
        print(30 * "=")
        print("Running pool management logic")
        nonlocal active_tasks
        
        # List of tasks still running
        remaining_tasks = []
        # Of all the active tasks
        print("Checking finished and running tasks")
        for f in active_tasks:
            # Indicate if it has been cancelled
            if f.done() and f.cancelled():
                print("Task was cancelled")
                # Mark it as done in the input buffer
                handler_b_input_queue.task_done()
            
            # If complented and not cancelled
            if f.done() and not f.cancelled():
                # Try to get its results
                try:
                    result = f.result()
                    # And put the result in the output buffer
                    handler_b_output_queue.put(result)
                    print(f"Task completed successfully with result: {result}")
                    # Mark it as done in the input buffer
                    handler_b_input_queue.task_done()
                except Exception as e:
                    # Handle potential exception indicating that the task because
                    # of some error
                    print(f"Task failed with exception: {e}")
                except ValueError:
                    print(40 * "=")
                    print("WARNING: task_done() called inappropriately. This may indicate the queue logic doesn't match future handling")
                    pass
            else:
                # Keeping the task in the list for next check
                remaining_tasks.append(f)
                
        # Then, update the list of active task with only the tasks stil remaining
        # and not cancelled or failed
        print("Updating tasks queue")
        active_tasks = remaining_tasks

        # Now, submitting new tasks to be made with dynamic adjustmente logic
        # (Simplified: just load up to max_workers)
        # A real implementation would need more complex logic for scaling down
        # and reacting to downstream backpressure.
        print("calculating workers available")
        current_load = len(active_tasks)
        can_submit = MAX_WORKERS_B - current_load
        print(f"Queue size: {handler_b_input_queue.qsize()}, Active tasks: {current_load}, Can submit: {can_submit}")

        # For each available worker
        print("Assigning tasks for available workers")
        for _ in range(can_submit):
            # Let's try getting it a task to do
            try:
                # Get data if available without blocking indefinitely
                print("Removing item from buffer")
                item = handler_b_input_queue.get_nowait() # If there isn't a task, raise empty queue exception and break loop
                print("Submitting task to worker pool")
                future = pool_b.submit(cpu_bound_handler_task, item)
                active_tasks.append(future)
            except queue.Empty:
                print("Input buffer is empty")
                break # No more items for now

    # 4 iterações com A adicionando trabalho no buffer
    for i in range(2):
        print(f"{i} turn of computations")
        # Simulate Producer (Handler A) putting data
        print("Simulating handler A work")
        # Adding 4 tasks each time
        for j in range(16):
            handler_b_input_queue.put(j % 5 + 1) # Add items with varying intensity
        
        print("Running handler B work")
        # while not producer_finished or not handler_b_input_queue.empty() or active_tasks:
        while not handler_b_input_queue.empty() or active_tasks:
            
            check_and_dispatch()
            # Check for results (example)
            try:
                res = handler_b_output_queue.get_nowait()
                print(f"Result received: {res}")
            except queue.Empty:
                pass
            time.sleep(0.1) # Check periodically
            # Add condition to set producer_finished = True eventually

    pool_b.shutdown(wait=True)
    print("Pool B finished.")

if __name__ == "__main__":
    manage_pool_b()