# new/source/framework/hybrid_pool.py

from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
import threading
from typing import Callable, Any, Optional
import os

CPU_LIMIT = os.cpu_count() or 4

class HybridPool:
    """Envia até CPU_LIMIT tarefas para ProcessPool; excedente vai em threads."""

    def __init__(self, limit: int | None = None) -> None:
        # The 'limit' passed to __init__ is the number of *processes* to use.
        # The CPU_LIMIT global is a hard cap on this process limit.
        self.process_limit = min(limit or CPU_LIMIT, CPU_LIMIT)
        # For ThreadPoolExecutor, max_workers can be higher if tasks are I/O bound or mixed.
        # Let's set it based on the process_limit or allow more flexibility.
        # Original: self.limit * 2. Let's keep a similar idea but based on process_limit.
        self.thread_pool_max_workers = self.process_limit * 2 # Or some other reasonable number

        self._proc: Optional[ProcessPoolExecutor] = None
        self._th: Optional[ThreadPoolExecutor] = None
        self._lock = threading.Lock()
        self._running_proc = 0

    def _initialize_pools(self):
        """Initializes the actual pool executors. Called by __enter__."""
        if self._proc is None:
            self._proc = ProcessPoolExecutor(max_workers=self.process_limit)
        if self._th is None:
            self._th = ThreadPoolExecutor(max_workers=self.thread_pool_max_workers)

    def submit(self, fn: Callable[..., Any], *a: Any, **kw: Any) -> Future:
        # Ensure pools are initialized if submit is called outside a 'with' block (though not recommended)
        if self._proc is None or self._th is None:
            self._initialize_pools()
        
        # Assertion to satisfy mypy that pools are not None here
        assert self._proc is not None
        assert self._th is not None

        with self._lock:
            target_pool = self._proc if self._running_proc < self.process_limit else self._th
            if target_pool is self._proc:
                self._running_proc += 1
        
        fut = target_pool.submit(fn, *a, **kw)
        if target_pool is self._proc: # Only add done callback for process pool tasks
            fut.add_done_callback(lambda _: self._dec_proc_count())
        return fut

    def _dec_proc_count(self):
        with self._lock:
            self._running_proc -= 1

    def shutdown(self, wait: bool = True) -> None:
        """Shuts down both the process and thread pools."""
        if self._th:
            self._th.shutdown(wait=wait)
            self._th = None # Clear to allow re-initialization if needed
        if self._proc:
            self._proc.shutdown(wait=wait)
            self._proc = None # Clear
        # Reset running process count if shutting down
        with self._lock:
            self._running_proc = 0


    # --- Context Manager Protocol Implementation ---
    def __enter__(self) -> 'HybridPool':
        """Called when entering the 'with' block. Initializes pools."""
        self._initialize_pools()
        return self # Return the pool object itself to be used with 'as pool'

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Called when exiting the 'with' block. Ensures pools are shut down."""
        self.shutdown(wait=True) # 'wait=True' is typical for context managers
        # If you want to suppress exceptions, you can return True from here based on exc_type.
        # For now, let exceptions propagate by returning None implicitly.