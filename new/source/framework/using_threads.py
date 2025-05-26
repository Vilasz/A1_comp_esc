# new/source/framework/common/threading_utils.py
from __future__ import annotations

import logging
import queue # Standard library queue
import threading
from typing import TypeVar, Generic, Tuple, Optional, Callable, Any

logger = logging.getLogger(__name__)
from new.source.utils.dataframe import Generic, V
from collections.abc import Hashable

K = TypeVar("K", bound=Hashable)
QueueItem = Tuple[K, V] # Standardizing on (Key, Value) tuples for queue items

class ThreadWrapper(threading.Thread, Generic[K, V]):
    """
    A base class for worker threads that process items from an input queue
    and optionally put results onto an output queue.
    Uses standard library `queue.Queue`.
    """
    def __init__(
        self,
        in_queue: queue.Queue[QueueItem[K, V]],
        out_queue: Optional[queue.Queue[QueueItem[K, V]]] = None,
        name: Optional[str] = None,
        # Removed use_mp as this wrapper is strictly for threading.
        # HybridPool or ProcessPoolExecutor should be used for multiprocessing.
    ):
        super().__init__(name=name, daemon=True)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self._running = threading.Event()
        self._running.set()
        self._processed_items_count = 0

    def handle_item(self, key: K, value: V) -> Optional[QueueItem[K, V]]:
        """
        Process a single item (key, value).
        Subclasses MUST override this method.
        Should return a (key, value) tuple for the output queue, or None.
        """
        raise NotImplementedError("Subclasses must implement handle_item.")

    def run(self) -> None:
        logger.info("%s started.", self.name)
        while self._running.is_set():
            try:
                item_from_queue = self.in_queue.get(block=True, timeout=0.5)

                if item_from_queue is None:
                    logger.info(f"{self.name}: Sentinel received. Shutting down.")
                    # Propagate sentinel if there's an output queue
                    if self.out_queue is not None:
                        try:
                            self.out_queue.put(None, block=False)
                        except queue.Full:
                            logger.warning(f"{self.name}: Output queue full while propagating SENTINEL.")
                    if hasattr(self.in_queue, 'task_done'): # For JoinableQueue
                        self.in_queue.task_done()
                    break # Exit the run loop

                # Get item with a timeout to allow checking self._running periodically
                key, value = item_from_queue
                self._processed_items_count +=1
            except queue.Empty:
                # Timeout occurred, loop again to check self._running
                continue
            except Exception as e: # Should ideally not happen if queue stores valid items
                logger.error("%s: Error getting item from in_queue: %s", self.name, e)
                continue # Or break, depending on desired error handling

            try:
                result_item = self.handle_item(key, value)
                if result_item is not None and self.out_queue is not None:
                    self.out_queue.put(result_item)
            except Exception: # Catch exceptions from handle_item
                logger.exception("%s: Error processing item (%s, %s)", self.name, key, str(value)[:50])
            finally:
                # Crucial for queue.join() to work if the input queue is a JoinableQueue
                # or if external logic relies on task_done.
                if hasattr(self.in_queue, 'task_done'):
                     self.in_queue.task_done()
        
        logger.info("%s stopped. Processed %d items.", self.name, self._processed_items_count)

    def stop(self) -> None:
        logger.info("Stopping %s...", self.name)
        self._running.clear()