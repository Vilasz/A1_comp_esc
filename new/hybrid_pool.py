from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
import threading
from typing import Callable, Any
import os

CPU_LIMIT = min(os.cpu_count() or 4, 12)

class HybridPool:
    """Envia até CPU_LIMIT tarefas para ProcessPool; excedente vai em threads."""

    def __init__(self, limit: int | None = None) -> None:
        self.limit = min(limit or CPU_LIMIT, CPU_LIMIT)
        self._proc = ProcessPoolExecutor(self.limit)
        self._th   = ThreadPoolExecutor(max_workers=self.limit * 2)
        self._lock = threading.Lock()
        self._running_proc = 0

    def submit(self, fn: Callable[..., Any], *a: Any, **kw: Any) -> Future:
        with self._lock:
            target = self._proc if self._running_proc < self.limit else self._th
            if target is self._proc:
                self._running_proc += 1
        fut = target.submit(fn, *a, **kw)
        if target is self._proc:
            fut.add_done_callback(lambda _: self._dec())
        return fut

    def _dec(self):
        with self._lock:
            self._running_proc -= 1

    def shutdown(self) -> None:
        self._proc.shutdown(wait=True)
        self._th.shutdown(wait=True)