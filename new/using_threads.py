import threading
from collections import defaultdict
from contextlib import contextmanager
from queue import Queue as _StdQueue
from typing import Dict, Generic, Iterator, Optional, Tuple
import logging

from dataframe import Generic, K, V, T



# Configuração de logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(threadName)10s] %(levelname)8s: %(message)s")
logger = logging.getLogger(__name__)


# Queue – fila thread‑safe com capacidade (Condition + deque opcional)

class Queue(Generic[K, V]):
    def __init__(self, capacity: int = 1000):
        self._capacity = capacity
        self._queue: _StdQueue[Tuple[K, V]] = _StdQueue()
        self._cv = threading.Condition()

    def enqueue(self, item: Tuple[K, V]) -> None:
        with self._cv:
            while self._queue.qsize() >= self._capacity:
                self._cv.wait()
            self._queue.put(item)
            self._cv.notify_all()

    def dequeue(self, timeout: Optional[float] = None) -> Tuple[K, V]:
        with self._cv:
            if not self._queue.qsize():
                waited = self._cv.wait(timeout)
                if not waited:
                    raise TimeoutError("Dequeue timeout")
            item = self._queue.get()
            self._cv.notify_all()
            return item

    # alias para manter compat. com C++ -----------------------------------
    enQueue = enqueue
    deQueue = dequeue


# MapMutex – locks por‑chave, devolvendo context manager

class MapMutex(Generic[T]):
    def __init__(self) -> None:
        self._locks: Dict[str, threading.Lock] = defaultdict(threading.Lock)
        self._data: Dict[str, T] = {}
        self._global = threading.Lock()

    @contextmanager
    def get_lock(self, key: str) -> Iterator[None]:
        with self._global:
            lock = self._locks[key]
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    # Métodos C++‑like -----------------------------------------------------
    def get(self, key: str, default: T | None = None) -> T:
        with self._global:
            return self._data.get(key, default)  # type: ignore[return‑value]

    def set(self, key: str, value: T) -> None:
        with self._global:
            self._data[key] = value


# ThreadWrapper – classe base para threads reutilizável

class ThreadWrapper(Generic[K, V], threading.Thread):
    """Abstração similar ao template C++.

    Caso a tarefa seja *CPU‑bound*, defina `use_mp=True` no construtor para
    usar *multiprocessing.Process*, evitando o GIL.  Isso é opcional; por
    padrão continuamos com threads por serem mais leves e suficientes para I/O.
    """

    def __init__(
        self,
        in_queue: Queue[K, V],
        out_queue: Optional[Queue[K, V]] = None,
        *,
        name: Optional[str] = None,
        use_mp: bool = False,
    ) -> None:
        self.in_queue = in_queue
        self.out_queue = out_queue
        self._running = threading.Event()
        self._running.set()
        self.use_mp = use_mp
        self._process = None
        threading.Thread.__init__(self, name=name, daemon=True)

    # Overridable ---------------------------------------------------------
    def handle(self, item: Tuple[K, V]) -> Optional[Tuple[K, V]]:  # noqa: D401
        raise NotImplementedError

    # Loop ----------------------------------------------------------------
    def run(self) -> None:  # noqa: D401
        logger.info("%s started (mp=%s)", self.name, self.use_mp)
        while self._running.is_set():
            try:
                item = self.in_queue.dequeue(timeout=0.5)
            except TimeoutError:
                continue
            try:
                result = self.handle(item)
                if result is not None and self.out_queue is not None:
                    self.out_queue.enqueue(result)
            except Exception:  # noqa: BLE001
                logger.exception("Error in thread %s", self.name)
        logger.info("%s stopped", self.name)

    # Control -------------------------------------------------------------
    def stop(self) -> None:
        self._running.clear()