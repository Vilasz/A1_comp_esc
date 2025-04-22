import threading
from typing import Dict, Generic, Optional, Tuple
import logging

from dataframe import DataFrame, Generic, T
from using_threads import Queue


# Configuração de logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(threadName)10s] %(levelname)8s: %(message)s")
logger = logging.getLogger(__name__)

class Handler(Generic[T]):
    def __init__(
        self,
        in_queue: Queue[str, "DataFrame[T]"],
        out_queues: Dict[str, Queue[str, "DataFrame[T]"]],
    ) -> None:
        self.in_queue = in_queue
        self.out_queues = out_queues
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._running = threading.Event()
        self._running.set()

    # Subclasses devem sobrepor 
    def route(self, key: str, df: "DataFrame[T]") -> Optional[Tuple[str, "DataFrame[T]"]]:  
        raise NotImplementedError

    # Loop interno 
    def _run(self) -> None:  
        while self._running.is_set():
            try:
                key, df = self.in_queue.dequeue(timeout=0.5)
            except TimeoutError:
                continue
            try:
                dest = self.route(key, df)
                if dest is not None:
                    dkey, dval = dest
                    if dkey in self.out_queues:
                        self.out_queues[dkey].enqueue((dkey, dval))
                    else:
                        logger.warning("Queue %s not found", dkey)
            except Exception:  
                logger.exception("Handler error")

    # Controle 
    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._running.clear()
        self._thread.join()