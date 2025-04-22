import threading
import time

from new.source.framework.using_threads import Queue

class Trigger:
    def __init__(self, out_queue: Queue[str, str], first: str, second: str) -> None:
        self.out_queue = out_queue
        self.first = first
        self.second = second
        self._running = threading.Event()
        self._running.set()
        self._thread = threading.Thread(target=self.run, daemon=True)

    # API 
    def run(self) -> None:  
        raise NotImplementedError

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._running.clear()
        self._thread.join()

    # Auxiliar 
    def add_to_queue(self) -> None:
        self.out_queue.enqueue((self.first, self.second))


class TimeTrigger(Trigger):
    def __init__(self, out_queue: Queue[str, str], interval_ms: int, first: str, second: str) -> None:
        super().__init__(out_queue, first, second)
        self.interval = interval_ms / 1000.0

    def run(self) -> None:  
        while self._running.is_set():
            time.sleep(self.interval)
            self.add_to_queue()


class RequestTrigger(Trigger):
    def run(self) -> None:  
        if self._running.is_set():
            self.add_to_queue()
            self._running.clear()  # executa só uma vez