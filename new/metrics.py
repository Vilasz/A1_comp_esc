import threading
from collections import defaultdict

class _Counter:
    def __init__(self) -> None:
        self.v = 0
        self._lock = threading.Lock()

    def inc(self, n: int = 1) -> None:
        with self._lock:
            self.v += n

    def value(self) -> int:
        return self.v


class _Histogram:
    def __init__(self, buckets: list[float]) -> None:
        self.buckets = sorted(buckets)
        self.counts = [0] * (len(buckets) + 1)
        self._lock = threading.Lock()

    def observe(self, x: float) -> None:
        with self._lock:
            for i, b in enumerate(self.buckets):
                if x <= b:
                    self.counts[i] += 1
                    break
            else:
                self.counts[-1] += 1

    def snapshot(self) -> list[int]:
        with self._lock:
            return self.counts.copy()


class Metrics:
    def __init__(self) -> None:
        self.counters: dict[str, _Counter] = defaultdict(_Counter)
        self.hists: dict[str, _Histogram] = {}

    def counter(self, name: str) -> _Counter:
        return self.counters[name]

    def histogram(self, name: str, buckets: list[float] | None = None) -> _Histogram:
        if name not in self.hists:
            self.hists[name] = _Histogram(buckets or [0.1, 0.5, 1, 2, 5])
        return self.hists[name]

    def report(self) -> str:
        lines: list[str] = []
        for n, c in self.counters.items():
            lines.append(f"{n} {c.value()}")
        for n, h in self.hists.items():
            for v, b in zip(h.snapshot(), h.buckets + ['+Inf']):
                lines.append(f'{n}_bucket{{le="{b}"}} {v}')
        return "\n".join(lines)


METRICS = Metrics()