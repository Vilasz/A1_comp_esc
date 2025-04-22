from typing import Callable, Any, Sequence
from collections import defaultdict, deque
import threading



# TASK SYSTEM + SCHEDULER Round‑Robin

TaskFn = Callable[[], Any]
_TASKS: dict[str, TaskFn] = {}
_DEPS: dict[str, set[str]] = defaultdict(set)

def task(*, depends: Sequence[str] | None = None):
    """Decorador que registra a função como task do DAG."""
    def deco(fn: TaskFn) -> TaskFn:
        name = fn.__name__
        _TASKS[name] = fn
        for d in depends or []:
            _DEPS[name].add(d)
        return fn
    return deco


class Scheduler:
    """Pega tasks sem dependências pendentes em Round‑Robin."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ready: deque[str] = deque(n for n, d in _DEPS.items() if not d)

    def next(self) -> str | None:
        with self._lock:
            if not self._ready:
                return None
            self._ready.rotate(-1)
            return self._ready[0]

    def done(self, name: str) -> None:
        with self._lock:
            for n, deps in _DEPS.items():
                deps.discard(name)
                if not deps and n not in self._ready:
                    self._ready.append(n)