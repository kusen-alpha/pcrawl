from __future__ import annotations

import threading
from typing import Any


class Stats:
    """Small thread-safe counter store.

    In process mode each worker owns a separate ``Stats`` instance; aggregate
    counters through your own monitoring sink when you need cluster-wide totals.
    """

    def __init__(self) -> None:
        self._values: dict[str, int | float] = {}
        self._lock = threading.Lock()

    def inc(self, key: str, value: int | float = 1) -> None:
        with self._lock:
            self._values[key] = self._values.get(key, 0) + value

    def set(self, key: str, value: int | float) -> None:
        with self._lock:
            self._values[key] = value

    def get(self, key: str, default: Any = 0) -> Any:
        with self._lock:
            return self._values.get(key, default)

    def snapshot(self) -> dict[str, int | float]:
        with self._lock:
            return dict(self._values)

    def merge(self, values: dict[str, int | float]) -> None:
        with self._lock:
            for key, value in values.items():
                self._values[key] = self._values.get(key, 0) + value
