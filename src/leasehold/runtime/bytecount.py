"""Bytes on the wire through a proxy, counted per task for the account's usage."""

from __future__ import annotations

import threading


class ByteCount:
    """Thread-safe running total."""

    def __init__(self) -> None:
        self._n = 0
        self._lock = threading.Lock()

    def add(self, n: int) -> None:
        with self._lock:
            self._n += max(0, n)

    @property
    def total(self) -> int:
        return self._n
