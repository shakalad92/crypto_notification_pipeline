from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from threading import Lock


class InMemoryMetrics:
    def __init__(self) -> None:
        self._counters: dict[str, int] = defaultdict(int)
        self._lock = Lock()

    def increment(self, name: str, value: int = 1) -> None:
        with self._lock:
            self._counters[name] += value

    def get(self, name: str) -> int:
        with self._lock:
            return self._counters.get(name, 0)


class InMemoryLogSink:
    def __init__(self) -> None:
        self._entries: list[dict[str, object]] = []
        self._lock = Lock()

    def record(self, action: str, **fields: object) -> None:
        entry = {
            "action": action,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **fields,
        }
        with self._lock:
            self._entries.append(entry)

    def all(self) -> list[dict[str, object]]:
        with self._lock:
            return list(self._entries)

    def filter_by_event(self, event_id: str) -> list[dict[str, object]]:
        with self._lock:
            return [entry for entry in self._entries if entry.get("event_id") == event_id]
