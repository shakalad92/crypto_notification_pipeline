from __future__ import annotations

from collections import deque
from threading import Lock

from .idempotency import EventStateStore
from .observability import InMemoryLogSink, InMemoryMetrics


class InMemoryQueue:
    def __init__(self) -> None:
        self._ready: deque[str] = deque()
        self._scheduled_retry: deque[str] = deque()
        self._inflight: deque[str] = deque()
        self._dead_letter: list[str] = []
        self._metrics: InMemoryMetrics | None = None
        self._logs: InMemoryLogSink | None = None
        self._state_store: EventStateStore | None = None
        self._lock = Lock()

    @property
    def ready_count(self) -> int:
        with self._lock:
            return len(self._ready)

    @property
    def dead_letter_count(self) -> int:
        with self._lock:
            return len(self._dead_letter)

    def publish(self, event_id: str) -> None:
        with self._lock:
            self._ready.append(event_id)

    def bind_observability(
        self,
        *,
        metrics: InMemoryMetrics,
        logs: InMemoryLogSink,
        state_store: EventStateStore | None = None,
    ) -> None:
        with self._lock:
            self._metrics = metrics
            self._logs = logs
            self._state_store = state_store

    def lease(self) -> str | None:
        with self._lock:
            if not self._ready:
                return None
            event_id = self._ready.popleft()
            self._inflight.append(event_id)
            return event_id

    def ack(self, event_id: str) -> None:
        with self._lock:
            try:
                self._inflight.remove(event_id)
            except ValueError:
                return

    def schedule_retry(self, event_id: str) -> None:
        with self._lock:
            self._scheduled_retry.append(event_id)

    def release_scheduled_retries(self) -> list[str]:
        released: list[str] = []
        with self._lock:
            while self._scheduled_retry:
                event_id = self._scheduled_retry.popleft()
                self._ready.append(event_id)
                released.append(event_id)
        return released

    def redeliver_unacked(self) -> list[str]:
        with self._lock:
            redelivered = list(self._inflight)
            self._inflight.clear()
            self._ready.extend(redelivered)
            metrics = self._metrics
            logs = self._logs
            state_store = self._state_store
        if metrics is not None and redelivered:
            metrics.increment("queue.redelivered", len(redelivered))
        if logs is not None:
            for event_id in redelivered:
                entry: dict[str, object] = {"event_id": event_id}
                if state_store is not None:
                    event = state_store.get(event_id)
                    entry.update(
                        {
                            "idempotency_key": event.idempotency_key,
                            "correlation_id": event.correlation_id,
                            "trace_id": event.trace_id,
                            "attempt": event.attempt,
                            "processing_state": event.processing_state.value,
                        }
                    )
                logs.record("queue.redelivered", **entry)
        return redelivered

    def inject_duplicate(self, event_id: str) -> None:
        with self._lock:
            self._ready.append(event_id)

    def dead_letter(self, event_id: str) -> None:
        with self._lock:
            self._dead_letter.append(event_id)
