from __future__ import annotations

from datetime import datetime, timezone

from .contracts import ProcessingState
from .idempotency import EventStateStore
from .notifiers import DeduplicatingNotifier, NonIdempotentNotifier, RetryableNotificationError
from .observability import InMemoryLogSink, InMemoryMetrics
from .queue import InMemoryQueue


class NotificationWorker:
    def __init__(
        self,
        *,
        queue: InMemoryQueue,
        state_store: EventStateStore,
        notifier: DeduplicatingNotifier | NonIdempotentNotifier,
        metrics: InMemoryMetrics,
        logs: InMemoryLogSink,
        max_attempts: int = 3,
    ) -> None:
        self._queue = queue
        self._state_store = state_store
        self._notifier = notifier
        self._metrics = metrics
        self._logs = logs
        self._max_attempts = max_attempts
        self._queue.bind_observability(metrics=metrics, logs=logs, state_store=state_store)

    def process_next(self, *, simulate_ack_loss: bool = False) -> str:
        event_id = self._queue.lease()
        if event_id is None:
            return "empty"

        event = self._state_store.get(event_id)

        if event.processing_state is ProcessingState.SENT:
            self._logs.record(
                "worker.started",
                event_id=event.event_id,
                idempotency_key=event.idempotency_key,
                correlation_id=event.correlation_id,
                trace_id=event.trace_id,
                attempt=event.attempt,
                processing_state=event.processing_state.value,
            )
            self._notifier.notify(event, metrics=self._metrics, logs=self._logs)
            if not simulate_ack_loss:
                self._queue.ack(event_id)
            return "sent"

        next_attempt = event.attempt + 1
        event = self._state_store.transition(
            event_id,
            new_state=ProcessingState.PROCESSING,
            attempt=next_attempt,
        )

        self._logs.record(
            "worker.started",
            event_id=event.event_id,
            idempotency_key=event.idempotency_key,
            correlation_id=event.correlation_id,
            trace_id=event.trace_id,
            attempt=event.attempt,
            processing_state=event.processing_state.value,
        )

        try:
            self._notifier.notify(event, metrics=self._metrics, logs=self._logs)
        except RetryableNotificationError:
            if event.attempt < self._max_attempts:
                event = self._state_store.transition(
                    event_id,
                    new_state=ProcessingState.RETRY_PENDING,
                )
                self._queue.schedule_retry(event_id)
                self._queue.ack(event_id)
                self._metrics.increment("worker.retried")
                self._metrics.increment("queue.retry_scheduled")
                self._logs.record(
                    "worker.retry_scheduled",
                    event_id=event.event_id,
                    idempotency_key=event.idempotency_key,
                    correlation_id=event.correlation_id,
                    trace_id=event.trace_id,
                    attempt=event.attempt,
                    processing_state=event.processing_state.value,
                )
                return "retry_pending"

            event = self._state_store.transition(
                event_id,
                new_state=ProcessingState.FAILED,
            )
            self._queue.dead_letter(event_id)
            self._queue.ack(event_id)
            self._metrics.increment("worker.failed")
            self._metrics.increment("queue.dead_lettered")
            self._logs.record(
                "worker.failed",
                event_id=event.event_id,
                idempotency_key=event.idempotency_key,
                correlation_id=event.correlation_id,
                trace_id=event.trace_id,
                attempt=event.attempt,
                processing_state=event.processing_state.value,
            )
            return "failed"

        event = self._state_store.transition(
            event_id,
            new_state=ProcessingState.SENT,
        )
        self._metrics.increment("worker.processed")
        self._logs.record(
            "worker.completed",
            event_id=event.event_id,
            idempotency_key=event.idempotency_key,
            correlation_id=event.correlation_id,
            trace_id=event.trace_id,
            attempt=event.attempt,
            processing_state=event.processing_state.value,
        )

        if simulate_ack_loss:
            self._metrics.increment("worker.ack_lost_simulated")
            self._logs.record(
                "worker.ack_lost_simulated",
                event_id=event.event_id,
                idempotency_key=event.idempotency_key,
                correlation_id=event.correlation_id,
                trace_id=event.trace_id,
                attempt=event.attempt,
                processing_state=event.processing_state.value,
            )
            return "sent"

        self._queue.ack(event_id)
        return "sent"

    def release_scheduled_retries(self) -> int:
        released = self._queue.release_scheduled_retries()
        for event_id in released:
            event = self._state_store.transition(
                event_id,
                new_state=ProcessingState.QUEUED,
                queued_at=datetime.now(timezone.utc),
            )
            self._logs.record(
                "queue.retry_released",
                event_id=event.event_id,
                idempotency_key=event.idempotency_key,
                correlation_id=event.correlation_id,
                trace_id=event.trace_id,
                attempt=event.attempt,
                processing_state=event.processing_state.value,
            )

        if released:
            self._metrics.increment("queue.retry_released", len(released))

        return len(released)
