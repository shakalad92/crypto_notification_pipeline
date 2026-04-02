from __future__ import annotations

from dataclasses import dataclass
from threading import Event, Lock

from .contracts import NotificationEvent
from .observability import InMemoryLogSink, InMemoryMetrics


class RetryableNotificationError(Exception):
    pass


@dataclass(frozen=True)
class NotificationResult:
    status: str


class InMemoryTransport:
    def __init__(self) -> None:
        self.deliveries: list[dict[str, str]] = []
        self._lock = Lock()

    def send(self, *, event: NotificationEvent) -> None:
        payload = {
            "event_id": event.event_id,
            "channel": event.request.notification_channel.value,
            "target": event.request.notification_target,
            "transaction_id": event.request.transaction_id,
        }
        with self._lock:
            self.deliveries.append(payload)


class FlakyTransport(InMemoryTransport):
    def __init__(self, *, failures_before_success: int) -> None:
        super().__init__()
        self._failures_before_success = failures_before_success
        self._attempts = 0

    def send(self, *, event: NotificationEvent) -> None:
        self._attempts += 1
        if self._attempts <= self._failures_before_success:
            raise RetryableNotificationError("transient notifier failure")
        super().send(event=event)


class AlwaysFailTransport:
    def send(self, *, event: NotificationEvent) -> None:
        raise RetryableNotificationError("permanent downstream outage")


class BlockingTransport(InMemoryTransport):
    def __init__(self) -> None:
        super().__init__()
        self.entered = Event()
        self.release = Event()

    def send(self, *, event: NotificationEvent) -> None:
        self.entered.set()
        self.release.wait(timeout=5)
        super().send(event=event)


class DeduplicatingNotifier:
    def __init__(self, transport: InMemoryTransport | FlakyTransport | AlwaysFailTransport | BlockingTransport) -> None:
        self._transport = transport
        self._delivered_event_ids: set[str] = set()
        self._lock = Lock()

    def notify(self, event: NotificationEvent, *, metrics: InMemoryMetrics, logs: InMemoryLogSink) -> NotificationResult:
        with self._lock:
            if event.event_id in self._delivered_event_ids:
                metrics.increment("notification.deduped")
                logs.record(
                    "notifier.deduped",
                    event_id=event.event_id,
                    idempotency_key=event.idempotency_key,
                    correlation_id=event.correlation_id,
                    trace_id=event.trace_id,
                    attempt=event.attempt,
                )
                return NotificationResult(status="deduped")

            # Keep check+send+mark atomic to prevent duplicate downstream sends
            # when workers race on replayed deliveries.
            self._transport.send(event=event)
            self._delivered_event_ids.add(event.event_id)

        metrics.increment("notification.sent")
        logs.record(
            "notifier.sent",
            event_id=event.event_id,
            idempotency_key=event.idempotency_key,
            correlation_id=event.correlation_id,
            trace_id=event.trace_id,
            attempt=event.attempt,
        )
        return NotificationResult(status="sent")


class NonIdempotentNotifier:
    def __init__(self, transport: InMemoryTransport | FlakyTransport | AlwaysFailTransport | BlockingTransport) -> None:
        self._transport = transport

    def notify(self, event: NotificationEvent, *, metrics: InMemoryMetrics, logs: InMemoryLogSink) -> NotificationResult:
        self._transport.send(event=event)
        metrics.increment("notification.sent")
        logs.record(
            "notifier.sent",
            event_id=event.event_id,
            idempotency_key=event.idempotency_key,
            correlation_id=event.correlation_id,
            trace_id=event.trace_id,
            attempt=event.attempt,
        )
        return NotificationResult(status="sent")
