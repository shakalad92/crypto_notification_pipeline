from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from uuid import uuid4

from .contracts import (
    PIPELINE_STATE_TRANSITIONS,
    NotificationEvent,
    ProcessingState,
    SubmitTransactionRequest,
)


@dataclass(frozen=True)
class ReservationResult:
    outcome: str
    event: NotificationEvent


@dataclass(frozen=True)
class _IdempotencyRecord:
    event_id: str
    payload_hash: str


class EventStateStore:
    def __init__(self) -> None:
        self._lock = Lock()
        self._by_event_id: dict[str, NotificationEvent] = {}
        self._by_idempotency_key: dict[str, _IdempotencyRecord] = {}

    def create_or_get(
        self,
        *,
        idempotency_key: str,
        payload_hash: str,
        request: SubmitTransactionRequest,
    ) -> ReservationResult:
        with self._lock:
            existing = self._by_idempotency_key.get(idempotency_key)
            if existing is not None:
                event = self._by_event_id[existing.event_id]
                if existing.payload_hash != payload_hash:
                    return ReservationResult(outcome="conflict", event=event)
                return ReservationResult(outcome="duplicate", event=event)

            now = datetime.now(timezone.utc)
            event = NotificationEvent(
                event_id=f"evt-{uuid4().hex}",
                idempotency_key=idempotency_key,
                payload_hash=payload_hash,
                correlation_id=f"corr-{uuid4().hex}",
                trace_id=f"trace-{uuid4().hex}",
                attempt=0,
                processing_state=ProcessingState.QUEUED,
                created_at=now,
                queued_at=now,
                request=request,
            )
            self._by_idempotency_key[idempotency_key] = _IdempotencyRecord(
                event_id=event.event_id,
                payload_hash=payload_hash,
            )
            self._by_event_id[event.event_id] = event
            return ReservationResult(outcome="accepted", event=event)

    def get(self, event_id: str) -> NotificationEvent:
        with self._lock:
            return self._by_event_id[event_id]

    def transition(
        self,
        event_id: str,
        *,
        new_state: ProcessingState,
        attempt: int | None = None,
        queued_at: datetime | None = None,
        allow_same_state: bool = False,
    ) -> NotificationEvent:
        with self._lock:
            current = self._by_event_id[event_id]
            if current.processing_state != new_state:
                allowed = PIPELINE_STATE_TRANSITIONS[current.processing_state]
                if new_state not in allowed:
                    raise ValueError(
                        f"Invalid state transition: {current.processing_state.value} -> {new_state.value}"
                    )
            elif not allow_same_state:
                raise ValueError(f"Invalid same-state transition to {new_state.value}")

            updated = current.model_copy(
                update={
                    "processing_state": new_state,
                    "attempt": current.attempt if attempt is None else attempt,
                    "queued_at": current.queued_at if queued_at is None else queued_at,
                }
            )
            self._by_event_id[event_id] = updated
            return updated

    def update_attempt(self, event_id: str, attempt: int) -> NotificationEvent:
        with self._lock:
            current = self._by_event_id[event_id]
            updated = current.model_copy(update={"attempt": attempt})
            self._by_event_id[event_id] = updated
            return updated
