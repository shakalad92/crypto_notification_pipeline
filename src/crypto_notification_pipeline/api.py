from __future__ import annotations

import hashlib
import json

from .contracts import ApiResponse, ApiResult, SubmitTransactionRequest
from .idempotency import EventStateStore
from .observability import InMemoryLogSink, InMemoryMetrics
from .queue import InMemoryQueue


class NotificationAPI:
    def __init__(
        self,
        *,
        queue: InMemoryQueue,
        state_store: EventStateStore,
        metrics: InMemoryMetrics,
        logs: InMemoryLogSink,
    ) -> None:
        self._queue = queue
        self._state_store = state_store
        self._metrics = metrics
        self._logs = logs
        self._queue.bind_observability(metrics=metrics, logs=logs, state_store=state_store)

    def submit_transaction(self, request: SubmitTransactionRequest, *, idempotency_key: str) -> ApiResponse:
        payload_hash = self._canonical_payload_hash(request)
        reservation = self._state_store.create_or_get(
            idempotency_key=idempotency_key,
            payload_hash=payload_hash,
            request=request,
        )
        event = reservation.event

        if reservation.outcome == "accepted":
            self._metrics.increment("api.accepted")
            self._logs.record(
                "api.accepted",
                event_id=event.event_id,
                idempotency_key=event.idempotency_key,
                correlation_id=event.correlation_id,
                trace_id=event.trace_id,
                attempt=event.attempt,
                processing_state=event.processing_state.value,
            )
            self._queue.publish(event.event_id)
            self._metrics.increment("queue.published")
            self._logs.record(
                "queue.published",
                event_id=event.event_id,
                idempotency_key=event.idempotency_key,
                correlation_id=event.correlation_id,
                trace_id=event.trace_id,
                attempt=event.attempt,
                processing_state=event.processing_state.value,
            )
            return ApiResponse(
                status_code=202,
                result=ApiResult.ACCEPTED,
                event_id=event.event_id,
                idempotency_key=event.idempotency_key,
                correlation_id=event.correlation_id,
                trace_id=event.trace_id,
                processing_state=event.processing_state,
            )

        if reservation.outcome == "duplicate":
            self._metrics.increment("api.duplicate")
            self._logs.record(
                "api.duplicate",
                event_id=event.event_id,
                idempotency_key=event.idempotency_key,
                correlation_id=event.correlation_id,
                trace_id=event.trace_id,
                attempt=event.attempt,
                processing_state=event.processing_state.value,
            )
            return ApiResponse(
                status_code=200,
                result=ApiResult.DUPLICATE,
                event_id=event.event_id,
                idempotency_key=event.idempotency_key,
                correlation_id=event.correlation_id,
                trace_id=event.trace_id,
                processing_state=event.processing_state,
            )

        self._metrics.increment("api.conflict")
        self._logs.record(
            "api.conflict",
            event_id=event.event_id,
            idempotency_key=event.idempotency_key,
            correlation_id=event.correlation_id,
            trace_id=event.trace_id,
            attempt=event.attempt,
            processing_state=event.processing_state.value,
        )
        return ApiResponse(
            status_code=409,
            result=ApiResult.CONFLICT,
            event_id=event.event_id,
            idempotency_key=event.idempotency_key,
            correlation_id=event.correlation_id,
            trace_id=event.trace_id,
            processing_state=event.processing_state,
        )

    @staticmethod
    def _canonical_payload_hash(request: SubmitTransactionRequest) -> str:
        payload = request.model_dump(mode="json")
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
