from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import threading

import pytest
from pydantic import ValidationError

from crypto_notification_pipeline.api import NotificationAPI
from crypto_notification_pipeline.contracts import (
    ApiResult,
    NotificationChannel,
    NotificationEvent,
    PIPELINE_STATE_TRANSITIONS,
    ProcessingState,
    SubmitTransactionRequest,
)
from crypto_notification_pipeline.idempotency import EventStateStore
from crypto_notification_pipeline.notifiers import (
    AlwaysFailTransport,
    BlockingTransport,
    DeduplicatingNotifier,
    FlakyTransport,
    InMemoryTransport,
    NonIdempotentNotifier,
)
from crypto_notification_pipeline.observability import InMemoryLogSink, InMemoryMetrics
from crypto_notification_pipeline.queue import InMemoryQueue
from crypto_notification_pipeline.worker import NotificationWorker


def build_request(
    *,
    transaction_id: str = "txn-001",
    amount: str = "1.25",
    channel: NotificationChannel = NotificationChannel.EMAIL,
    target: str | None = None,
) -> SubmitTransactionRequest:
    return SubmitTransactionRequest(
        transaction_id=transaction_id,
        user_id="user-123",
        asset="BTC",
        amount=Decimal(amount),
        network="bitcoin",
        tx_hash="0xabc123",
        occurred_at=datetime(2026, 4, 2, 10, 0, tzinfo=timezone.utc),
        notification_channel=channel,
        notification_target=target
        or ("ops@example.com" if channel is NotificationChannel.EMAIL else "https://hooks.example.com"),
    )


def build_system(
    *,
    notifier=None,
    max_attempts: int = 3,
):
    queue = InMemoryQueue()
    metrics = InMemoryMetrics()
    logs = InMemoryLogSink()
    state_store = EventStateStore()
    notifier = notifier or DeduplicatingNotifier(InMemoryTransport())
    api = NotificationAPI(queue=queue, state_store=state_store, metrics=metrics, logs=logs)
    worker = NotificationWorker(
        queue=queue,
        state_store=state_store,
        notifier=notifier,
        metrics=metrics,
        logs=logs,
        max_attempts=max_attempts,
    )
    return api, worker, queue, state_store, metrics, logs, notifier


def test_contract_models_serialize_expected_shape():
    request = build_request()
    dumped_request = request.model_dump(mode="json")

    assert dumped_request["transaction_id"] == "txn-001"
    assert dumped_request["notification_channel"] == "email"
    assert dumped_request["amount"] == "1.25"

    event = NotificationEvent(
        event_id="evt-001",
        idempotency_key="idemp-001",
        payload_hash="payload-hash",
        correlation_id="corr-001",
        trace_id="trace-001",
        attempt=1,
        processing_state=ProcessingState.QUEUED,
        created_at=datetime(2026, 4, 2, 10, 0, tzinfo=timezone.utc),
        queued_at=datetime(2026, 4, 2, 10, 0, tzinfo=timezone.utc),
        request=request,
    )

    dumped_event = event.model_dump(mode="json")
    assert dumped_event["request"]["transaction_id"] == "txn-001"
    assert dumped_event["processing_state"] == "queued"
    assert dumped_event["correlation_id"] == "corr-001"


def test_contract_validation_rejects_invalid_payloads():
    with pytest.raises(ValidationError):
        SubmitTransactionRequest(
            transaction_id="txn-001",
            user_id="user-123",
            asset="BTC",
            amount=Decimal("-1"),
            network="bitcoin",
            tx_hash="0xabc123",
            occurred_at=datetime(2026, 4, 2, 10, 0, tzinfo=timezone.utc),
            notification_channel=NotificationChannel.EMAIL,
            notification_target="ops@example.com",
        )

    with pytest.raises(ValidationError):
        SubmitTransactionRequest(
            transaction_id="txn-001",
            user_id="user-123",
            asset="BTC",
            amount=Decimal("1.0"),
            network="bitcoin",
            tx_hash="0xabc123",
            occurred_at=datetime(2026, 4, 2, 10, 0, tzinfo=timezone.utc),
            notification_channel="sms",
            notification_target="ops@example.com",
        )


def test_api_contract_returns_accepted_duplicate_and_conflict():
    api, _, queue, _, _, _, _ = build_system()
    request = build_request()

    accepted = api.submit_transaction(request, idempotency_key="idemp-001")
    duplicate = api.submit_transaction(request, idempotency_key="idemp-001")
    conflict = api.submit_transaction(
        build_request(amount="2.5"),
        idempotency_key="idemp-001",
    )

    assert accepted.status_code == 202
    assert accepted.result is ApiResult.ACCEPTED
    assert accepted.processing_state is ProcessingState.QUEUED
    assert duplicate.status_code == 200
    assert duplicate.result is ApiResult.DUPLICATE
    assert duplicate.event_id == accepted.event_id
    assert duplicate.correlation_id == accepted.correlation_id
    assert duplicate.trace_id == accepted.trace_id
    assert conflict.status_code == 409
    assert conflict.result is ApiResult.CONFLICT
    assert queue.ready_count == 1


def test_state_machine_transitions_are_explicit():
    notifier = DeduplicatingNotifier(FlakyTransport(failures_before_success=1))
    api, worker, _, store, _, _, _ = build_system(notifier=notifier, max_attempts=3)
    request = build_request()

    response = api.submit_transaction(request, idempotency_key="idemp-002")
    assert store.get(response.event_id).processing_state is ProcessingState.QUEUED

    first_result = worker.process_next()
    assert first_result == "retry_pending"
    assert store.get(response.event_id).processing_state is ProcessingState.RETRY_PENDING

    worker.release_scheduled_retries()
    assert store.get(response.event_id).processing_state is ProcessingState.QUEUED

    second_result = worker.process_next()
    assert second_result == "sent"
    assert store.get(response.event_id).processing_state is ProcessingState.SENT


def test_contract_state_machine_definition_is_consistent():
    assert PIPELINE_STATE_TRANSITIONS[ProcessingState.QUEUED] == (ProcessingState.PROCESSING,)
    assert PIPELINE_STATE_TRANSITIONS[ProcessingState.PROCESSING] == (
        ProcessingState.RETRY_PENDING,
        ProcessingState.SENT,
        ProcessingState.FAILED,
    )
    assert PIPELINE_STATE_TRANSITIONS[ProcessingState.RETRY_PENDING] == (ProcessingState.QUEUED,)
    assert PIPELINE_STATE_TRANSITIONS[ProcessingState.SENT] == ()
    assert PIPELINE_STATE_TRANSITIONS[ProcessingState.FAILED] == ()


def test_duplicate_requests_are_idempotent():
    api, _, queue, _, _, _, _ = build_system()
    request = build_request()

    first = api.submit_transaction(request, idempotency_key="idemp-003")
    second = api.submit_transaction(request, idempotency_key="idemp-003")

    assert first.event_id == second.event_id
    assert first.status_code == 202
    assert second.status_code == 200
    assert queue.ready_count == 1


def test_same_idempotency_key_with_different_payload_conflicts():
    api, _, queue, _, _, _, _ = build_system()

    accepted = api.submit_transaction(build_request(), idempotency_key="idemp-004")
    conflict = api.submit_transaction(
        build_request(transaction_id="txn-009"),
        idempotency_key="idemp-004",
    )

    assert accepted.status_code == 202
    assert conflict.status_code == 409
    assert queue.ready_count == 1


def test_concurrent_duplicate_submissions_create_one_logical_event():
    api, _, queue, _, metrics, _, _ = build_system()
    request = build_request()
    barrier = threading.Barrier(3)
    results = []
    errors = []

    def submit():
        try:
            barrier.wait()
            results.append(api.submit_transaction(request, idempotency_key="idemp-race"))
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=submit) for _ in range(2)]
    for thread in threads:
        thread.start()

    barrier.wait()
    for thread in threads:
        thread.join()

    assert not errors
    assert {result.event_id for result in results} == {results[0].event_id}
    assert sorted(result.status_code for result in results) == [200, 202]
    assert queue.ready_count == 1
    assert metrics.get("api.accepted") == 1
    assert metrics.get("api.duplicate") == 1


def test_duplicate_submit_surfaces_queued_state_during_worker_lag():
    api, _, _, _, _, _, _ = build_system()
    request = build_request()

    accepted = api.submit_transaction(request, idempotency_key="idemp-lag")
    duplicate = api.submit_transaction(request, idempotency_key="idemp-lag")

    assert accepted.processing_state is ProcessingState.QUEUED
    assert duplicate.processing_state is ProcessingState.QUEUED


def test_duplicate_submit_surfaces_processing_state_while_worker_is_active():
    transport = BlockingTransport()
    notifier = DeduplicatingNotifier(transport)
    api, worker, _, _, _, _, _ = build_system(notifier=notifier)
    request = build_request()
    response = api.submit_transaction(request, idempotency_key="idemp-processing")

    thread = threading.Thread(target=worker.process_next)
    thread.start()
    assert transport.entered.wait(timeout=2)

    duplicate = api.submit_transaction(request, idempotency_key="idemp-processing")
    assert duplicate.processing_state is ProcessingState.PROCESSING
    assert duplicate.event_id == response.event_id

    transport.release.set()
    thread.join()


def test_duplicate_submit_surfaces_retry_pending_state_before_retry_release():
    notifier = DeduplicatingNotifier(FlakyTransport(failures_before_success=1))
    api, worker, _, _, _, _, _ = build_system(notifier=notifier)
    request = build_request()

    api.submit_transaction(request, idempotency_key="idemp-retry-pending")
    worker.process_next()

    duplicate = api.submit_transaction(request, idempotency_key="idemp-retry-pending")
    assert duplicate.processing_state is ProcessingState.RETRY_PENDING


def test_retry_flow_preserves_identity_and_sends_once():
    transport = FlakyTransport(failures_before_success=1)
    notifier = DeduplicatingNotifier(transport)
    api, worker, _, store, metrics, logs, _ = build_system(notifier=notifier)
    request = build_request(channel=NotificationChannel.WEBHOOK)

    accepted = api.submit_transaction(request, idempotency_key="idemp-retry")

    assert worker.process_next() == "retry_pending"
    worker.release_scheduled_retries()
    assert worker.process_next() == "sent"

    record = store.get(accepted.event_id)
    assert record.processing_state is ProcessingState.SENT
    assert record.attempt == 2
    assert record.correlation_id == accepted.correlation_id
    assert record.trace_id == accepted.trace_id
    assert len(transport.deliveries) == 1
    assert metrics.get("worker.retried") == 1
    assert metrics.get("queue.retry_scheduled") == 1
    assert metrics.get("queue.retry_released") == 1
    assert [entry["attempt"] for entry in logs.filter_by_event(accepted.event_id) if entry["action"] == "worker.started"] == [1, 2]


def test_permanent_failure_moves_event_to_failed_and_dead_letter():
    notifier = DeduplicatingNotifier(AlwaysFailTransport())
    api, worker, queue, store, metrics, _, _ = build_system(notifier=notifier, max_attempts=2)
    request = build_request()

    accepted = api.submit_transaction(request, idempotency_key="idemp-dead-letter")

    assert worker.process_next() == "retry_pending"
    worker.release_scheduled_retries()
    assert worker.process_next() == "failed"

    assert store.get(accepted.event_id).processing_state is ProcessingState.FAILED
    assert queue.dead_letter_count == 1
    assert metrics.get("worker.failed") == 1
    assert metrics.get("queue.dead_lettered") == 1


def test_duplicate_queue_delivery_is_absorbed_by_downstream_idempotency():
    transport = InMemoryTransport()
    notifier = DeduplicatingNotifier(transport)
    api, worker, queue, store, metrics, _, _ = build_system(notifier=notifier)
    request = build_request()

    accepted = api.submit_transaction(request, idempotency_key="idemp-duplicate-delivery")
    queue.inject_duplicate(accepted.event_id)

    assert worker.process_next() == "sent"
    assert worker.process_next() == "sent"

    assert store.get(accepted.event_id).processing_state is ProcessingState.SENT
    assert len(transport.deliveries) == 1
    assert metrics.get("notification.sent") == 1
    assert metrics.get("notification.deduped") == 1


def test_partial_failure_send_succeeds_but_ack_is_lost():
    transport = InMemoryTransport()
    notifier = DeduplicatingNotifier(transport)
    api, worker, queue, store, metrics, logs, _ = build_system(notifier=notifier)
    request = build_request()

    accepted = api.submit_transaction(request, idempotency_key="idemp-ack-loss")

    assert worker.process_next(simulate_ack_loss=True) == "sent"
    queue.redeliver_unacked()
    assert worker.process_next() == "sent"

    assert store.get(accepted.event_id).processing_state is ProcessingState.SENT
    assert len(transport.deliveries) == 1
    assert metrics.get("worker.ack_lost_simulated") == 1
    assert metrics.get("queue.redelivered") == 1
    dedupe_logs = [entry for entry in logs.filter_by_event(accepted.event_id) if entry["action"] == "notifier.deduped"]
    assert len(dedupe_logs) == 1


@pytest.mark.parametrize(
    ("channel", "target"),
    [
        (NotificationChannel.EMAIL, "alerts@example.com"),
        (NotificationChannel.WEBHOOK, "https://hooks.example.com/notify"),
    ],
)
def test_end_to_end_delivery_for_email_and_webhook(channel, target):
    transport = InMemoryTransport()
    notifier = DeduplicatingNotifier(transport)
    api, worker, _, store, metrics, logs, _ = build_system(notifier=notifier)
    request = build_request(channel=channel, target=target)

    accepted = api.submit_transaction(request, idempotency_key=f"idemp-{channel.value}")
    assert worker.process_next() == "sent"

    assert store.get(accepted.event_id).processing_state is ProcessingState.SENT
    assert transport.deliveries[0]["channel"] == channel.value
    assert transport.deliveries[0]["target"] == target
    assert metrics.get("api.accepted") == 1
    assert metrics.get("queue.published") == 1
    assert metrics.get("worker.processed") == 1
    assert metrics.get("notification.sent") == 1
    actions = [entry["action"] for entry in logs.filter_by_event(accepted.event_id)]
    assert actions[0] == "api.accepted"
    assert "worker.completed" in actions


def test_observability_reconstructs_full_retry_lifecycle():
    transport = FlakyTransport(failures_before_success=1)
    notifier = DeduplicatingNotifier(transport)
    api, worker, _, _, _, logs, _ = build_system(notifier=notifier)
    request = build_request()

    accepted = api.submit_transaction(request, idempotency_key="idemp-observability")
    worker.process_next()
    worker.release_scheduled_retries()
    worker.process_next()

    event_logs = logs.filter_by_event(accepted.event_id)
    assert event_logs
    correlation_ids = {entry["correlation_id"] for entry in event_logs}
    trace_ids = {entry["trace_id"] for entry in event_logs}
    assert correlation_ids == {accepted.correlation_id}
    assert trace_ids == {accepted.trace_id}

    actions = [entry["action"] for entry in event_logs]
    assert actions.index("api.accepted") < actions.index("queue.published")
    assert actions.index("worker.started") < actions.index("worker.retry_scheduled")
    assert actions.index("worker.retry_scheduled") < actions.index("queue.retry_released")
    assert actions[-1] == "worker.completed"


def test_negative_control_without_downstream_idempotency_duplicates_delivery():
    transport = InMemoryTransport()
    notifier = NonIdempotentNotifier(transport)
    api, worker, queue, _, _, _, _ = build_system(notifier=notifier)
    request = build_request()

    api.submit_transaction(request, idempotency_key="idemp-negative")
    worker.process_next(simulate_ack_loss=True)
    queue.redeliver_unacked()
    worker.process_next()

    assert len(transport.deliveries) == 2
