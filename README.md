# Crypto Notification Pipeline

Minimal, production-oriented Python system that exercises distributed-systems testing concerns for a crypto transaction notification flow:

`transaction submit -> API -> in-memory queue -> worker -> email/webhook notifier`

The implementation stays deliberately small and framework-free, but it preserves the behaviors that matter in real systems:

- request-level idempotency with atomic reservation
- at-least-once queue delivery semantics
- worker retry with observable intermediate states
- downstream deduplication by `event_id`
- structured logs and counters with end-to-end correlation metadata

## Architecture At A Glance

Pipeline shape:

- API -> queue -> worker -> notifier
- request intake is accepted once per `idempotency_key`
- asynchronous processing is at-least-once and replay-safe

Idempotency strategy (3 layers):

- API reservation dedupe by `idempotency_key` + canonical `payload_hash`
- worker lifecycle state is bound to one stable `event_id`
- notifier dedupes downstream delivery by `event_id`

Retry and failure modes:

- retryable notifier failures move state to `retry_pending`
- scheduled retries move back to `queued` before reprocessing
- retry exhaustion moves to terminal `failed` and dead-letter

Concurrency guarantees:

- idempotency reservation is protected by `threading.Lock`
- concurrent duplicate submissions converge on one logical event
- duplicate queue deliveries are absorbed by downstream dedupe

Observability keys:

- every lifecycle log carries `correlation_id` and `trace_id`
- logs and counters expose queue/worker/notifier outcomes for replay analysis

## Architecture

The package is split into small seams so tests can validate each distributed-systems risk explicitly.

- `contracts.py`
  - `pydantic` request, event, and API response models
  - enums for channel, API result, and processing state
- `api.py`
  - in-process API boundary with HTTP-like responses
  - canonical payload hashing
  - atomic idempotency lookup/create
- `idempotency.py`
  - thread-safe state store keyed by both `idempotency_key` and `event_id`
  - current processing state, attempt counter, and correlation metadata
- `queue.py`
  - in-memory Kafka-like queue with ready, inflight, scheduled retry, and dead-letter buckets
  - duplicate delivery injection and unacked redelivery support
- `worker.py`
  - state transitions, retry scheduling, dead-letter handling, and notifier invocation
- `notifiers.py`
  - email/webhook transport doubles
  - deduplicating notifier keyed by `event_id`
  - non-idempotent negative-control notifier used to prove why dedupe is required
- `observability.py`
  - in-memory counters and structured log sink

## Idempotency Strategy

There are three separate idempotency layers.

### 1. API idempotency

The API stores:

- `idempotency_key`
- canonical `payload_hash`
- `event_id`
- `correlation_id`
- `trace_id`
- current `processing_state`
- current `attempt`

Behavior:

- new key + new payload => create one event and return `202 accepted`
- same key + same payload => return the original event and `200 duplicate`
- same key + different payload => return `409 conflict`

Correctness under concurrency:

- `EventStateStore.create_or_get()` is guarded by a `threading.Lock`
- two concurrent identical requests converge on the same record
- only the caller that created the record publishes to the queue

### 2. Worker/event replay safety

The queue can redeliver the same message. The worker treats that as normal at-least-once behavior rather than assuming exactly-once delivery.

- every lease increments the attempt counter and moves the event to `processing`
- retryable failures move the event to `retry_pending`
- scheduled retries are explicitly released back to `queued`
- permanent failure after the retry budget moves the event to `failed` and dead-letter storage

### 3. Downstream idempotency

The notifier keeps a delivered registry keyed by `event_id`.

This is the final correctness barrier when:

- the queue delivers the same event twice
- the worker retries after a transient error
- the notification is sent successfully but queue ack is lost

If the same `event_id` is processed again after a successful send, the notifier returns an idempotent no-op result instead of sending a second email or webhook.

## Retry Strategy

The worker uses explicit, observable state transitions:

- `queued`
- `processing`
- `retry_pending`
- `queued` again when the scheduled retry is released
- terminal `sent` or `failed`

Retry behavior:

- retryable notifier failures are captured as `RetryableNotificationError`
- if `attempt < max_attempts`, the event is marked `retry_pending` and moved to the scheduled retry bucket
- `release_scheduled_retries()` makes the retry visible as `queued` again
- if `attempt >= max_attempts`, the event is marked `failed`, acked, and moved to dead-letter storage

This keeps eventual consistency explicit. The API returns the current known state, not an invented final state.

## Failure Modes Covered

### Partial failure: notification sent, ack lost

Flow:

- worker marks the event `processing`
- notifier sends successfully
- worker marks the event `sent`
- ack is intentionally withheld
- queue redelivers the unacked message
- notifier dedupes by `event_id`

Expected behavior:

- only one logical notification is sent
- the second processing attempt is absorbed as a deduped replay
- terminal state remains `sent`

### Duplicate queue delivery

Flow:

- the same event is injected into the ready queue twice
- worker processes both copies

Expected behavior:

- transport sees one logical send
- notifier reports one `sent` and one `deduped`
- final event state is still `sent`

### Delayed processing / worker lag

Flow:

- API accepts the request and publishes to the queue
- worker does not run yet
- duplicate API request checks the same `idempotency_key`

Expected behavior:

- the duplicate response returns the original `event_id`
- `processing_state` remains `queued`
- the API does not pretend the notification has already been delivered

## Observability

Every lifecycle record carries:

- `event_id`
- `idempotency_key`
- `correlation_id`
- `trace_id`
- `attempt`
- `processing_state`
- `action`
- timestamp

The queue, worker, and notifier emit structured actions such as:

- `api.accepted`
- `api.duplicate`
- `api.conflict`
- `queue.published`
- `queue.retry_released`
- `queue.redelivered`
- `worker.started`
- `worker.retry_scheduled`
- `worker.failed`
- `worker.completed`
- `worker.ack_lost_simulated`
- `notifier.sent`
- `notifier.deduped`

Counters include:

- `api.accepted`
- `api.duplicate`
- `api.conflict`
- `queue.published`
- `queue.redelivered`
- `worker.processed`
- `worker.retried`
- `worker.failed`
- `worker.ack_lost_simulated`
- `notification.sent`
- `notification.deduped`

Because `correlation_id` and `trace_id` flow from API to queue to worker to notifier, the log stream can reconstruct one event's lifecycle end-to-end.

## Test Coverage And Why It Matters

The suite is intentionally shaped around production-like failure modes rather than only happy paths.

- contract serialization and validation
  - proves the API/event schema is stable and rejects invalid inputs
- accepted / duplicate / conflict API behavior
  - proves request-level idempotency semantics
- explicit state-machine transitions
  - proves `queued -> processing -> retry_pending -> queued -> sent`
- concurrent duplicate submission
  - proves one logical event is created under race conditions
- duplicate submit during lag, processing, and retry
  - proves eventual consistency and current-state visibility
- transient retry then success
  - proves retries preserve identity and do not create duplicate downstream sends
- permanent failure and dead-letter
  - proves retry exhaustion handling
- duplicate queue delivery
  - proves downstream idempotency absorbs replay
- partial failure with lost ack
  - proves correctness when a send succeeds but broker ack does not
- end-to-end email and webhook flows
  - proves both notification channels work through the same pipeline
- observability reconstruction
  - proves logs carry enough metadata to trace one event across components
- negative control without downstream idempotency
  - proves the system really would duplicate sends if notifier dedupe were removed

## Why This Is Production-Oriented

The code intentionally keeps infrastructure in-memory, but the behavior mirrors
real distributed systems boundaries:

- idempotency is enforced at ingress and at downstream delivery boundaries
- queue processing is modeled as at-least-once, not exactly-once
- retries and dead-letter outcomes are explicit and observable
- partial failure modes (send success with ack loss) are handled without
  duplicate user-facing notifications
- correlation and trace metadata are preserved from API to worker/notifier logs

This makes the implementation suitable as a correctness baseline before swapping
in external queue/broker and transport adapters.

## Running The Project

Create a local environment and install dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -e .
```

Run the test suite:

```bash
pytest -q
.venv/bin/pytest -q
```

No tests are skipped and no `xfail` markers are used.
