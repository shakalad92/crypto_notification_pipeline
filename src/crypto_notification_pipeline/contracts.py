from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class NotificationChannel(str, Enum):
    EMAIL = "email"
    WEBHOOK = "webhook"


class ProcessingState(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    RETRY_PENDING = "retry_pending"
    SENT = "sent"
    FAILED = "failed"


class ApiResult(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    CONFLICT = "conflict"


PIPELINE_STATE_TRANSITIONS: dict[ProcessingState, tuple[ProcessingState, ...]] = {
    ProcessingState.QUEUED: (ProcessingState.PROCESSING,),
    ProcessingState.PROCESSING: (ProcessingState.RETRY_PENDING, ProcessingState.SENT, ProcessingState.FAILED),
    ProcessingState.RETRY_PENDING: (ProcessingState.QUEUED,),
    ProcessingState.SENT: (),
    ProcessingState.FAILED: (),
}


class SubmitTransactionRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    transaction_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    asset: str = Field(min_length=1)
    amount: Decimal = Field(gt=Decimal("0"))
    network: str = Field(min_length=1)
    tx_hash: str = Field(min_length=1)
    occurred_at: datetime
    notification_channel: NotificationChannel
    notification_target: str = Field(min_length=1)


class NotificationEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    payload_hash: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    trace_id: str = Field(min_length=1)
    attempt: int = Field(ge=0)
    processing_state: ProcessingState
    created_at: datetime
    queued_at: datetime
    request: SubmitTransactionRequest


class ApiResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    status_code: int = Field(ge=100, le=599)
    result: ApiResult
    event_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    trace_id: str = Field(min_length=1)
    processing_state: ProcessingState
