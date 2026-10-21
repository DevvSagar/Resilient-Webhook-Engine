import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict

from app.models.delivery import DeliveryStatus


class DeliveryResponse(BaseModel):
    """
    Public representation of a webhook delivery attempt log.
    Includes network status codes, latency in ms, error details, and retry schedules.
    """
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event_id: uuid.UUID
    subscription_id: uuid.UUID
    status: DeliveryStatus
    attempt_number: int
    response_status_code: Optional[int] = None
    response_body: Optional[str] = None
    error_message: Optional[str] = None
    latency_ms: Optional[float] = None
    next_retry_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class BulkReplayResponse(BaseModel):
    """Response returned when initiating a bulk replay from the Dead Letter Queue."""
    message: str
    replayed_count: int
    subscription_id: Optional[uuid.UUID] = None
