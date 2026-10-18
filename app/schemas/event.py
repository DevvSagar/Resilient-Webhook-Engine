import uuid
from datetime import datetime
from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field


class EventCreate(BaseModel):
    """
    Payload required to ingest an event into the engine.
    The idempotency_key is mandatory to guarantee zero duplicate deliveries.
    """
    idempotency_key: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique client-generated key to prevent duplicate processing.",
        examples=["order_9988_pay"],
    )
    event_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="The topic/type of the event.",
        examples=["dominos.order"],
    )
    payload: Dict[str, Any] = Field(
        ...,
        description="Arbitrary JSON payload containing event details.",
        examples=[{"order_id": 1045, "pizza": "Margherita", "customer": "Sagar"}],
    )


class EventResponse(BaseModel):
    """
    Public representation of an ingested event.
    """
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    idempotency_key: str
    event_type: str
    payload: Dict[str, Any]
    status: str = Field(
        default="queued",
        description="Current ingestion status: 'queued' or 'already_processed'.",
    )
    created_at: datetime
