import uuid
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class SubscriptionBase(BaseModel):
    """Shared properties across subscription schemas."""
    target_url: HttpUrl = Field(
        ...,
        description="The destination HTTPS/HTTP URL to deliver webhooks to.",
        examples=["https://api.merchant.com/webhooks"],
    )
    event_types: List[str] = Field(
        ...,
        min_length=1,
        description="List of event topics to subscribe to.",
        examples=[["payment.success", "order.cancelled"]],
    )


class SubscriptionCreate(SubscriptionBase):
    """Payload required to register a new webhook subscription."""
    pass


class SubscriptionUpdate(BaseModel):
    """Payload for updating an existing subscription."""
    target_url: Optional[HttpUrl] = Field(
        None,
        description="Updated destination URL.",
    )
    event_types: Optional[List[str]] = Field(
        None,
        min_length=1,
        description="Updated list of event topics.",
    )
    is_active: Optional[bool] = Field(
        None,
        description="Toggle subscription active status.",
    )


class SubscriptionResponse(BaseModel):
    """Public representation of a webhook subscription."""
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    target_url: str
    secret_key: str
    event_types: List[str]
    is_active: bool
    created_at: datetime
    updated_at: datetime
