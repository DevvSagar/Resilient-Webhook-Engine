import enum
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import (
    DateTime,
    Enum as SQLEnum,
    Float,
    ForeignKey,
    Integer,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base

if TYPE_CHECKING:
    from app.models.event import Event
    from app.models.subscription import Subscription


class DeliveryStatus(str, enum.Enum):
    """Lifecycle states of a webhook delivery attempt."""
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    RETRYING = "RETRYING"
    FAILED = "FAILED"  # Permanently failed after max retries (Dead Letter)


class DeliveryAttempt(Base):
    """
    Audit log and state machine for each webhook delivery attempt.
    Tracks HTTP response codes, latency, errors, and exponential retry schedules.
    """
    __tablename__ = "delivery_attempts"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
    )
    event_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("events.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    subscription_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("subscriptions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    status: Mapped[DeliveryStatus] = mapped_column(
        SQLEnum(DeliveryStatus, name="delivery_status_enum"),
        default=DeliveryStatus.PENDING,
        nullable=False,
        index=True,
    )
    attempt_number: Mapped[int] = mapped_column(
        Integer,
        default=1,
        nullable=False,
    )
    response_status_code: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
    )
    response_body: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
    )
    latency_ms: Mapped[Optional[float]] = mapped_column(
        Float,
        nullable=True,
    )
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # Relationships
    event: Mapped["Event"] = relationship(back_populates="delivery_attempts")
    subscription: Mapped["Subscription"] = relationship(back_populates="delivery_attempts")

    def __repr__(self) -> str:
        return (
            f"<DeliveryAttempt(id={self.id}, event_id={self.event_id}, "
            f"sub_id={self.subscription_id}, status={self.status.value}, "
            f"attempt={self.attempt_number})>"
        )
