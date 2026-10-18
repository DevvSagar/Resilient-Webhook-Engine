import secrets
import uuid
from datetime import datetime
from typing import List

from sqlalchemy import Boolean, DateTime, JSON, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


def generate_secret_key() -> str:
    """Generate a high-entropy 64-character hex secret for HMAC signing."""
    return secrets.token_hex(32)


class Subscription(Base):
    """
    Represents an external client's registered webhook endpoint.
    """
    __tablename__ = "subscriptions"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        default=uuid.uuid4,
        index=True,
    )
    target_url: Mapped[str] = mapped_column(
        String(512),
        nullable=False,
    )
    secret_key: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default=generate_secret_key,
    )
    event_types: Mapped[List[str]] = mapped_column(
        JSON,
        nullable=False,
        default=list,
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        nullable=False,
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

    def __repr__(self) -> str:
        return f"<Subscription(id={self.id}, target_url='{self.target_url}', active={self.is_active})>"
