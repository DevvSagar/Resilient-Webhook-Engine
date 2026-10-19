import uuid
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.delivery import DeliveryAttempt, DeliveryStatus
from app.schemas.delivery import DeliveryResponse

router = APIRouter(prefix="/deliveries", tags=["Deliveries"])


@router.get(
    "",
    response_model=List[DeliveryResponse],
    summary="List delivery attempts and audit logs",
)
async def list_deliveries(
    status_filter: Optional[DeliveryStatus] = Query(None, alias="status", description="Filter by status: SUCCESS, RETRYING, FAILED, PENDING"),
    event_id: Optional[uuid.UUID] = Query(None, description="Filter by specific event ID"),
    subscription_id: Optional[uuid.UUID] = Query(None, description="Filter by specific subscription ID"),
    limit: int = Query(default=50, ge=1, le=100, description="Max logs to return"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
    db: AsyncSession = Depends(get_db),
):
    """
    Lists delivery attempts and audit history with filtering and pagination.
    Useful for inspecting latencies, response codes, and failed jobs.
    """
    query = select(DeliveryAttempt)

    if status_filter is not None:
        query = query.where(DeliveryAttempt.status == status_filter)
    if event_id is not None:
        query = query.where(DeliveryAttempt.event_id == event_id)
    if subscription_id is not None:
        query = query.where(DeliveryAttempt.subscription_id == subscription_id)

    query = query.order_by(DeliveryAttempt.created_at.desc()).limit(limit).offset(offset)
    result = await db.execute(query)
    return result.scalars().all()


@router.get(
    "/{delivery_id}",
    response_model=DeliveryResponse,
    summary="Get delivery attempt details by ID",
)
async def get_delivery(
    delivery_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves full details of a single delivery attempt, including HTTP status, latency ms, and errors.
    """
    query = select(DeliveryAttempt).where(DeliveryAttempt.id == delivery_id)
    result = await db.execute(query)
    delivery = result.scalar_one_or_none()

    if not delivery:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Delivery attempt with ID '{delivery_id}' not found.",
        )

    return delivery
