import uuid
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.delivery import DeliveryAttempt, DeliveryStatus
from app.models.event import Event
from app.models.subscription import Subscription
from app.schemas.delivery import BulkReplayResponse, DeliveryResponse
from app.services.dispatcher import deliver_to_subscription

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
    "/dlq",
    response_model=List[DeliveryResponse],
    summary="List quarantined Dead Letter Queue (DLQ) deliveries",
)
async def list_dlq_deliveries(
    subscription_id: Optional[uuid.UUID] = Query(None, description="Filter DLQ by subscription ID"),
    limit: int = Query(default=50, ge=1, le=100, description="Max logs to return"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns all permanently failed deliveries quarantined in the Dead Letter Queue.
    Sorted in chronological FIFO order (oldest first).
    """
    query = select(DeliveryAttempt).where(DeliveryAttempt.status == DeliveryStatus.FAILED)
    if subscription_id is not None:
        query = query.where(DeliveryAttempt.subscription_id == subscription_id)

    query = query.order_by(DeliveryAttempt.created_at.asc()).limit(limit).offset(offset)
    result = await db.execute(query)
    return result.scalars().all()


@router.post(
    "/dlq/replay-all",
    response_model=BulkReplayResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Bulk replay quarantined Dead Letter Queue deliveries in FIFO order",
)
async def bulk_replay_dlq(
    subscription_id: Optional[uuid.UUID] = Query(None, description="Replay DLQ for a specific subscription"),
    limit: int = Query(default=100, ge=1, le=500, description="Max orders to queue for replay"),
    db: AsyncSession = Depends(get_db),
):
    """
    Bulk replays quarantined Dead Letter Queue orders in chronological FIFO order.
    Flips eligible FAILED rows to RETRYING with next_retry_at = NOW().
    The autonomous background worker and Redis rate limiter safely deliver them.
    """
    query = select(DeliveryAttempt).where(DeliveryAttempt.status == DeliveryStatus.FAILED)

    if subscription_id is not None:
        sub_res = await db.execute(select(Subscription).where(Subscription.id == subscription_id))
        sub = sub_res.scalar_one_or_none()
        if not sub or not sub.is_active:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Specified subscription does not exist or is inactive.",
            )
        query = query.where(DeliveryAttempt.subscription_id == subscription_id)

    # Strict FIFO ordering
    query = query.order_by(DeliveryAttempt.created_at.asc()).limit(limit)
    result = await db.execute(query)
    failed_attempts = result.scalars().all()

    if not failed_attempts:
        return BulkReplayResponse(
            message="No quarantined deliveries found matching criteria.",
            replayed_count=0,
            subscription_id=subscription_id,
        )

    now_utc = datetime.now(timezone.utc)
    for attempt in failed_attempts:
        attempt.status = DeliveryStatus.RETRYING
        attempt.next_retry_at = now_utc
        attempt.error_message = "Replay initiated from Dead Letter Queue."

    await db.commit()

    return BulkReplayResponse(
        message=f"Successfully queued {len(failed_attempts)} dead-letter deliveries for replay.",
        replayed_count=len(failed_attempts),
        subscription_id=subscription_id,
    )


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


#  manually + 1 req at a time 
@router.post(
    "/{delivery_id}/replay",
    response_model=DeliveryResponse,
    status_code=status.HTTP_200_OK,
    summary="Manually replay a failed or dead-letter delivery",
)
async def replay_delivery(
    delivery_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """
    Manually replays a failed delivery attempt from the Dead Letter Queue.
    Resets the attempt counter to 1 and immediately dispatches the event.
    """
    query = select(DeliveryAttempt).where(DeliveryAttempt.id == delivery_id)
    result = await db.execute(query)
    old_delivery = result.scalar_one_or_none()

    if not old_delivery:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Delivery attempt with ID '{delivery_id}' not found.",
        )

    if old_delivery.status == DeliveryStatus.SUCCESS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot replay an already successful delivery attempt.",
        )

    # Fetch parent Event
    event_query = select(Event).where(Event.id == old_delivery.event_id)
    event_res = await db.execute(event_query)
    event = event_res.scalar_one_or_none()
    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Associated event record not found.",
        )

    # Fetch parent Subscription
    sub_query = select(Subscription).where(Subscription.id == old_delivery.subscription_id)
    sub_res = await db.execute(sub_query)
    subscription = sub_res.scalar_one_or_none()
    if not subscription or not subscription.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Target subscription is inactive or deleted.",
        )

    # Trigger fresh delivery attempt (attempt_number=1)
    replayed_attempt = await deliver_to_subscription(
        event=event,
        subscription=subscription,
        db=db,
        attempt_number=1,
    )

    return replayed_attempt
