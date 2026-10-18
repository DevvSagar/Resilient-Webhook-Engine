import uuid
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.subscription import Subscription
from app.schemas.subscription import (
    SubscriptionCreate,
    SubscriptionResponse,
    SubscriptionUpdate,
)

router = APIRouter(prefix="/subscriptions", tags=["Subscriptions"])


@router.post(
    "",
    response_model=SubscriptionResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new webhook subscription",
)
async def create_subscription(
    payload: SubscriptionCreate,
    db: AsyncSession = Depends(get_db),
):
    """
    Registers an external client endpoint (target_url) and the topics it subscribes to.
    Automatically generates a high-entropy 64-character secret key for HMAC verification.
    """
    subscription = Subscription(
        target_url=str(payload.target_url),
        event_types=payload.event_types,
    )
    db.add(subscription)
    await db.commit()
    await db.refresh(subscription)
    return subscription


@router.get(
    "",
    response_model=List[SubscriptionResponse],
    summary="List all webhook subscriptions",
)
async def list_subscriptions(
    limit: int = Query(default=50, ge=1, le=100, description="Max subscriptions to return"),
    offset: int = Query(default=0, ge=0, description="Offset for pagination"),
    active_only: bool = Query(default=True, description="Filter for active subscriptions only"),
    db: AsyncSession = Depends(get_db),
):
    """
    Lists registered webhook subscriptions with pagination and status filtering.
    """
    query = select(Subscription)
    if active_only:
        query = query.where(Subscription.is_active.is_(True))
    query = query.order_by(Subscription.created_at.desc()).limit(limit).offset(offset)
    
    result = await db.execute(query)
    return result.scalars().all()


@router.get(
    "/{subscription_id}",
    response_model=SubscriptionResponse,
    summary="Get subscription by ID",
)
async def get_subscription(
    subscription_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves details for a single subscription by its UUID.
    """
    query = select(Subscription).where(Subscription.id == subscription_id)
    result = await db.execute(query)
    subscription = result.scalar_one_or_none()
    
    if not subscription:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Subscription with ID '{subscription_id}' not found.",
        )
    return subscription


@router.patch(
    "/{subscription_id}",
    response_model=SubscriptionResponse,
    summary="Update subscription settings or status",
)
async def update_subscription(
    subscription_id: uuid.UUID,
    payload: SubscriptionUpdate,
    db: AsyncSession = Depends(get_db),
):
    """
    Updates target_url, subscribed event topics, or toggles is_active status.
    """
    query = select(Subscription).where(Subscription.id == subscription_id)
    result = await db.execute(query)
    subscription = result.scalar_one_or_none()
    
    if not subscription:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Subscription with ID '{subscription_id}' not found.",
        )

    if payload.target_url is not None:
        subscription.target_url = str(payload.target_url)
    if payload.event_types is not None:
        subscription.event_types = payload.event_types
    if payload.is_active is not None:
        subscription.is_active = payload.is_active

    await db.commit()
    await db.refresh(subscription)
    return subscription


@router.delete(
    "/{subscription_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a subscription",
)
async def delete_subscription(
    subscription_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """
    Permanently deletes a webhook subscription.
    """
    query = select(Subscription).where(Subscription.id == subscription_id)
    result = await db.execute(query)
    subscription = result.scalar_one_or_none()
    
    if not subscription:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Subscription with ID '{subscription_id}' not found.",
        )

    await db.delete(subscription)
    await db.commit()
    return None
