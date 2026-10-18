import uuid
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.event import Event
from app.schemas.event import EventCreate, EventResponse

router = APIRouter(prefix="/events", tags=["Events"])


@router.post(
    "",
    response_model=EventResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest an event with idempotency guarantees",
)
async def ingest_event(
    payload: EventCreate,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """
    Ingests an event to be dispatched to matching webhook subscribers.
    Guarantees strict deduplication via idempotency_key.

    - If idempotency_key is NEW:
      Saves the event, returns HTTP 202 Accepted, and queues delivery.
    - If idempotency_key ALREADY EXISTS:
      Returns the existing event with HTTP 200 OK and status='already_processed'.
    """
    # 1. Idempotency Check: query existing event by key
    query = select(Event).where(Event.idempotency_key == payload.idempotency_key)
    result = await db.execute(query)
    existing_event = result.scalar_one_or_none()

    if existing_event:
        # Idempotency hit: return existing event with 200 OK without re-processing
        response.status_code = status.HTTP_200_OK
        return EventResponse(
            id=existing_event.id,
            idempotency_key=existing_event.idempotency_key,
            event_type=existing_event.event_type,
            payload=existing_event.payload,
            status="already_processed",
            created_at=existing_event.created_at,
        )

    # 2. Brand new event: create and commit
    event = Event(
        idempotency_key=payload.idempotency_key,
        event_type=payload.event_type,
        payload=payload.payload,
    )
    db.add(event)

    try:
        await db.commit()
        await db.refresh(event)
    except IntegrityError:
        # Race-condition safeguard: another concurrent request committed the same key
        await db.rollback()
        query = select(Event).where(Event.idempotency_key == payload.idempotency_key)
        result = await db.execute(query)
        existing_event = result.scalar_one_or_none()
        if existing_event:
            response.status_code = status.HTTP_200_OK
            return EventResponse(
                id=existing_event.id,
                idempotency_key=existing_event.idempotency_key,
                event_type=existing_event.event_type,
                payload=existing_event.payload,
                status="already_processed",
                created_at=existing_event.created_at,
            )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Conflict inserting event with given idempotency key.",
        )

    return EventResponse(
        id=event.id,
        idempotency_key=event.idempotency_key,
        event_type=event.event_type,
        payload=event.payload,
        status="queued",
        created_at=event.created_at,
    )


@router.get(
    "/{event_id}",
    response_model=EventResponse,
    summary="Get event by ID",
)
async def get_event(
    event_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """
    Retrieves an ingested event by its UUID.
    """
    query = select(Event).where(Event.id == event_id)
    result = await db.execute(query)
    event = result.scalar_one_or_none()

    if not event:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Event with ID '{event_id}' not found.",
        )

    return EventResponse(
        id=event.id,
        idempotency_key=event.idempotency_key,
        event_type=event.event_type,
        payload=event.payload,
        status="stored",
        created_at=event.created_at,
    )


@router.get(
    "",
    response_model=List[EventResponse],
    summary="List recent events",
)
async def list_events(
    limit: int = Query(default=50, ge=1, le=100, description="Max events to return"),
    offset: int = Query(default=0, ge=0, description="Offset for pagination"),
    db: AsyncSession = Depends(get_db),
):
    """
    Lists recent ingested events with pagination.
    """
    query = select(Event).order_by(Event.created_at.desc()).limit(limit).offset(offset)
    result = await db.execute(query)
    events = result.scalars().all()

    return [
        EventResponse(
            id=evt.id,
            idempotency_key=evt.idempotency_key,
            event_type=evt.event_type,
            payload=evt.payload,
            status="stored",
            created_at=evt.created_at,
        )
        for evt in events
    ]
