import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import List

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.security import generate_webhook_signature
from app.models.delivery import DeliveryAttempt, DeliveryStatus
from app.models.event import Event
from app.models.subscription import Subscription


async def deliver_to_subscription(
    event: Event,
    subscription: Subscription,
    db: AsyncSession,
    attempt_number: int = 1,
) -> DeliveryAttempt:
    """
    Executes a single delivery attempt of an event to a specific subscription.
    Handles HMAC signing, async HTTP POST dispatch, latency tracking, and error logging.
    """
    # 1. Create a PENDING delivery attempt log in PostgreSQL
    attempt = DeliveryAttempt(
        event_id=event.id,
        subscription_id=subscription.id,
        status=DeliveryStatus.PENDING,
        attempt_number=attempt_number,
    )
    db.add(attempt)
    await db.commit()
    await db.refresh(attempt)

    # 2. Prepare payload bytes and HMAC signature
    payload_str = json.dumps(event.payload, separators=(",", ":"))
    signature_header = generate_webhook_signature(
        secret_key=subscription.secret_key,
        payload=payload_str,
    )

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "RelayHook-Dispatcher/1.0",
        "X-Webhook-Signature": signature_header,
        "X-Event-Type": event.event_type,
        "X-Delivery-Attempt": str(attempt_number),
    }

    # 3. Fire the non-blocking HTTP POST request across the network
    start_time = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=settings.DEFAULT_TIMEOUT_SECONDS) as client:
            resp = await client.post(
                url=subscription.target_url,
                content=payload_str,
                headers=headers,
            )
            latency_ms = (time.perf_counter() - start_time) * 1000.0

            attempt.latency_ms = round(latency_ms, 2)
            attempt.response_status_code = resp.status_code
            attempt.response_body = resp.text[:500] if resp.text else None

            # Outcome Evaluation: 2xx Success vs 4xx/5xx Failure
            if 200 <= resp.status_code < 300:
                attempt.status = DeliveryStatus.SUCCESS
                attempt.next_retry_at = None
                attempt.error_message = None
            else:
                attempt.status = DeliveryStatus.RETRYING
                attempt.error_message = f"Destination server responded with HTTP {resp.status_code}"
                # Schedule retry: exponential backoff (2 ^ attempt_number seconds)
                delay_seconds = 2 ** attempt_number
                attempt.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
   
    except (httpx.TimeoutException, httpx.RequestError) as exc:
        # Network drop, connection timeout, or DNS resolution failure
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        attempt.latency_ms = round(latency_ms, 2)
        attempt.status = DeliveryStatus.RETRYING
        attempt.response_status_code = None
        attempt.error_message = f"Network connection error: {type(exc).__name__} - {str(exc)}"
        delay_seconds = 2 ** attempt_number
        attempt.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

    except Exception as exc:
        # Fallback safeguard against any other unexpected exception
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        attempt.latency_ms = round(latency_ms, 2)
        attempt.status = DeliveryStatus.RETRYING
        attempt.error_message = f"Unexpected delivery error: {str(exc)}"
        delay_seconds = 2 ** attempt_number
        attempt.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

    await db.commit()
    await db.refresh(attempt)
    return attempt
    

async def dispatch_event(event_id: uuid.UUID) -> List[DeliveryAttempt]:
    """
    Main background entry point for event delivery.
    1. Fetches the event by event_id.
    2. Queries all active subscriptions listening to event.event_type.
    3. Dispatches deliveries concurrently and records attempts.
    """
    async with AsyncSessionLocal() as db:
        # 1. Fetch Event
        event_query = select(Event).where(Event.id == event_id)
        result = await db.execute(event_query)
        event = result.scalar_one_or_none()
        if not event:
            return []

        # 2. Fetch Active Subscriptions
        sub_query = select(Subscription).where(Subscription.is_active.is_(True))
        sub_result = await db.execute(sub_query)
        all_active_subs = sub_result.scalars().all()

        # Match subscriptions subscribing to this event's topic
        matching_subs = [
            sub for sub in all_active_subs
            if event.event_type in sub.event_types
        ]

        if not matching_subs:
            return []

        # 3. Deliver to each matching subscription
        delivery_results = []
        for sub in matching_subs:
            attempt = await deliver_to_subscription(
                event=event,
                subscription=sub,
                db=db,
                attempt_number=1,
            )
            delivery_results.append(attempt)

        return delivery_results
