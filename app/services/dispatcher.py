import json
import random
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
from app.observability import record_delivery_attempt, track_in_flight_delivery


def calculate_backoff_delay(attempt_number: int) -> float:
    """
    Calculates exponential backoff delay with randomized full jitter.
    Formula: min(2^attempt + random(0.1, 1.0), MAX_BACKOFF_SECONDS).
    Guaranteed never to exceed settings.MAX_BACKOFF_SECONDS.
    """
    base_delay = 2 ** attempt_number
    jitter = random.uniform(0.1, 1.0)
    return round(min(base_delay + jitter, settings.MAX_BACKOFF_SECONDS), 2)


def _apply_failure_state(
    attempt: DeliveryAttempt,
    error_reason: str,
    attempt_number: int,
) -> None:
    """
    Evaluates retry eligibility against settings.MAX_RETRY_ATTEMPTS.
    - If attempt < MAX_RETRY_ATTEMPTS: schedules next retry with jittered backoff.
    - If attempt >= MAX_RETRY_ATTEMPTS: quarantines into Dead Letter Queue (status=FAILED).
    """
    if attempt_number < settings.MAX_RETRY_ATTEMPTS:
        attempt.status = DeliveryStatus.RETRYING
        delay = calculate_backoff_delay(attempt_number)
        attempt.next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
        attempt.error_message = error_reason
    else:
        # Quarantined in Dead Letter Queue (DLQ)
        attempt.status = DeliveryStatus.FAILED
        attempt.next_retry_at = None
        attempt.error_message = (
            f"{error_reason} (Max {settings.MAX_RETRY_ATTEMPTS} retries exhausted. Moved to Dead Letter Queue)"
        )


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

    # 2. Prepare payload bytes, timestamp, and HMAC signature
    timestamp = int(time.time())
    payload_str = json.dumps(event.payload, separators=(",", ":"))
    signature_header = generate_webhook_signature(
        secret_key=subscription.secret_key,
        payload=payload_str,
        timestamp=timestamp,
    )

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "RelayHook-Dispatcher/1.0",
        "X-Webhook-Id": str(event.id),
        "X-Webhook-Timestamp": str(timestamp),
        "X-Webhook-Signature": signature_header,
        "X-Event-Type": event.event_type,
        "X-Delivery-Attempt": str(attempt_number),
    }

    # 3. Fire the non-blocking HTTP POST request across the network
    start_time = time.perf_counter()
    with track_in_flight_delivery():
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
                    _apply_failure_state(
                        attempt=attempt,
                        error_reason=f"Destination server responded with HTTP {resp.status_code}",
                        attempt_number=attempt_number,
                    )
       
        except (httpx.TimeoutException, httpx.RequestError) as exc:
            # Network drop, connection timeout, or DNS resolution failure
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            attempt.latency_ms = round(latency_ms, 2)
            attempt.response_status_code = None
            _apply_failure_state(
                attempt=attempt,
                error_reason=f"Network connection error: {type(exc).__name__} - {str(exc)}",
                attempt_number=attempt_number,
            )

        except Exception as exc:
            # Fallback safeguard against any other unexpected exception
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            attempt.latency_ms = round(latency_ms, 2)
            _apply_failure_state(
                attempt=attempt,
                error_reason=f"Unexpected delivery error: {str(exc)}",
                attempt_number=attempt_number,
            )

    await db.commit()
    await db.refresh(attempt)

    # 4. Observe Prometheus metrics
    record_delivery_attempt(
        status=attempt.status.value,
        latency_seconds=(attempt.latency_ms or 0.0) / 1000.0,
    )

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
