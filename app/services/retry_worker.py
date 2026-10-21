import asyncio
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.redis import get_redis_client
from app.models.delivery import DeliveryAttempt, DeliveryStatus
from app.models.event import Event
from app.models.subscription import Subscription
from app.services.dispatcher import deliver_to_subscription
from app.services.rate_limiter import check_rate_limit

logger = logging.getLogger("relayhook.retry_worker")

# Module-level worker state for graceful lifecycle management
_worker_task: Optional[asyncio.Task] = None
_stop_event = asyncio.Event()

# Per-subscriber concurrency limits (Max in-flight HTTP connections per subscriber)
_subscriber_semaphores: Dict[uuid.UUID, asyncio.Semaphore] = defaultdict(
    lambda: asyncio.Semaphore(settings.DEFAULT_MAX_CONCURRENT_PER_SUBSCRIBER)
)


async def _dispatch_single_retry(old_attempt_id: uuid.UUID) -> bool:
    """
    Safely processes a single overdue retry attempt in its own isolated database session.
    Enforces Rate Limiting (Token Bucket) and Concurrency Limits (Semaphore).

    Returns:
        True if the delivery attempt was dispatched.
        False if it was deferred due to rate limits or inactive subscription.
    """
    async with AsyncSessionLocal() as db:
        now_utc = datetime.now(timezone.utc)

        # 1. Fetch the overdue attempt
        query = select(DeliveryAttempt).where(DeliveryAttempt.id == old_attempt_id)
        res = await db.execute(query)
        old_attempt = res.scalar_one_or_none()

        if not old_attempt:
            return False

        # 2. Fetch parent Subscription and Event
        sub_res = await db.execute(
            select(Subscription).where(Subscription.id == old_attempt.subscription_id)
        )
        subscription = sub_res.scalar_one_or_none()

        evt_res = await db.execute(
            select(Event).where(Event.id == old_attempt.event_id)
        )
        event = evt_res.scalar_one_or_none()

        # Handle deactivated partner or missing event
        if not subscription or not subscription.is_active or not event:
            old_attempt.status = DeliveryStatus.FAILED
            old_attempt.next_retry_at = None
            old_attempt.error_message = "Subscription or Event is no longer active."
            await db.commit()
            return False

        # 3. 🛡️ GATE 1: Token-Bucket Rate Limiter Check
        redis = get_redis_client()
        rate_result = await check_rate_limit(redis, subscription.id)

        if not rate_result.allowed:
            # 🛑 Bucket is empty for this partner!
            # Do NOT fail the order. Politely postpone next_retry_at by retry_after seconds!
            delay = max(0.5, rate_result.retry_after)
            old_attempt.next_retry_at = now_utc + timedelta(seconds=delay)
            old_attempt.error_message = (
                f"Delivery throttled by rate limiter. Rescheduled in {delay:.2f}s."
            )
            await db.commit()
            logger.info(
                "⏸️ Throttling delivery to %s: Bucket empty. Rescheduled in %.2fs.",
                subscription.target_url,
                delay,
            )
            return False

        # 4. Retire old attempt so it cannot be picked up again
        old_attempt.next_retry_at = None
        await db.commit()

        # 5. 🛡️ GATE 2: Per-Subscriber Concurrency Semaphore & GATE 3: Dispatcher
        semaphore = _subscriber_semaphores[subscription.id]
        async with semaphore:
            next_attempt_number = old_attempt.attempt_number + 1
            logger.info(
                "🚀 Dispatched concurrent retry for Event %s -> %s (Attempt %d/%d)",
                event.id,
                subscription.target_url,
                next_attempt_number,
                settings.MAX_RETRY_ATTEMPTS,
            )
            await deliver_to_subscription(
                event=event,
                subscription=subscription,
                db=db,
                attempt_number=next_attempt_number,
            )

        return True


async def process_due_retries(batch_size: Optional[int] = None) -> int:
    """
    Executes a high-throughput sweep of the database to find and redeliver due retries.
    Uses asyncio.gather() to fire deliveries concurrently with per-subscriber rate limiting.
    """
    if batch_size is None:
        batch_size = settings.WORKER_BATCH_SIZE

    async with AsyncSessionLocal() as db:
        now_utc = datetime.now(timezone.utc)

        # Query ripe retries ordered by scheduled time (oldest due first)
        query = (
            select(DeliveryAttempt.id)
            .where(
                DeliveryAttempt.status == DeliveryStatus.RETRYING,
                DeliveryAttempt.next_retry_at <= now_utc,
            )
            .order_by(DeliveryAttempt.next_retry_at.asc())
            .limit(batch_size)
        )
        result = await db.execute(query)
        due_ids: List[uuid.UUID] = list(result.scalars().all())

    if not due_ids:
        return 0

    logger.info("Found %d due retry deliveries to process.", len(due_ids))

    # 🚀 Fire all retries concurrently with individual isolated sessions & rate limiters
    tasks = [_dispatch_single_retry(attempt_id) for attempt_id in due_ids]
    await asyncio.gather(*tasks, return_exceptions=True)

    return len(due_ids)


async def retry_worker_loop(poll_interval: Optional[float] = None) -> None:
    """
    Continuous background loop that polls PostgreSQL for overdue retries.
    Uses the 'Greedy Drain' pattern:
    - If a full batch (50) was processed, loops immediately without sleeping!
    - Only sleeps when the backlog is empty.
    """
    if poll_interval is None:
        poll_interval = settings.WORKER_POLL_INTERVAL_SECONDS

    logger.info("🔄 RelayHook Retry Worker started (polling every %ss)...", poll_interval)
    while not _stop_event.is_set():
        try:
            processed = await process_due_retries()
            if processed > 0:
                logger.info("⚡ Processed batch of %d retried webhooks.", processed)

            # 🏎️ THE GREEDY CHECK:
            # If we processed a full batch, more orders are waiting in PostgreSQL!
            # Skip sleep and immediately grab the next batch!
            if processed >= settings.WORKER_BATCH_SIZE:
                continue

        except Exception as exc:
            logger.error("⚠️ Error in retry worker tick: %s", exc, exc_info=True)

        # Only sleep if queue is caught up / empty, or wake up instantly on shutdown
        try:
            await asyncio.wait_for(_stop_event.wait(), timeout=poll_interval)
        except asyncio.TimeoutError:
            pass

    logger.info("🛑 RelayHook Retry Worker stopped gracefully.")


def start_retry_worker() -> asyncio.Task:
    """Spawns the retry worker loop as a background asyncio task."""
    global _worker_task
    _stop_event.clear()
    _worker_task = asyncio.create_task(retry_worker_loop())
    return _worker_task


async def stop_retry_worker() -> None:
    """Signals the retry worker to stop and awaits its graceful shutdown."""
    global _worker_task
    _stop_event.set()
    if _worker_task and not _worker_task.done():
        try:
            await asyncio.wait_for(_worker_task, timeout=5.0)
        except asyncio.TimeoutError:
            _worker_task.cancel()
        _worker_task = None
