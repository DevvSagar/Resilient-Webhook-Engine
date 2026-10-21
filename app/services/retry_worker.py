import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.models.delivery import DeliveryAttempt, DeliveryStatus
from app.models.event import Event
from app.models.subscription import Subscription
from app.services.dispatcher import deliver_to_subscription

logger = logging.getLogger("relayhook.retry_worker")

# Module-level worker state for graceful lifecycle management
_worker_task: Optional[asyncio.Task] = None
_stop_event = asyncio.Event()


async def process_due_retries(batch_size: Optional[int] = None) -> int:
    """
    Executes a single sweep of the database to find and redeliver due retries.
    
    1. Queries up to `batch_size` DeliveryAttempts where:
       status == 'RETRYING' and next_retry_at <= NOW()
    2. Marks each due attempt as processed (clearing next_retry_at to avoid double-processing).
    3. Re-dispatches the webhook with attempt_number = old_attempt + 1.
    4. Returns the number of retried deliveries.
    """
    if batch_size is None:
        batch_size = settings.WORKER_BATCH_SIZE
    async with AsyncSessionLocal() as db:
        now_utc = datetime.now(timezone.utc)
        
        # 1. Query ripe retries ordered by scheduled time (oldest first)
        query = (
            select(DeliveryAttempt)
            .where(
                DeliveryAttempt.status == DeliveryStatus.RETRYING,
                DeliveryAttempt.next_retry_at <= now_utc,
            )
            .order_by(DeliveryAttempt.next_retry_at.asc())
            .limit(batch_size)
        )
        result = await db.execute(query)
        due_attempts = result.scalars().all()

        if not due_attempts:
            return 0

        logger.info("Found %d due retry deliveries to process.", len(due_attempts))

        # 2. Process each due attempt
        retried_count = 0
        for old_attempt in due_attempts:
            # Clear next_retry_at on the old record so it's not picked up again
            old_attempt.next_retry_at = None
            await db.commit()

            # Fetch the parent Event
            event_result = await db.execute(
                select(Event).where(Event.id == old_attempt.event_id)
            )
            event = event_result.scalar_one_or_none()

            # Fetch the parent Subscription
            sub_result = await db.execute(
                select(Subscription).where(Subscription.id == old_attempt.subscription_id)
            )
            subscription = sub_result.scalar_one_or_none()

            # If subscription was deleted or deactivated, quarantine delivery
            if not subscription or not subscription.is_active:
                old_attempt.status = DeliveryStatus.FAILED
                old_attempt.error_message = "Subscription was deactivated or deleted prior to retry."
                await db.commit()
                continue

            if not event:
                old_attempt.status = DeliveryStatus.FAILED
                old_attempt.error_message = "Event record was deleted prior to retry."
                await db.commit()
                continue

            # 3. Fire the new delivery attempt (incrementing attempt_number)
            next_attempt_number = old_attempt.attempt_number + 1
            logger.info(
                "Redelivering event %s to %s (Attempt %d/%d)...",
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
            retried_count += 1

        return retried_count


async def retry_worker_loop(poll_interval: Optional[float] = None) -> None:
    """
    Continuous background loop that polls PostgreSQL for overdue retries.
    Runs every `poll_interval` seconds until `_stop_event` is set.
    """
    if poll_interval is None:
        poll_interval = settings.WORKER_POLL_INTERVAL_SECONDS

    logger.info("🔄 RelayHook Retry Worker started (polling every %ss)...", poll_interval)
    while not _stop_event.is_set():
        try:
            processed = await process_due_retries()
            if processed > 0:
                logger.info("⚡ Successfully dispatched %d retried webhooks.", processed)
        except Exception as exc:
            logger.error("⚠️ Error in retry worker tick: %s", exc, exc_info=True)

        # Wait for poll_interval, or wake up instantly if stop_event is triggered
        try:
            await asyncio.wait_for(_stop_event.wait(), timeout=poll_interval)
        except asyncio.TimeoutError:
            pass  # Normal tick timeout, loop again

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
