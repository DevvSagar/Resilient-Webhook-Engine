import asyncio
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import FastAPI, Request, Response
import uvicorn
from sqlalchemy import select

from app.core.database import AsyncSessionLocal
from app.core.redis import init_redis, close_redis
from app.models.delivery import DeliveryAttempt, DeliveryStatus
from app.models.event import Event
from app.models.subscription import Subscription
from app.services.rate_limiter import check_rate_limit
from app.services.retry_worker import process_due_retries

# Dummy receiver app on port 8008
mock_app = FastAPI()
received_orders = []


@mock_app.post("/receiver")
async def dummy_receiver(request: Request):
    data = await request.json()
    received_orders.append(data)
    return {"status": "ok", "received": True}


async def main():
    # 1. Start mock server in background
    config = uvicorn.Config(mock_app, host="127.0.0.1", port=8008, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    await asyncio.sleep(0.5)

    redis = await init_redis()

    print("=================================================================")
    print("⚡ TESTING STEP 3: WORKER CONCURRENCY + RATE LIMITING GOVERNOR")
    print("=================================================================\n")

    async with AsyncSessionLocal() as db:
        now_utc = datetime.now(timezone.utc)

        # 1. Create 2 Subscriptions: Domino's and Starbucks
        dominos_sub = Subscription(
            target_url="http://127.0.0.1:8008/receiver",
            event_types=["order.pizza"],
            is_active=True,
        )
        starbucks_sub = Subscription(
            target_url="http://127.0.0.1:8008/receiver",
            event_types=["order.coffee"],
            is_active=True,
        )
        db.add_all([dominos_sub, starbucks_sub])
        await db.commit()

        # 2. Create 2 Events
        dominos_evt = Event(
            idempotency_key=f"dom_{uuid.uuid4()}",
            event_type="order.pizza",
            payload={"item": "Pepperoni Pizza"},
        )
        starbucks_evt = Event(
            idempotency_key=f"sbux_{uuid.uuid4()}",
            event_type="order.coffee",
            payload={"item": "Caramel Macchiato"},
        )
        db.add_all([dominos_evt, starbucks_evt])
        await db.commit()

        # 3. Create Overdue RETRYING Attempts (Scheduled in the past)
        past_time = now_utc - timedelta(seconds=10)
        dominos_attempt = DeliveryAttempt(
            event_id=dominos_evt.id,
            subscription_id=dominos_sub.id,
            status=DeliveryStatus.RETRYING,
            attempt_number=1,
            next_retry_at=past_time,
        )
        starbucks_attempt = DeliveryAttempt(
            event_id=starbucks_evt.id,
            subscription_id=starbucks_sub.id,
            status=DeliveryStatus.RETRYING,
            attempt_number=1,
            next_retry_at=past_time,
        )
        db.add_all([dominos_attempt, starbucks_attempt])
        await db.commit()
        await db.refresh(dominos_attempt)
        await db.refresh(starbucks_attempt)

    print("📝 Step 1: Created overdue retries for Domino's & Starbucks.")

    # 4. ARTIFICIALLY DRAIN Domino's Token Bucket (Simulate Domino's being busy/overloaded)
    # Drain Domino's tokens to 0
    await redis.delete(f"ratelimit:sub:{dominos_sub.id}")
    for _ in range(10):
        await check_rate_limit(redis, dominos_sub.id, capacity=10, refill_rate=1.0)

    print("🛑 Step 2: Drained Domino's token bucket to 0 (Domino's is busy). Starbucks bucket is untouched.\n")

    # 5. Run Worker's process_due_retries()
    print("🏃 Step 3: Running Worker process_due_retries()...")
    processed = await process_due_retries(batch_size=50)
    print(f"   Worker processed {processed} due tasks in parallel!\n")

    # 6. Verify Results in PostgreSQL
    async with AsyncSessionLocal() as db:
        # Check Domino's attempt
        res_d = await db.execute(select(DeliveryAttempt).where(DeliveryAttempt.id == dominos_attempt.id))
        d_att = res_d.scalar_one()

        # Check Starbucks attempt
        res_s = await db.execute(select(DeliveryAttempt).where(DeliveryAttempt.id == starbucks_attempt.id))
        s_att = res_s.scalar_one()

        print("📊 Step 4: Inspecting PostgreSQL States:")
        print(f"   ☕ Starbucks: Old Attempt retired (next_retry_at={s_att.next_retry_at})")
        print(f"   🍕 Domino's:  Postponed by Rate Limiter! (next_retry_at={d_att.next_retry_at})")
        print(f"                 Reason: {d_att.error_message}")

        assert s_att.next_retry_at is None, "Starbucks should have been dispatched and retired!"
        assert d_att.next_retry_at is not None, "Domino's should have been postponed due to rate limiting!"
        assert d_att.next_retry_at > now_utc, "Domino's retry must be scheduled in the future!"
        print("\n   ✅ SUCCESS: Starbucks proceeded while Domino's was safely protected without blocking the system!")

    # Cleanup
    server.should_exit = True
    await server_task
    await close_redis()

    print("\n=================================================================")
    print("🎉 STEP 3 VERIFIED! CONCURRENCY + RATE LIMITER WORKING TOGETHER!")
    print("=================================================================")


if __name__ == "__main__":
    asyncio.run(main())
