import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import select

from app.core.database import AsyncSessionLocal
from app.main import app
from app.models.delivery import DeliveryAttempt, DeliveryStatus
from app.models.event import Event
from app.models.subscription import Subscription


async def main():
    print("=================================================================")
    print("🚢 TESTING STEP 4: BULK DLQ FIFO REPLAY ENDPOINT")
    print("=================================================================\n")

    now_utc = datetime.now(timezone.utc)
    dominos_sub_id = None
    attempt_ids = []

    # 1. Create 3 Quarantined (FAILED) deliveries in PostgreSQL
    async with AsyncSessionLocal() as db:
        sub = Subscription(
            target_url="http://127.0.0.1:8008/kitchen",
            event_types=["order.pizza"],
            is_active=True,
        )
        db.add(sub)
        await db.commit()
        await db.refresh(sub)
        dominos_sub_id = sub.id

        # 3 Events created at T-30m, T-20m, T-10m
        for i, delta in enumerate([30, 20, 10], start=1):
            evt_time = now_utc - timedelta(minutes=delta)
            evt = Event(
                idempotency_key=f"bulk_test_{uuid.uuid4()}",
                event_type="order.pizza",
                payload={"order_num": i, "pizza": f"Pizza #{i}"},
                created_at=evt_time,
            )
            db.add(evt)
            await db.commit()
            await db.refresh(evt)

            attempt = DeliveryAttempt(
                event_id=evt.id,
                subscription_id=sub.id,
                status=DeliveryStatus.FAILED,
                attempt_number=5,
                error_message="Destination server error (Max 5 retries exhausted. Moved to Dead Letter Queue)",
                next_retry_at=None,
                created_at=evt_time,
            )
            db.add(attempt)
            await db.commit()
            await db.refresh(attempt)
            attempt_ids.append(attempt.id)

    print(f"📝 Step 1: Created 3 Quarantined FAILED orders for Domino's ({dominos_sub_id}).")

    # 2. Test GET /deliveries/dlq
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        print("\n🔍 Step 2: Calling GET /api/v1/deliveries/dlq...")
        get_res = await client.get(f"/api/v1/deliveries/dlq?subscription_id={dominos_sub_id}")
        assert get_res.status_code == 200, f"Expected 200, got {get_res.status_code}"
        dlq_list = get_res.json()
        print(f"   Found {len(dlq_list)} quarantined deliveries in DLQ.")
        assert len(dlq_list) == 3, "Expected 3 orders in DLQ!"
        
        # Verify strict FIFO chronological ordering
        times = [item["created_at"] for item in dlq_list]
        print(f"   Chronological timestamps: {times}")
        assert times == sorted(times), "Orders in DLQ must be sorted in strict FIFO order (created_at ASC)!"
        print("   ✅ Verified: Strict FIFO ordering confirmed!")

        # 3. Test POST /deliveries/dlq/replay-all
        print("\n🚀 Step 3: Triggering 1-Click POST /api/v1/deliveries/dlq/replay-all...")
        replay_res = await client.post(f"/api/v1/deliveries/dlq/replay-all?subscription_id={dominos_sub_id}")
        assert replay_res.status_code == 202, f"Expected 202 Accepted, got {replay_res.status_code}"
        replay_data = replay_res.json()
        print(f"   Response 202 Accepted: {replay_data}")
        assert replay_data["replayed_count"] == 3, "Should have queued 3 deliveries!"

    # 4. Inspect PostgreSQL: Verify states were flipped back onto the conveyor belt
    async with AsyncSessionLocal() as db:
        res = await db.execute(select(DeliveryAttempt).where(DeliveryAttempt.id.in_(attempt_ids)))
        updated_attempts = res.scalars().all()
        print("\n📊 Step 4: Inspecting PostgreSQL States:")
        for att in updated_attempts:
            print(f"   Delivery {att.id}: Status={att.status.value} | next_retry_at={att.next_retry_at}")
            assert att.status == DeliveryStatus.RETRYING, "Status should be flipped to RETRYING!"
            assert att.next_retry_at is not None, "next_retry_at must be populated!"
        print("   ✅ Verified: All 3 orders successfully flipped to RETRYING with next_retry_at = NOW()!")

    # 5. Call replay-all a second time (Should be idempotent: 0 replayed)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        print("\n🔁 Step 5: Testing Idempotency (Calling replay-all again immediately)...")
        replay_res2 = await client.post(f"/api/v1/deliveries/dlq/replay-all?subscription_id={dominos_sub_id}")
        assert replay_res2.status_code == 202
        replay_data2 = replay_res2.json()
        print(f"   Second Replay Response: {replay_data2}")
        assert replay_data2["replayed_count"] == 0, "No failed orders should be left to replay!"
        print("   ✅ Verified: Zero double-replays!")

    print("\n=================================================================")
    print("🎉 STEP 4 VERIFIED! BULK DLQ FIFO REPLAY WORKING FLAWLESSLY!")
    print("=================================================================")


if __name__ == "__main__":
    asyncio.run(main())
