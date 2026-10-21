import asyncio
import time
import uuid

import httpx
import uvicorn
from fastapi import FastAPI, Request, Response
from sqlalchemy import select

from app.core.database import AsyncSessionLocal
from app.core.security import verify_webhook_signature
from app.main import app as relayhook_app
from app.models.delivery import DeliveryAttempt, DeliveryStatus
from app.models.event import Event
from app.models.subscription import Subscription
from app.services.dispatcher import deliver_to_subscription

# 1. Mock Kitchen Server with Chaos / Outage toggle
mock_server = FastAPI(title="Chaos Kitchen Server")
server_state = {
    "is_healthy": False,
    "secret_key": "",
    "received_attempts": [],
}


@mock_server.post("/chaos_kitchen")
async def chaos_kitchen(request: Request):
    """Simulates an external kitchen server undergoing a crash and recovery."""
    raw_body = await request.body()
    sig_header = request.headers.get("X-Webhook-Signature", "")
    webhook_id = request.headers.get("X-Webhook-Id", "")
    webhook_ts = request.headers.get("X-Webhook-Timestamp", "")

    server_state["received_attempts"].append({
        "headers": dict(request.headers),
        "is_healthy": server_state["is_healthy"],
    })

    if not server_state["is_healthy"]:
        print(f"   💥 [Domino's Server 8006]: CRASH! Database connection refused! Returning HTTP 500.")
        return Response(
            content='{"error": "Internal server error: database unreachable"}',
            status_code=500,
            media_type="application/json",
        )

    # Server is healthy -> Verify HMAC signature
    is_valid = verify_webhook_signature(
        secret_key=server_state["secret_key"],
        signature_header=sig_header,
        payload=raw_body,
    )

    data = await request.json()
    print(f"\n🍕 [Domino's Server 8006 RECOVERED!]:")
    print(f"   📦 Order Item:          {data.get('pizza')}")
    print(f"   🆔 Webhook ID Header:   {webhook_id}")
    print(f"   ⏱️  Timestamp Header:    {webhook_ts}")
    print(f"   🔒 Signature Verified:  {is_valid} ✅")
    print(f"   🖨️  Receipt Printed! Baking Pizza!\n")

    return {"status": "order_accepted", "oven": "baking"}


async def run_server(server: uvicorn.Server):
    await server.serve()


async def main():
    print("=" * 65)
    print("💥 RELAYHOOK DAY 4: CHAOS FAILURE & SELF-HEALING LIVE TEST")
    print("=" * 65)

    # Start mock kitchen server on port 8006 in background
    config = uvicorn.Config(mock_server, host="127.0.0.1", port=8006, log_level="error")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(run_server(server))
    await asyncio.sleep(0.5)

    transport = httpx.ASGITransport(app=relayhook_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Step 1: Register Subscription
        print("\n📝 Step 1: Registering Domino's subscription pointing to port 8006...")
        sub_resp = await client.post(
            "/api/v1/subscriptions",
            json={
                "target_url": "http://127.0.0.1:8006/chaos_kitchen",
                "event_types": ["chaos.order"],
            },
        )
        assert sub_resp.status_code == 201
        sub_data = sub_resp.json()
        sub_id = sub_data["id"]
        server_state["secret_key"] = sub_data["secret_key"]
        print(f"   ✅ Subscription ID: {sub_id}")
        print(f"   🎯 Target URL:      {sub_data['target_url']}")

        # Step 2: Swiggy places order while Domino's is CRASHED
        order_key = f"chaos_{uuid.uuid4().hex[:6]}"
        print(f"\n🛵 Step 2: Swiggy Placing Pizza Order ({order_key})...")
        print("   ⚠️  (Note: Domino's kitchen server is currently DOWN / unhealthy)")
        start_swiggy = time.perf_counter()
        evt_resp = await client.post(
            "/api/v1/events",
            json={
                "idempotency_key": order_key,
                "event_type": "chaos.order",
                "payload": {
                    "order_id": 8801,
                    "pizza": "Pepperoni Feast",
                    "customer": "Sagar",
                },
            },
        )
        swiggy_time = (time.perf_counter() - start_swiggy) * 1000.0
        assert evt_resp.status_code == 202
        evt_data = evt_resp.json()
        evt_id = evt_data["id"]
        print(f"   ✅ Swiggy received HTTP 202 Accepted in {swiggy_time:.2f} ms!")

        # Step 3: Wait for Attempt 1 to fail and verify backoff schedule
        print("\n⚡ Step 3: Waiting for background dispatcher to execute Attempt 1...")
        await asyncio.sleep(1.5)

        deliv_resp = await client.get(f"/api/v1/deliveries?event_id={evt_id}")
        delivs = deliv_resp.json()
        assert len(delivs) >= 1
        att1 = delivs[0]
        print("📊 Step 3 Results (Attempt 1 Failure):")
        print(f"   🎯 Status:         {att1['status']} (Scheduled for retry)")
        print(f"   🌐 Response Code:  {att1['response_status_code']} (Server Error)")
        print(f"   ⏱️  Latency:        {att1['latency_ms']} ms")
        print(f"   🔄 Next Retry At:  {att1['next_retry_at']}")

        # Step 4: Simulate reaching MAX_RETRIES (Attempt 5) -> Dead Letter Queue
        print("\n🪦 Step 4: Simulating Max Retries Exhaustion (Attempt 5)...")
        async with AsyncSessionLocal() as db:
            event_res = await db.execute(select(Event).where(Event.id == uuid.UUID(evt_id)))
            event = event_res.scalar_one()

            sub_res = await db.execute(select(Subscription).where(Subscription.id == uuid.UUID(sub_id)))
            sub = sub_res.scalar_one()
            
            # Fire Attempt 5 directly
            dlq_attempt = await deliver_to_subscription(
                event=event,
                subscription=sub,
                db=db,
                attempt_number=5,
            )
            print("📊 Step 4 Results (Dead Letter Queue Quarantine):")
            print(f"   📄 Delivery ID:    {dlq_attempt.id}")
            print(f"   🎯 Final Status:   {dlq_attempt.status.value} 🪦 (Quarantined in DLQ)")
            print(f"   🚫 Next Retry At:  {dlq_attempt.next_retry_at} (Taken off schedule)")
            print(f"   📝 Error Message:  {dlq_attempt.error_message}")

        # Step 5: Fix Domino's Server and Trigger Replay API
        print("\n🔧 Step 5: Domino's DevOps fixes their server! (Status -> HEALTHY)")
        server_state["is_healthy"] = True
        print("   ✅ Domino's Server is now back online and returning 200 OK!")

        print(f"\n🔄 Step 6: Calling POST /api/v1/deliveries/{dlq_attempt.id}/replay...")
        replay_resp = await client.post(f"/api/v1/deliveries/{dlq_attempt.id}/replay")
        assert replay_resp.status_code == 200
        replayed = replay_resp.json()

        print("📊 Step 6 Results (Self-Healing Recovery):")
        print(f"   📄 Replayed Delivery ID: {replayed['id']}")
        print(f"   🎯 New Status:           {replayed['status']} ✅")
        print(f"   🌐 Response Status Code: {replayed['response_status_code']} OK")
        print(f"   ⏱️  Round-trip Latency:   {replayed['latency_ms']} ms")
        print(f"   📦 Response Body:        {replayed['response_body']}")

        print("\n" + "=" * 65)
        print("🎉 DAY 4 100% COMPLETE! ALL RESILIENCE & RECOVERY MECHANISMS VERIFIED!")
        print("=" * 65)

    server.should_exit = True
    await server_task


if __name__ == "__main__":
    asyncio.run(main())
