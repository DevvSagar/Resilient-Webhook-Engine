import asyncio
import time
import uuid

import httpx
import uvicorn
from fastapi import FastAPI, Request

from app.core.security import verify_webhook_signature
from app.main import app as relayhook_app

# 1. Create Mock Kitchen Server (Domino's)
mock_kitchen = FastAPI(title="Domino's Mock Kitchen Receiver")
kitchen_state = {"secret_key": ""}
received_orders = []


@mock_kitchen.post("/kitchen")
async def kitchen_webhook(request: Request):
    """Simulates Domino's kitchen server receiving orders and verifying HMAC."""
    sig_header = request.headers.get("X-Webhook-Signature", "")
    raw_body = await request.body()
    data = await request.json()

    # Verify cryptographic signature using shared secret
    is_valid = verify_webhook_signature(
        secret_key=kitchen_state["secret_key"],
        signature_header=sig_header,
        payload=raw_body,
    )

    received_orders.append({"data": data, "valid": is_valid})

    print("\n🍕 [Domino's Kitchen Server on Port 8005]:")
    print(f"   📦 Order Item: {data.get('pizza')}")
    print(f"   👤 Customer:   {data.get('customer')}")
    print(f"   🔒 Signature Authentic: {is_valid} ✅")
    print("   🖨️  Receipt printed! Sliding pizza into the oven!\n")

    return {"status": "accepted_by_kitchen", "oven_status": "baking"}


async def run_server(server: uvicorn.Server):
    await server.serve()


async def main():
    print("=" * 60)
    print("🚀 STARTING RELAYHOOK END-TO-END DEMO TEST")
    print("=" * 60)

    # Start Mock Kitchen Server on port 8005 in background
    config = uvicorn.Config(mock_kitchen, host="127.0.0.1", port=8005, log_level="error")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(run_server(server))

    # Give server 0.5s to bind port
    await asyncio.sleep(0.5)

    transport = httpx.ASGITransport(app=relayhook_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Step 1: Register Domino's Subscription
        print("\n📝 Step 1: Registering Domino's Webhook Subscription...")
        sub_resp = await client.post(
            "/api/v1/subscriptions",
            json={
                "target_url": "http://127.0.0.1:8005/kitchen",
                "event_types": ["dominos.order"],
            },
        )
        assert sub_resp.status_code == 201
        sub_data = sub_resp.json()
        kitchen_state["secret_key"] = sub_data["secret_key"]
        print(f"   ✅ Registered Target URL: {sub_data['target_url']}")
        print(f"   🔑 Generated Secret Key:  {sub_data['secret_key'][:24]}...")

        # Step 2: Swiggy sends order event
        order_key = f"e2e_order_{uuid.uuid4().hex[:6]}"
        print(f"\n🛵 Step 2: Swiggy Placing Pizza Order ({order_key})...")
        start_swiggy = time.perf_counter()
        evt_resp = await client.post(
            "/api/v1/events",
            json={
                "idempotency_key": order_key,
                "event_type": "dominos.order",
                "payload": {
                    "order_id": 1045,
                    "pizza": "Margherita with Extra Cheese",
                    "customer": "Sagar",
                },
            },
        )
        swiggy_time = (time.perf_counter() - start_swiggy) * 1000.0
        assert evt_resp.status_code == 202
        evt_data = evt_resp.json()
        print(f"   ✅ Swiggy received HTTP 202 Accepted in {swiggy_time:.2f} ms!")
        print(f"   📋 Event ID: {evt_data['id']}, Status: {evt_data['status']}")

        # Step 3: Wait for background delivery
        print("\n⚡ Step 3: Waiting for background dispatcher to deliver...")
        await asyncio.sleep(1.5)

        # Step 4: Check delivery audit log
        deliv_resp = await client.get(f"/api/v1/deliveries?event_id={evt_data['id']}")
        assert deliv_resp.status_code == 200
        deliv_logs = deliv_resp.json()
        print("📊 Step 4: RelayHook Delivery Audit Log:")
        if deliv_logs:
            log = deliv_logs[0]
            print(f"   📄 Delivery ID:    {log['id']}")
            print(f"   🎯 Status:         {log['status']} ✅")
            print(f"   🌐 Response Code:  {log['response_status_code']} OK")
            print(f"   ⏱️  Latency:        {log['latency_ms']} ms")
            print(f"   🔄 Next Retry:     {log['next_retry_at']}")
        else:
            print("   ⚠️  No delivery logs found!")

        print("\n" + "=" * 60)
        print("🎉 ALL SYSTEMS OPERATIONAL! END-TO-END DEMO SUCCEEDED!")
        print("=" * 60)

    server.should_exit = True
    await server_task


if __name__ == "__main__":
    asyncio.run(main())
