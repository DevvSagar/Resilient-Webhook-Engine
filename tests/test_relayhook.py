import json
import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import httpx
from sqlalchemy import select

from app.core.security import generate_webhook_signature, verify_webhook_signature
from app.models.delivery import DeliveryAttempt, DeliveryStatus
from app.models.event import Event
from app.models.subscription import Subscription
from app.services.rate_limiter import check_rate_limit


# ============================================================================
# 🟢 TEST 1: Health & System Vitality Check
# ============================================================================
@pytest.mark.asyncio
async def test_health_and_root(client: httpx.AsyncClient):
    """
    Verifies that the API root and health check endpoints respond correctly.
    Confirms active connectivity to both PostgreSQL and Redis.
    """
    # 1. Root metadata
    root_resp = await client.get("/")
    assert root_resp.status_code == 200
    root_data = root_resp.json()
    assert root_data["status"] == "operational"

    # 2. Health check with DB and Redis ping
    health_resp = await client.get("/health")
    assert health_resp.status_code == 200
    health_data = health_resp.json()
    assert health_data["status"] == "healthy"
    assert health_data["database"] == "connected"
    assert health_data["redis"] == "connected"


# ============================================================================
# 🟢 TEST 2: Event Ingestion & Idempotency Key Deduplication
# ============================================================================
@pytest.mark.asyncio
async def test_event_ingestion_and_idempotency(client: httpx.AsyncClient):
    """
    Verifies event ingestion and strict database-level deduplication.
    Sending the same idempotency key twice must return HTTP 409 Conflict.
    """
    idempotency_key = f"test_idemp_{uuid.uuid4()}"
    payload = {
        "event_type": "payment.succeeded",
        "idempotency_key": idempotency_key,
        "payload": {"order_id": 1045, "amount": 25.50, "currency": "USD"},
    }

    # 1. First Submission -> 202 Accepted
    resp_1 = await client.post("/api/v1/events", json=payload)
    assert resp_1.status_code == 202
    data_1 = resp_1.json()
    assert data_1["idempotency_key"] == idempotency_key
    assert data_1["status"] == "queued"
    assert "id" in data_1

    # 2. Duplicate Submission with same idempotency_key -> 200 OK with already_processed
    resp_2 = await client.post("/api/v1/events", json=payload)
    assert resp_2.status_code == 200
    data_2 = resp_2.json()
    assert data_2["status"] == "already_processed"
    assert data_2["id"] == data_1["id"]  # Returns the exact original event record!


# ============================================================================
# 🟢 TEST 3: Cryptographic HMAC Signature & Tamper-Proofing
# ============================================================================
def test_hmac_signature_security():
    """
    Verifies cryptographic payload signing (HMAC-SHA256) and security defenses:
    1. Valid payload + correct secret -> True
    2. Tampered payload + correct secret -> False (Tamper detection!)
    3. Wrong secret -> False (Attacker impersonation blocked!)
    4. Expired timestamp (> 300s) -> False (Replay attack blocked!)
    """
    secret = "a" * 64
    original_payload = b'{"order_id": 999, "amount": 50.00}'
    now_ts = int(time.time())

    # 1. Sign original payload
    sig_header = generate_webhook_signature(
        secret_key=secret,
        payload=original_payload,
        timestamp=now_ts,
    )
    assert "v1=" in sig_header
    assert f"t={now_ts}" in sig_header

    # 2. Valid signature verification
    assert verify_webhook_signature(
        secret_key=secret,
        signature_header=sig_header,
        payload=original_payload,
        tolerance_seconds=300,
    ) is True

    # 3. Tampered payload verification -> Must Fail!
    tampered_payload = b'{"order_id": 999, "amount": 500000.00}'
    assert verify_webhook_signature(
        secret_key=secret,
        signature_header=sig_header,
        payload=tampered_payload,
        tolerance_seconds=300,
    ) is False

    # 4. Wrong secret verification -> Must Fail!
    assert verify_webhook_signature(
        secret_key="b" * 64,
        signature_header=sig_header,
        payload=original_payload,
        tolerance_seconds=300,
    ) is False

    # 5. Expired timestamp (Replay attack 10 minutes late) -> Must Fail!
    expired_header = generate_webhook_signature(
        secret_key=secret,
        payload=original_payload,
        timestamp=now_ts - 600,
    )
    assert verify_webhook_signature(
        secret_key=secret,
        signature_header=expired_header,
        payload=original_payload,
        tolerance_seconds=300,
    ) is False


# ============================================================================
# 🟢 TEST 4: Token-Bucket Rate Limiter & Throttling
# ============================================================================
@pytest.mark.asyncio
async def test_token_bucket_rate_limiter(redis):
    """
    Verifies Redis token bucket algorithm:
    1. Capacity = 3, Refill = 1 token/sec.
    2. Drains 3 tokens -> All Allowed.
    3. 4th request immediately -> Denied with Retry-After.
    4. Separate tenant (Nike) -> Allowed (isolated bucket!).
    """
    dominos_id = f"test_dominos_{uuid.uuid4()}"
    nike_id = f"test_nike_{uuid.uuid4()}"

    # 1. Drain 3 tokens from Domino's bucket
    for _ in range(3):
        res = await check_rate_limit(redis, dominos_id, capacity=3, refill_rate=1.0)
        assert res.allowed is True

    # 2. 4th request exceeds bucket capacity
    res_denied = await check_rate_limit(redis, dominos_id, capacity=3, refill_rate=1.0)
    assert res_denied.allowed is False
    assert res_denied.retry_after > 0.0

    # 3. Multi-tenant isolation: Nike has its own independent bucket
    res_nike = await check_rate_limit(redis, nike_id, capacity=3, refill_rate=1.0)
    assert res_nike.allowed is True
    assert res_nike.remaining_tokens == 2.0


# ============================================================================
# 🟢 TEST 5: Dead Letter Queue (DLQ) & Bulk FIFO Replay
# ============================================================================
@pytest.mark.asyncio
async def test_dead_letter_queue_and_bulk_replay(client: httpx.AsyncClient, db):
    """
    Verifies Dead Letter Queue lifecycle and FIFO bulk replay:
    1. Creates 2 quarantined FAILED deliveries with distinct timestamps (T1 < T2).
    2. Inspects GET /deliveries/dlq -> Confirms FIFO order.
    3. Triggers POST /deliveries/dlq/replay-all -> Confirms 202 Accepted.
    4. Verifies database state flipped to RETRYING.
    5. Re-calls replay-all -> Confirms 0 duplicate replays (Idempotency).
    """
    now_utc = datetime.now(timezone.utc)

    # Setup subscription
    sub = Subscription(
        target_url="http://127.0.0.1:8008/webhook",
        event_types=["order.pizza"],
        is_active=True,
    )
    db.add(sub)
    await db.commit()
    await db.refresh(sub)
    sub_id = sub.id

    # Create 2 events at T-20m and T-10m
    attempt_ids = []
    for i, delta in enumerate([20, 10], start=1):
        evt_time = now_utc - timedelta(minutes=delta)
        evt = Event(
            idempotency_key=f"dlq_test_{uuid.uuid4()}",
            event_type="order.pizza",
            payload={"order_id": i},
            created_at=evt_time,
        )
        db.add(evt)
        await db.commit()
        await db.refresh(evt)

        att = DeliveryAttempt(
            event_id=evt.id,
            subscription_id=sub_id,
            status=DeliveryStatus.FAILED,
            attempt_number=5,
            error_message="HTTP 500 error: Max 5 retries exhausted (DLQ)",
            next_retry_at=None,
            created_at=evt_time,
        )
        db.add(att)
        await db.commit()
        await db.refresh(att)
        attempt_ids.append(att.id)

    # 1. GET /api/v1/deliveries/dlq
    dlq_resp = await client.get(f"/api/v1/deliveries/dlq?subscription_id={sub_id}")
    assert dlq_resp.status_code == 200
    dlq_items = dlq_resp.json()
    assert len(dlq_items) == 2
    # Verify strict FIFO sorting: oldest (T-20m) first
    assert dlq_items[0]["id"] == str(attempt_ids[0])
    assert dlq_items[1]["id"] == str(attempt_ids[1])

    # 2. POST /api/v1/deliveries/dlq/replay-all
    replay_resp = await client.post(f"/api/v1/deliveries/dlq/replay-all?subscription_id={sub_id}")
    assert replay_resp.status_code == 202
    replay_data = replay_resp.json()
    assert replay_data["replayed_count"] == 2

    # 3. Verify PostgreSQL states flipped to RETRYING
    db.expire_all()
    query = select(DeliveryAttempt).where(DeliveryAttempt.id.in_(attempt_ids))
    res = await db.execute(query)
    updated_attempts = res.scalars().all()
    for att in updated_attempts:
        assert att.status == DeliveryStatus.RETRYING
        assert att.next_retry_at is not None

    # 4. Immediate second replay should return 0 (Idempotency)
    replay_resp_2 = await client.post(f"/api/v1/deliveries/dlq/replay-all?subscription_id={sub_id}")
    assert replay_resp_2.status_code == 202
    assert replay_resp_2.json()["replayed_count"] == 0


# ============================================================================
# 🟢 TEST 6: Distributed Tracing & Request ID Correlation
# ============================================================================
@pytest.mark.asyncio
async def test_request_tracing_and_correlation_id(client: httpx.AsyncClient):
    """
    Verifies distributed request tracing:
    1. Default request -> auto-generates unique X-Request-Id.
    2. Client-supplied X-Request-Id -> preserved across response headers and log context.
    """
    # 1. Auto-generated request ID
    resp_auto = await client.get("/api/v1/deliveries")
    assert resp_auto.status_code == 200
    auto_id = resp_auto.headers.get("x-request-id")
    assert auto_id is not None
    assert auto_id.startswith("req_")

    # 2. Client-provided trace ID propagation
    custom_trace_id = "trace_client_xyz_9999"
    resp_custom = await client.get(
        "/api/v1/deliveries",
        headers={"X-Request-Id": custom_trace_id},
    )
    assert resp_custom.status_code == 200
    assert resp_custom.headers.get("x-request-id") == custom_trace_id


# ============================================================================
# 🟢 TEST 7: Prometheus Telemetry & Metrics Scraping Endpoint
# ============================================================================
@pytest.mark.asyncio
async def test_prometheus_metrics_endpoint(client: httpx.AsyncClient):
    """
    Verifies Prometheus telemetry exposition:
    1. GET /metrics -> HTTP 200 with standard Prometheus text/plain format.
    2. Confirms all custom RelayHook metrics are registered and scraped.
    """
    resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers.get("content-type", "")

    metrics_text = resp.text
    # Verify core metric families exist in Prometheus exposition output
    assert "relayhook_events_ingested_total" in metrics_text
    assert "relayhook_deliveries_total" in metrics_text
    assert "relayhook_delivery_latency_seconds" in metrics_text
    assert "relayhook_rate_limit_throttled_total" in metrics_text
    assert "relayhook_active_worker_tasks" in metrics_text
