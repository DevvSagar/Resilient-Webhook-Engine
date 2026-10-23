import time
from contextlib import contextmanager
from typing import Generator

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from starlette.responses import Response

# ============================================================================
# 📊 PROMETHEUS METRIC DEFINITIONS
# ============================================================================

# 1. Total events ingested by RelayHook (classified by topic & intake status)
EVENTS_INGESTED_TOTAL = Counter(
    "relayhook_events_ingested_total",
    "Total incoming webhook events ingested into RelayHook",
    ["event_type", "status"],
)

# 2. Total delivery attempts executed across all subscriber endpoints
DELIVERIES_TOTAL = Counter(
    "relayhook_deliveries_total",
    "Total webhook HTTP delivery attempts dispatched to subscribers",
    ["status"],
)

# 3. Webhook round-trip dispatch latency histogram (in seconds)
# Buckets configured to capture fast microsecond responses up to timeout ceilings
DELIVERY_LATENCY_SECONDS = Histogram(
    "relayhook_delivery_latency_seconds",
    "Round-trip HTTP delivery latency in seconds from dispatch to response",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# 4. Total rate-limiting throttles enforced by Redis Token Bucket
RATE_LIMIT_THROTTLED_TOTAL = Counter(
    "relayhook_rate_limit_throttled_total",
    "Total webhook dispatches deferred due to token-bucket rate limits",
    ["subscription_id"],
)

# 5. Current active in-flight concurrent delivery tasks
ACTIVE_WORKER_TASKS = Gauge(
    "relayhook_active_worker_tasks",
    "Number of webhook delivery HTTP requests currently in-flight across workers",
)


# ============================================================================
# 🛠️ CONVENIENCE HELPERS
# ============================================================================

def record_event_ingestion(event_type: str, status: str) -> None:
    """Increments the ingested events counter with event topic and intake status."""
    EVENTS_INGESTED_TOTAL.labels(event_type=event_type, status=status).inc()


def record_delivery_attempt(status: str, latency_seconds: float) -> None:
    """Records delivery completion status and observes round-trip latency."""
    DELIVERIES_TOTAL.labels(status=status).inc()
    if latency_seconds > 0:
        DELIVERY_LATENCY_SECONDS.observe(latency_seconds)


def record_rate_limit_throttle(subscription_id: str) -> None:
    """Increments the throttle counter when a subscriber bucket is empty."""
    RATE_LIMIT_THROTTLED_TOTAL.labels(subscription_id=str(subscription_id)).inc()


@contextmanager
def track_in_flight_delivery() -> Generator[None, None, None]:
    """Context manager tracking active concurrent delivery coroutines."""
    ACTIVE_WORKER_TASKS.inc()
    try:
        yield
    finally:
        ACTIVE_WORKER_TASKS.dec()


def get_metrics_response() -> Response:
    """
    Renders Prometheus exposition format metrics for scraper ingestion.
    Used by the /metrics endpoint.
    """
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)
