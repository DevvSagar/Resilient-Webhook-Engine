import time
import uuid
from dataclasses import dataclass
from typing import Optional, Union

from redis.asyncio import Redis

from app.core.config import settings

# Atomic Lua Script for Token-Bucket Rate Limiting
# Executed entirely inside Redis memory to eliminate race conditions under high concurrency.
TOKEN_BUCKET_LUA_SCRIPT = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

-- 1. Fetch current bucket state
local data = redis.call("HMGET", key, "tokens", "last_updated")
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if tokens == nil or last_updated == nil then
    -- First request for this subscriber: initialize bucket at full capacity
    tokens = capacity
    last_updated = now
else
    -- Compute elapsed time and add refilled tokens
    local elapsed = math.max(0, now - last_updated)
    tokens = math.min(capacity, tokens + (elapsed * refill_rate))
    last_updated = now
end

-- 2. Evaluate if request is allowed
local allowed = 0
local retry_after = 0

if tokens >= requested then
    -- Spend tokens
    tokens = tokens - requested
    allowed = 1
else
    -- Bucket empty: calculate exact seconds to wait until next token drips in
    local needed = requested - tokens
    retry_after = needed / refill_rate
end

-- 3. Save state and reset 60-second inactivity timer
redis.call("HMSET", key, "tokens", tostring(tokens), "last_updated", tostring(last_updated))
redis.call("EXPIRE", key, ttl)

return {allowed, tostring(tokens), tostring(retry_after)}
"""


@dataclass
class RateLimitResult:
    """Encapsulates the decision and state of a rate limit evaluation."""
    allowed: bool
    remaining_tokens: float
    retry_after: float  # Seconds to wait if allowed is False (0.0 if allowed)


async def check_rate_limit(
    redis: Redis,
    subscription_id: Union[str, uuid.UUID],
    capacity: Optional[int] = None,
    refill_rate: Optional[float] = None,
    requested_tokens: int = 1,
    inactivity_ttl_seconds: int = 60,
) -> RateLimitResult:
    """
    Evaluates whether a webhook dispatch is allowed under the token bucket algorithm.
    Guarantees atomic, race-condition-free execution via an in-memory Redis Lua script.
    """
    if capacity is None:
        capacity = settings.DEFAULT_RATE_LIMIT_CAPACITY
    if refill_rate is None:
        refill_rate = settings.DEFAULT_RATE_LIMIT_REFILL_RATE

    key = f"ratelimit:sub:{str(subscription_id)}"
    now = time.time()

    # Execute atomic Lua script inside Redis
    result = await redis.eval(
        TOKEN_BUCKET_LUA_SCRIPT,
        1,  # Number of keys
        key,
        capacity,
        refill_rate,
        now,
        requested_tokens,
        inactivity_ttl_seconds,
    )

    allowed = bool(result[0] == 1)
    remaining_tokens = round(float(result[1]), 2)
    retry_after = round(float(result[2]), 2)

    return RateLimitResult(
        allowed=allowed,
        remaining_tokens=remaining_tokens,
        retry_after=retry_after,
    )
