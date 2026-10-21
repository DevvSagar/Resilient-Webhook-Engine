from typing import AsyncGenerator, Optional
import redis.asyncio as aioredis
from redis.asyncio import ConnectionPool, Redis

from app.core.config import settings

# Global references for connection pool and client
_redis_pool: Optional[ConnectionPool] = None
_redis_client: Optional[Redis] = None


def get_redis_pool() -> ConnectionPool:
    """
    Lazily creates and returns the singleton Redis ConnectionPool.
    Reuses pre-warmed sockets to avoid TCP handshake overhead on every query.
    """
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = ConnectionPool.from_url(
            settings.REDIS_URL,
            max_connections=20,
            decode_responses=True,  # Automatically decode byte strings to utf-8 strings
        )
    return _redis_pool


def get_redis_client() -> Redis:
    """
    Returns a singleton async Redis client instance.
    Safe for use across background tasks and workers.
    """
    global _redis_client
    if _redis_client is None:
        pool = get_redis_pool()
        _redis_client = aioredis.Redis(connection_pool=pool)
    return _redis_client


async def init_redis() -> Redis:
    """
    Initializes the Redis connection pool and verifies connectivity with a PING.
    Called during FastAPI startup lifespan.
    """
    client = get_redis_client()
    await client.ping()
    return client


async def close_redis() -> None:
    """
    Gracefully terminates the Redis connection pool and closes all open sockets.
    Called during FastAPI shutdown lifespan.
    """
    global _redis_client, _redis_pool
    if _redis_client is not None:
        await _redis_client.aclose()
        _redis_client = None
    if _redis_pool is not None:
        await _redis_pool.disconnect()
        _redis_pool = None


async def get_redis() -> AsyncGenerator[Redis, None]:
    """
    FastAPI dependency yielding a thread-safe, async Redis client.
    Usage in route: `redis: Redis = Depends(get_redis)`
    """
    client = get_redis_client()
    yield client
