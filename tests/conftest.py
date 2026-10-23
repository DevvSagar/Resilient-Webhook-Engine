import pytest
import httpx
from typing import AsyncGenerator

from app.main import app
from app.core.database import AsyncSessionLocal
from app.core.redis import init_redis, close_redis, get_redis_client


@pytest.fixture(scope="function")
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Provides a lightweight async HTTP test client bound to FastAPI."""
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac


@pytest.fixture(scope="function")
async def db():
    """Provides a dedicated async PostgreSQL session for testing."""
    async with AsyncSessionLocal() as session:
        yield session


@pytest.fixture(scope="function")
async def redis():
    """Provides an active async Redis connection."""
    client = get_redis_client()
    await client.ping()
    yield client
