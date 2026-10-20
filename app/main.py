from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import engine, get_db
from app.api.v1 import api_router


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifecycle manager.
    Executes startup and shutdown tasks.
    """
    print(f"🚀 Starting {settings.PROJECT_NAME}...")
    yield
    print(f"🛑 Shutting down {settings.PROJECT_NAME}...")
    await engine.dispose()


app = FastAPI(
    title=settings.PROJECT_NAME,
    version="0.1.0",
    description="High-throughput, resilient async webhook delivery engine featuring HMAC verification, idempotency, and retries.",
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Cross-Origin Resource Sharing (CORS) Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", tags=["Root"])
async def root():
    """Root metadata endpoint."""
    return {
        "service": settings.PROJECT_NAME,
        "version": "0.1.0",
        "docs_url": "/docs",
        "status": "operational",
    }


@app.get("/health", tags=["Health"])
async def health_check(db: AsyncSession = Depends(get_db)):
    """
    Production health check endpoint.
    Actively pings PostgreSQL with 'SELECT 1' to verify connection vitality.
    """
    try:
        result = await db.execute(text("SELECT 1"))
        scalar = result.scalar()
        if scalar != 1:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database ping returned unexpected response",
            )
        return {
            "status": "healthy",
            "database": "connected",
            "service": settings.PROJECT_NAME,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection failure: {str(e)}",
        )


# Mount versioned API routes (/api/v1)
app.include_router(api_router, prefix=settings.API_V1_STR)
