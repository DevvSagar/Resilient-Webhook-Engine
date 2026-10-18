"""API version 1 package."""
from fastapi import APIRouter
from app.api.v1.subscriptions import router as subscriptions_router
from app.api.v1.events import router as events_router

api_router = APIRouter()
api_router.include_router(subscriptions_router)
api_router.include_router(events_router)

__all__ = ["api_router"]
