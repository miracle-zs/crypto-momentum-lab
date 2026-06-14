from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def create_async_database_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


def create_sync_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True)
