from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool


def create_async_database_engine(
    database_url: str,
    *,
    pooled: bool = True,
) -> AsyncEngine:
    if not pooled:
        return create_async_engine(database_url, poolclass=NullPool)
    return create_async_engine(database_url, pool_pre_ping=True)


def create_sync_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True)
