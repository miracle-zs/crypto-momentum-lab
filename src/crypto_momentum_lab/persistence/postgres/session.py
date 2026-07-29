from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

_POOL_SIZE = 5
_MAX_OVERFLOW = 5
_POOL_TIMEOUT_SECONDS = 10


def create_async_database_engine(
    database_url: str,
    *,
    pooled: bool = True,
) -> AsyncEngine:
    if not pooled:
        return create_async_engine(database_url, poolclass=NullPool)
    return create_async_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=_POOL_SIZE,
        max_overflow=_MAX_OVERFLOW,
        pool_timeout=_POOL_TIMEOUT_SECONDS,
    )


def create_sync_engine(database_url: str) -> Engine:
    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=_POOL_SIZE,
        max_overflow=_MAX_OVERFLOW,
        pool_timeout=_POOL_TIMEOUT_SECONDS,
    )
