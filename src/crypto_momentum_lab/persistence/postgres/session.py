from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

_POOL_SIZE = 5
_MAX_OVERFLOW = 5
_POOL_TIMEOUT_SECONDS = 10

# Keep the database planes explicit even when they currently resolve to the
# same PostgreSQL instance.  Separate pools prevent a market-data burst or a
# best-effort telemetry write from consuming connections needed by order and
# account state transitions.
_EXECUTION_POOL_SIZE = 4
_EXECUTION_MAX_OVERFLOW = 0
_EXECUTION_POOL_TIMEOUT_SECONDS = 3
_EXECUTION_COMMAND_TIMEOUT_SECONDS = 5
_MAINTENANCE_POOL_SIZE = 1
_MAINTENANCE_MAX_OVERFLOW = 0
_MAINTENANCE_POOL_TIMEOUT_SECONDS = 3
_MAINTENANCE_COMMAND_TIMEOUT_SECONDS = 60
_MARKET_POOL_SIZE = 2
_MARKET_MAX_OVERFLOW = 0
_MARKET_POOL_TIMEOUT_SECONDS = 2
_MARKET_COMMAND_TIMEOUT_SECONDS = 5
_OBSERVABILITY_POOL_SIZE = 1
_OBSERVABILITY_MAX_OVERFLOW = 0
_OBSERVABILITY_POOL_TIMEOUT_SECONDS = 1
_OBSERVABILITY_COMMAND_TIMEOUT_SECONDS = 1
_CHECKPOINT_POOL_SIZE = 1
_CHECKPOINT_MAX_OVERFLOW = 0
_CHECKPOINT_POOL_TIMEOUT_SECONDS = 2
_CHECKPOINT_COMMAND_TIMEOUT_SECONDS = 10


def create_async_database_engine(
    database_url: str,
    *,
    pooled: bool = True,
    pool_size: int = _POOL_SIZE,
    max_overflow: int = _MAX_OVERFLOW,
    pool_timeout_seconds: float = _POOL_TIMEOUT_SECONDS,
    command_timeout_seconds: float | None = None,
) -> AsyncEngine:
    if not pooled:
        return create_async_engine(database_url, poolclass=NullPool)
    if pool_size <= 0:
        raise ValueError("pool_size must be positive")
    if max_overflow < 0:
        raise ValueError("max_overflow must not be negative")
    if pool_timeout_seconds <= 0:
        raise ValueError("pool_timeout_seconds must be positive")
    connect_args = (
        {}
        if command_timeout_seconds is None
        else {"command_timeout": command_timeout_seconds}
    )
    return create_async_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout_seconds,
        connect_args=connect_args,
    )


def create_execution_database_engine(
    database_url: str,
    *,
    pool_size: int = _EXECUTION_POOL_SIZE,
    max_overflow: int = _EXECUTION_MAX_OVERFLOW,
    pool_timeout_seconds: float = _EXECUTION_POOL_TIMEOUT_SECONDS,
    command_timeout_seconds: float = _EXECUTION_COMMAND_TIMEOUT_SECONDS,
) -> AsyncEngine:
    """Create the bounded, latency-prioritized execution data pool."""

    return create_async_database_engine(
        database_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout_seconds=pool_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
    )


def create_maintenance_database_engine(database_url: str) -> AsyncEngine:
    """Create an isolated, slower pool for bounded maintenance work."""

    return create_async_database_engine(
        database_url,
        pool_size=_MAINTENANCE_POOL_SIZE,
        max_overflow=_MAINTENANCE_MAX_OVERFLOW,
        pool_timeout_seconds=_MAINTENANCE_POOL_TIMEOUT_SECONDS,
        command_timeout_seconds=_MAINTENANCE_COMMAND_TIMEOUT_SECONDS,
    )


def create_market_database_engine(database_url: str) -> AsyncEngine:
    """Create the bounded pool used by market state and universe reads."""

    return create_async_database_engine(
        database_url,
        pool_size=_MARKET_POOL_SIZE,
        max_overflow=_MARKET_MAX_OVERFLOW,
        pool_timeout_seconds=_MARKET_POOL_TIMEOUT_SECONDS,
        command_timeout_seconds=_MARKET_COMMAND_TIMEOUT_SECONDS,
    )


def create_observability_database_engine(database_url: str) -> AsyncEngine:
    """Create a small, best-effort pool for telemetry writes."""

    return create_async_database_engine(
        database_url,
        pool_size=_OBSERVABILITY_POOL_SIZE,
        max_overflow=_OBSERVABILITY_MAX_OVERFLOW,
        pool_timeout_seconds=_OBSERVABILITY_POOL_TIMEOUT_SECONDS,
        command_timeout_seconds=_OBSERVABILITY_COMMAND_TIMEOUT_SECONDS,
    )


def create_checkpoint_database_engine(database_url: str) -> AsyncEngine:
    """Create an isolated pool for durable strategy checkpoint writes."""

    return create_async_database_engine(
        database_url,
        pool_size=_CHECKPOINT_POOL_SIZE,
        max_overflow=_CHECKPOINT_MAX_OVERFLOW,
        pool_timeout_seconds=_CHECKPOINT_POOL_TIMEOUT_SECONDS,
        command_timeout_seconds=_CHECKPOINT_COMMAND_TIMEOUT_SECONDS,
    )


def create_sync_engine(database_url: str) -> Engine:
    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=_POOL_SIZE,
        max_overflow=_MAX_OVERFLOW,
        pool_timeout=_POOL_TIMEOUT_SECONDS,
    )
