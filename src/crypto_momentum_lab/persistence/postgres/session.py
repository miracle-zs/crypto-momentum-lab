import os
from math import ceil

from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool


def _env_int(key: str, default: int) -> int:
    val = os.environ.get(key, "").strip()
    if not val:
        return default
    try:
        return int(val)
    except ValueError as exc:
        raise ValueError(f"Invalid integer for {key}={val}") from exc


def _env_float(key: str, default: float) -> float:
    val = os.environ.get(key, "").strip()
    if not val:
        return default
    try:
        return float(val)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {key}={val}") from exc


_POOL_SIZE = _env_int("CML_DB_POOL_SIZE", 5)
_MAX_OVERFLOW = _env_int("CML_DB_MAX_OVERFLOW", 5)
_POOL_TIMEOUT_SECONDS = _env_float("CML_DB_POOL_TIMEOUT_SECONDS", 10.0)

# Keep the database planes explicit even when they currently resolve to the
# same PostgreSQL instance.  Separate pools prevent a market-data burst or a
# best-effort telemetry write from consuming connections needed by order and
# account state transitions.
_EXECUTION_POOL_SIZE = _env_int("CML_DB_EXECUTION_POOL_SIZE", 4)
_EXECUTION_MAX_OVERFLOW = _env_int("CML_DB_EXECUTION_MAX_OVERFLOW", 0)
_EXECUTION_POOL_TIMEOUT_SECONDS = _env_float(
    "CML_DB_EXECUTION_POOL_TIMEOUT_SECONDS", 3.0
)
_EXECUTION_COMMAND_TIMEOUT_SECONDS = _env_float(
    "CML_DB_EXECUTION_COMMAND_TIMEOUT_SECONDS", 5.0
)

_ACCOUNT_POOL_SIZE = _env_int("CML_DB_ACCOUNT_POOL_SIZE", 2)
_ACCOUNT_MAX_OVERFLOW = _env_int("CML_DB_ACCOUNT_MAX_OVERFLOW", 0)
_ACCOUNT_POOL_TIMEOUT_SECONDS = _env_float("CML_DB_ACCOUNT_POOL_TIMEOUT_SECONDS", 3.0)
_ACCOUNT_COMMAND_TIMEOUT_SECONDS = _env_float(
    "CML_DB_ACCOUNT_COMMAND_TIMEOUT_SECONDS", 5.0
)

_MAINTENANCE_POOL_SIZE = _env_int("CML_DB_MAINTENANCE_POOL_SIZE", 1)
_MAINTENANCE_MAX_OVERFLOW = _env_int("CML_DB_MAINTENANCE_MAX_OVERFLOW", 0)
_MAINTENANCE_POOL_TIMEOUT_SECONDS = _env_float(
    "CML_DB_MAINTENANCE_POOL_TIMEOUT_SECONDS", 3.0
)
_MAINTENANCE_COMMAND_TIMEOUT_SECONDS = _env_float(
    "CML_DB_MAINTENANCE_COMMAND_TIMEOUT_SECONDS", 60.0
)

_MARKET_POOL_SIZE = _env_int("CML_DB_MARKET_POOL_SIZE", 2)
_MARKET_MAX_OVERFLOW = _env_int("CML_DB_MARKET_MAX_OVERFLOW", 0)
_MARKET_POOL_TIMEOUT_SECONDS = _env_float("CML_DB_MARKET_POOL_TIMEOUT_SECONDS", 2.0)
_MARKET_COMMAND_TIMEOUT_SECONDS = _env_float(
    "CML_DB_MARKET_COMMAND_TIMEOUT_SECONDS", 5.0
)

_OBSERVABILITY_POOL_SIZE = _env_int("CML_DB_OBSERVABILITY_POOL_SIZE", 2)
_OBSERVABILITY_MAX_OVERFLOW = _env_int("CML_DB_OBSERVABILITY_MAX_OVERFLOW", 2)
_OBSERVABILITY_POOL_TIMEOUT_SECONDS = _env_float(
    "CML_DB_OBSERVABILITY_POOL_TIMEOUT_SECONDS", 5.0
)
_OBSERVABILITY_COMMAND_TIMEOUT_SECONDS = _env_float(
    "CML_DB_OBSERVABILITY_COMMAND_TIMEOUT_SECONDS", 10.0
)

_CHECKPOINT_POOL_SIZE = _env_int("CML_DB_CHECKPOINT_POOL_SIZE", 1)
_CHECKPOINT_MAX_OVERFLOW = _env_int("CML_DB_CHECKPOINT_MAX_OVERFLOW", 0)
_CHECKPOINT_POOL_TIMEOUT_SECONDS = _env_float(
    "CML_DB_CHECKPOINT_POOL_TIMEOUT_SECONDS", 2.0
)
_CHECKPOINT_COMMAND_TIMEOUT_SECONDS = _env_float(
    "CML_DB_CHECKPOINT_COMMAND_TIMEOUT_SECONDS", 10.0
)

_DASHBOARD_POOL_SIZE = _env_int("CML_DB_DASHBOARD_POOL_SIZE", 4)
_DASHBOARD_MAX_OVERFLOW = _env_int("CML_DB_DASHBOARD_MAX_OVERFLOW", 2)
_DASHBOARD_POOL_TIMEOUT_SECONDS = _env_float(
    "CML_DB_DASHBOARD_POOL_TIMEOUT_SECONDS", 5.0
)
_DASHBOARD_COMMAND_TIMEOUT_SECONDS = _env_float(
    "CML_DB_DASHBOARD_COMMAND_TIMEOUT_SECONDS", 10.0
)
_DASHBOARD_POOL_RECYCLE_SECONDS = _env_int("CML_DB_DASHBOARD_POOL_RECYCLE_SECONDS", 900)


def _server_timeout_settings(command_timeout_seconds: float) -> dict[str, str]:
    """Return PostgreSQL-side guards matching the client's request budget.

    The driver timeout bounds how long the caller waits; these settings also
    bound server work and abandoned transactions after a client disconnect.
    The idle guard is deliberately longer than one command so normal
    transaction assembly is unaffected.
    """

    timeout_ms = max(1, ceil(command_timeout_seconds * 1000))
    idle_transaction_timeout_ms = max(30_000, timeout_ms * 2)
    return {
        "statement_timeout": f"{timeout_ms}ms",
        "idle_in_transaction_session_timeout": f"{idle_transaction_timeout_ms}ms",
    }


def create_async_database_engine(
    database_url: str,
    *,
    pooled: bool = True,
    pool_size: int = _POOL_SIZE,
    max_overflow: int = _MAX_OVERFLOW,
    pool_timeout_seconds: float = _POOL_TIMEOUT_SECONDS,
    command_timeout_seconds: float | None = None,
    pool_recycle_seconds: int | None = None,
) -> AsyncEngine:
    if not pooled:
        return create_async_engine(database_url, poolclass=NullPool)
    if pool_size <= 0:
        raise ValueError("pool_size must be positive")
    if max_overflow < 0:
        raise ValueError("max_overflow must not be negative")
    if pool_timeout_seconds <= 0:
        raise ValueError("pool_timeout_seconds must be positive")
    if command_timeout_seconds is not None and command_timeout_seconds <= 0:
        raise ValueError("command_timeout_seconds must be positive")
    if pool_recycle_seconds is not None and pool_recycle_seconds <= 0:
        raise ValueError("pool_recycle_seconds must be positive")
    connect_args = (
        {}
        if command_timeout_seconds is None
        else {
            "command_timeout": command_timeout_seconds,
            "server_settings": _server_timeout_settings(command_timeout_seconds),
        }
    )
    engine_options: dict[str, object] = {
        "pool_pre_ping": True,
        "pool_size": pool_size,
        "max_overflow": max_overflow,
        "pool_timeout": pool_timeout_seconds,
        "connect_args": connect_args,
    }
    if pool_recycle_seconds is not None:
        engine_options["pool_recycle"] = pool_recycle_seconds
    return create_async_engine(database_url, **engine_options)


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


def create_account_database_engine(
    database_url: str,
    *,
    pool_size: int = _ACCOUNT_POOL_SIZE,
    max_overflow: int = _ACCOUNT_MAX_OVERFLOW,
    pool_timeout_seconds: float = _ACCOUNT_POOL_TIMEOUT_SECONDS,
    command_timeout_seconds: float = _ACCOUNT_COMMAND_TIMEOUT_SECONDS,
) -> AsyncEngine:
    """Create the small pool used by one serial account-sync daemon."""

    return create_async_database_engine(
        database_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout_seconds=pool_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
    )


def create_maintenance_database_engine(
    database_url: str,
    *,
    pool_size: int = _MAINTENANCE_POOL_SIZE,
    max_overflow: int = _MAINTENANCE_MAX_OVERFLOW,
    pool_timeout_seconds: float = _MAINTENANCE_POOL_TIMEOUT_SECONDS,
    command_timeout_seconds: float = _MAINTENANCE_COMMAND_TIMEOUT_SECONDS,
) -> AsyncEngine:
    """Create an isolated, slower pool for bounded maintenance work."""

    return create_async_database_engine(
        database_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout_seconds=pool_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
    )


def create_market_database_engine(
    database_url: str,
    *,
    pool_size: int = _MARKET_POOL_SIZE,
    max_overflow: int = _MARKET_MAX_OVERFLOW,
    pool_timeout_seconds: float = _MARKET_POOL_TIMEOUT_SECONDS,
    command_timeout_seconds: float = _MARKET_COMMAND_TIMEOUT_SECONDS,
) -> AsyncEngine:
    """Create the bounded pool used by market state and universe reads."""

    return create_async_database_engine(
        database_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout_seconds=pool_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
    )


def create_observability_database_engine(
    database_url: str,
    *,
    pool_size: int = _OBSERVABILITY_POOL_SIZE,
    max_overflow: int = _OBSERVABILITY_MAX_OVERFLOW,
    pool_timeout_seconds: float = _OBSERVABILITY_POOL_TIMEOUT_SECONDS,
    command_timeout_seconds: float = _OBSERVABILITY_COMMAND_TIMEOUT_SECONDS,
) -> AsyncEngine:
    """Create a small, best-effort pool for telemetry writes."""

    return create_async_database_engine(
        database_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout_seconds=pool_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
    )


def create_checkpoint_database_engine(
    database_url: str,
    *,
    pool_size: int = _CHECKPOINT_POOL_SIZE,
    max_overflow: int = _CHECKPOINT_MAX_OVERFLOW,
    pool_timeout_seconds: float = _CHECKPOINT_POOL_TIMEOUT_SECONDS,
    command_timeout_seconds: float = _CHECKPOINT_COMMAND_TIMEOUT_SECONDS,
) -> AsyncEngine:
    """Create an isolated pool for durable strategy checkpoint writes."""

    return create_async_database_engine(
        database_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout_seconds=pool_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
    )


def create_dashboard_database_engine(
    database_url: str,
    *,
    pool_size: int = _DASHBOARD_POOL_SIZE,
    max_overflow: int = _DASHBOARD_MAX_OVERFLOW,
    pool_timeout_seconds: float = _DASHBOARD_POOL_TIMEOUT_SECONDS,
    command_timeout_seconds: float = _DASHBOARD_COMMAND_TIMEOUT_SECONDS,
    pool_recycle_seconds: int = _DASHBOARD_POOL_RECYCLE_SECONDS,
) -> AsyncEngine:
    """Create a bounded, recyclable pool for read-only dashboard queries."""

    return create_async_database_engine(
        database_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout_seconds=pool_timeout_seconds,
        command_timeout_seconds=command_timeout_seconds,
        pool_recycle_seconds=pool_recycle_seconds,
    )


def create_sync_engine(
    database_url: str,
    *,
    pool_size: int = _POOL_SIZE,
    max_overflow: int = _MAX_OVERFLOW,
    pool_timeout_seconds: float = _POOL_TIMEOUT_SECONDS,
) -> Engine:
    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout_seconds,
    )
