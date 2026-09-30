"""Shared transient failure policy for native live runtime dependencies."""

from sqlalchemy.exc import SQLAlchemyError


def is_transient_runtime_error(error: Exception) -> bool:
    return isinstance(
        error,
        (SQLAlchemyError, TimeoutError, ConnectionError, OSError),
    )
