"""Read existing execution position views without mutating execution state."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime

from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionView,
)


async def list_position_views(
    *,
    books: dict[str, PositionBook],
    stream_scopes: dict[str, AccountFactStreamScope],
    read: Callable[..., Awaitable[PositionView]],
    durable_restore_failed: bool,
    environment: str,
    account_label: str,
    event_cut: datetime | None = None,
    stream_id: str | None = None,
    stream_epoch: str | None = None,
    symbols: frozenset[str] | None = None,
) -> tuple[PositionView, ...]:
    """List existing views for one account, optionally narrowed by stream/symbol."""
    if not environment.strip() or not account_label.strip():
        raise ValueError("environment and account_label must not be empty")
    if (stream_id is None) != (stream_epoch is None):
        raise ValueError("stream_id and stream_epoch must be supplied together")
    if stream_id is not None and stream_epoch is not None and (
        not stream_id.strip() or not stream_epoch.strip()
    ):
        raise ValueError("stream_id and stream_epoch must not be empty")
    if event_cut is not None and (
        event_cut.tzinfo is None or event_cut.utcoffset() is None
    ):
        raise ValueError("event_cut must be timezone-aware")
    if durable_restore_failed and not stream_scopes:
        raise RuntimeError("execution facts require successful durable restoration")
    scopes = []
    for book in tuple(books.values()):
        key = book.position_key
        if key.environment != environment or key.account_label != account_label:
            continue
        if symbols is not None and key.symbol not in symbols:
            continue
        source = stream_scopes.get(key.canonical_id)
        if stream_id is not None and (
            source is None
            or source.stream_id != stream_id
            or source.stream_epoch != stream_epoch
        ):
            continue
        scopes.append(
            ExecutionScope(
                environment=key.environment,
                account_label=key.account_label,
                symbol=key.symbol,
                position_side=key.position_side,
            )
        )
    scopes.sort(key=lambda scope: (scope.symbol, scope.position_side.value))
    return tuple(
        [
            await read(
                scope,
                event_cut=event_cut,
                stream_id=stream_id,
                stream_epoch=stream_epoch,
            )
            for scope in scopes
        ]
    )
