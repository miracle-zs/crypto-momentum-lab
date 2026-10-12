"""One publication owner for every committed execution candidate."""

import pathlib
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide

NOW = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)


def _scope() -> ExecutionScope:
    return ExecutionScope(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        position_side=FuturesPositionSide.BOTH,
    )


def _book(scope: ExecutionScope) -> ExecutionBook:
    key = scope.to_position_key()
    book = ExecutionBook()
    book._stream_scopes[key.canonical_id] = AccountFactStreamScope.for_position_key(
        key, stream_id="account_event_hub", stream_epoch="epoch-1"
    )
    book._ensure_journal(key)
    book._head_revisions[key.canonical_id] = 0
    return book


@pytest.mark.asyncio
async def test_committed_candidate_is_published_under_the_key_lock() -> None:
    scope = _scope()
    key = scope.to_position_key()
    book = _book(scope)
    candidate = book._staged_copy(key=key)
    command = TradeCommand(
        "order-publish",
        key,
        TradeCommandType.ENTRY,
        StrategySide.LONG,
        EntryType.MARKET,
        Decimal("1.0"),
        created_at=NOW,
    )
    candidate.register_prepared_command(command, scope)

    observed_lock_state: list[bool] = []

    def _read_back() -> str:
        observed_lock_state.append(book._mutation_lock(key).locked())
        return "read-back"

    published = await book._publish_committed_candidate(
        candidate, key=key, after=_read_back
    )

    # The committed candidate became visible, the read-back ran inside the same
    # critical section, and its value was returned to the caller.
    assert book.get_outbox(command.command_id) is not None
    assert observed_lock_state == [True]
    assert published == "read-back"


@pytest.mark.asyncio
async def test_publishing_without_read_back_returns_none() -> None:
    scope = _scope()
    key = scope.to_position_key()
    book = _book(scope)
    candidate = book._staged_copy(key=key)

    assert (
        await book._publish_committed_candidate(candidate, key=key) is None
    )


def test_only_the_book_publishes_candidates() -> None:
    """Acceptance, evidence and command flows must all use the single owner.

    A second direct ``_publish_candidate`` call site outside the book would let
    the monotonic merge and the terminal-winner rule drift between flows.
    """
    root = pathlib.Path("src/crypto_momentum_lab")
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        text = path.read_text()
        if "_publish_candidate(" not in text:
            continue
        for number, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("#") or "_publish_candidate(" not in stripped:
                continue
            if stripped.startswith(("def ", "async def ")):
                continue
            # The book may call its own rule; every other module (and the book's
            # transaction flows) must publish through the single owner.
            if path.name == "execution_book.py" and stripped.startswith(
                "self._publish_candidate("
            ):
                continue
            offenders.append(f"{path}:{number}: {stripped}")

    assert offenders == []
