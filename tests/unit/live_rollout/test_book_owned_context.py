"""The restored Book owns batches; account reads supply exposure only."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from crypto_momentum_lab.domain.account import AccountPositionSnapshot
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    PostgresLiveContextProvider,
)

NOW = datetime(2026, 10, 2, tzinfo=UTC)


@pytest.mark.parametrize("from_hub", [True, False])
async def test_book_owned_context_never_rebuilds_legacy_order_and_fill_history(
    from_hub,
):
    position = AccountPositionSnapshot(
        "live",
        "primary",
        "BTCUSDT",
        "LONG",
        Decimal("0.5"),
        Decimal("100"),
        Decimal("110"),
        Decimal("5"),
        Decimal("55"),
        5,
        "cross",
        NOW,
        {},
    )
    session = AsyncMock()
    session.__aenter__.return_value = session
    session.execute.side_effect = AssertionError("unnecessary legacy ownership query")
    session.scalar.side_effect = [
        NOW,
        SimpleNamespace(position_count=1, status="ready"),
        NOW,
    ]
    session.scalars.return_value = SimpleNamespace(all=lambda: (position,))
    sessions = Mock(return_value=session)
    provider = object.__new__(PostgresLiveContextProvider)
    provider._sessions = sessions
    provider._execution_book = object()
    provider._account_label = "primary"
    provider._run_id = "run"
    snapshot = SimpleNamespace(
        config=SimpleNamespace(observed_at=NOW), positions=(position,)
    )
    result = await provider._account_position_view(
        (),
        account_snapshot=snapshot if from_hub else None,
    )
    assert result == (
        NOW,
        frozenset({"BTCUSDT"}),
        Decimal("5"),
        Decimal("55"),
        (),
        frozenset(),
        frozenset(),
        {},
    )
    assert sessions.call_count == int(not from_hub)
    assert session.scalar.await_count == (0 if from_hub else 3)
    assert session.scalars.await_count == int(not from_hub)


@pytest.mark.parametrize("owned", [True, False])
async def test_book_readiness_controls_cache_and_unowned_exposure_stays_blocked(owned):
    from dataclasses import replace

    from crypto_momentum_lab.domain.account import AccountFillEvent
    from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
    from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
    from crypto_momentum_lab.domain.execution.position_book import PositionBook
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFacts,
        AccountFactStreamScope,
        PositionKey,
    )
    from crypto_momentum_lab.domain.execution.recovery_models import DurableJournalCut
    from tests.unit.live_rollout.test_postgres_runtime import _runtime_context

    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="hub", stream_epoch="epoch"
    )
    opening = AccountFillEvent(
        "live",
        "primary",
        "BTCUSDT",
        "entry-trade",
        "entry",
        "BUY",
        Decimal("100"),
        Decimal("0.5"),
        Decimal("0"),
        Decimal("0"),
        "USDT",
        NOW,
        {"positionSide": "LONG"},
    )
    journal = AccountJournal.from_durable_cut(
        DurableJournalCut(
            scope=scope,
            facts=AccountFacts(
                position_key=key,
                stream_scope=scope,
                fills=(opening,) if owned else (),
            ),
            revision=1,
            as_of=NOW,
        )
    )
    book = ExecutionBook(books_by_key={key.canonical_id: PositionBook(journal)})
    provider = object.__new__(PostgresLiveContextProvider)
    provider._account_label = "primary"
    provider._execution_book = book
    provider._cache_epoch = 7
    context = replace(
        _runtime_context(),
        context_epoch=7,
        open_position_symbols=frozenset({"BTCUSDT"}),
        pending_position_symbols=frozenset({"BTCUSDT"}),
        unmanaged_position_symbols=frozenset({"BTCUSDT"}),
        account_snapshot=SimpleNamespace(
            positions=(
                SimpleNamespace(
                    symbol="BTCUSDT",
                    position_side="LONG",
                    position_amt=Decimal("0.5"),
                ),
            )
        ),
    )
    provider._cached_context = context
    result = await provider._with_execution_book(
        context, SimpleNamespace(bucket_end=NOW)
    )
    expected_unmanaged = frozenset() if owned else frozenset({"BTCUSDT"})
    assert bool(result.managed_positions) is owned
    assert result.pending_position_symbols == frozenset()
    assert result.unmanaged_position_symbols == expected_unmanaged
    provider.invalidate()
    assert not provider.is_current(result)
