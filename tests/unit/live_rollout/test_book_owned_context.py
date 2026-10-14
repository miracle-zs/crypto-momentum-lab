"""The restored Book owns batches; account reads supply exposure only."""

from datetime import UTC, datetime, timedelta
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
    provider = PostgresLiveContextProvider(
        session_factory=lambda: None,
        account_label="primary",
        run_id="test-run",
        strategy_name="test-strategy",
        strategy_config_hash="test-config",
    )
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
    from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
    from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
    from crypto_momentum_lab.domain.execution.position_book import PositionBook
    from crypto_momentum_lab.domain.execution.position_ledger_models import (
        AccountFactStreamScope,
        AccountFillLoadProvenance,
        FactCoverageInterval,
        FactCoverageStatus,
        PositionKey,
    )
    from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts
    from crypto_momentum_lab.domain.execution.snapshot_encoding import (
        stable_snapshot_anchor_id,
    )
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
    snap_time = NOW - timedelta(hours=1)
    zero_snap = AccountPositionSnapshot(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        mark_price=Decimal("100"),
        notional=Decimal("0"),
        leverage=5,
        margin_type="cross",
        observed_at=snap_time,
        raw_payload={},
    )
    current_snap = AccountPositionSnapshot(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        position_amt=Decimal("0.5") if owned else Decimal("0"),
        entry_price=Decimal("100") if owned else Decimal("0"),
        unrealized_pnl=Decimal("0"),
        mark_price=Decimal("100"),
        notional=Decimal("50") if owned else Decimal("0"),
        leverage=5,
        margin_type="cross",
        observed_at=NOW,
        raw_payload={},
    )
    anchor_id = stable_snapshot_anchor_id(zero_snap)
    prov = AccountFillLoadProvenance(
        stream_scope=scope,
        load_id="scan-1",
        scan_origin_from_id=None,
        scan_origin_start_time_ms=1000,
        request_from_id=None,
        next_from_id=None,
        page_count=1,
        page_exhausted=True,
        truncated=False,
        checked_through=NOW,
        observed_at=NOW,
        source_anchor_id=anchor_id,
        source_anchor_event_cut=snap_time,
        source_anchor_kind="zero_snapshot",
    )
    cov = FactCoverageInterval(
        start_at=snap_time,
        end_at=NOW,
        source_cursor="scan-1",
        status=FactCoverageStatus.CONFIRMED,
        stream_scope=scope,
        evidence_observed_at=NOW,
        load_provenance=prov,
        page_exhausted=True,
        not_truncated=True,
    )
    facts = AccountFacts(
        position_key=key,
        stream_scope=scope,
        fills=(opening,) if owned else (),
        coverage=cov,
        snapshots=(zero_snap, current_snap),
        fill_load_provenance=prov,
    )
    book = ExecutionBook(books_by_key={key.canonical_id: PositionBook.from_facts(facts)})
    provider = PostgresLiveContextProvider(
        session_factory=lambda: None,
        account_label="primary",
        run_id="test-run",
        strategy_name="test-strategy",
        strategy_config_hash="test-config",
    )
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
            sequence=0,
            positions=(
                SimpleNamespace(
                    symbol="BTCUSDT",
                    position_side="LONG",
                    position_amt=Decimal("0.5"),
                ),
            ),
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
