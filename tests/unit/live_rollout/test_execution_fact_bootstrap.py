from types import SimpleNamespace

import pytest

from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.live_rollout.runtime_orchestrator import (
    _bootstrap_execution_position_facts,
)


@pytest.mark.asyncio
async def test_bootstrap_observes_only_actual_exchange_rows_in_order() -> None:
    rows = (object(), object())
    observed = []

    async def fetch_positions(*, include_flat):
        assert include_flat is True
        return rows

    async def observe_account_snapshot(row):
        observed.append(row)

    await _bootstrap_execution_position_facts(
        SimpleNamespace(fetch_positions=fetch_positions),
        SimpleNamespace(observe_account_snapshot=observe_account_snapshot),
    )
    assert observed == list(rows)


@pytest.mark.asyncio
async def test_bootstrap_missing_exchange_rows_does_not_invent_flat_positions() -> None:
    observed = []

    async def fetch_positions(*, include_flat):
        return ()

    async def observe_account_snapshot(row):
        observed.append(row)

    await _bootstrap_execution_position_facts(
        SimpleNamespace(fetch_positions=fetch_positions),
        SimpleNamespace(observe_account_snapshot=observe_account_snapshot),
    )
    assert observed == []


@pytest.mark.asyncio
async def test_bootstrap_read_failure_fails_startup_without_observing() -> None:
    observed = []

    async def fetch_positions(*, include_flat):
        raise RuntimeError("exchange unavailable")

    async def observe_account_snapshot(row):
        observed.append(row)

    with pytest.raises(RuntimeError, match="exchange unavailable"):
        await _bootstrap_execution_position_facts(
            SimpleNamespace(fetch_positions=fetch_positions),
            SimpleNamespace(observe_account_snapshot=observe_account_snapshot),
        )
    assert observed == []


@pytest.mark.asyncio
async def test_bootstrap_real_flat_hedge_rows_ready_both_execution_scopes() -> None:
    from datetime import UTC, datetime
    from decimal import Decimal

    from crypto_momentum_lab.domain.account.models import AccountPositionSnapshot
    from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
    from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
    from crypto_momentum_lab.execution_account.orders.coordinator import (
        OrderExecutionCoordinator,
    )

    rows = tuple(
        AccountPositionSnapshot(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            position_side=side,
            position_amt=Decimal("0"),
            entry_price=Decimal("0"),
            mark_price=Decimal("30000"),
            unrealized_pnl=Decimal("0"),
            notional=Decimal("0"),
            leverage=5,
            margin_type="cross",
            observed_at=datetime(2026, 9, 27, tzinfo=UTC),
            raw_payload={},
        )
        for side in ("LONG", "SHORT")
    )

    async def fetch_positions(*, include_flat):
        return rows

    coordinator = OrderExecutionCoordinator(
        backend=object(),
        account_label="primary",
        environment="live",
        execution_book=ExecutionBook(),
    )
    await _bootstrap_execution_position_facts(
        SimpleNamespace(fetch_positions=fetch_positions),
        coordinator,
    )
    for side in (FuturesPositionSide.LONG, FuturesPositionSide.SHORT):
        view = await coordinator.execution_book.read(
            ExecutionScope("live", "primary", "BTCUSDT", side)
        )
        assert view.zero_position_snapshot_confirmed is True
        assert view.is_ready_for_trade is True
