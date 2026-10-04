"""Position snapshots must not stamp identical views in a fill burst."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountPositionSnapshot,
)
from crypto_momentum_lab.execution_account.sync import (
    ExecutionAccountSyncService,
)
from crypto_momentum_lab.execution_account.sync_models import (
    ExecutionAccountSyncConfig,
)


class _RecordingRepo:
    def __init__(self) -> None:
        self.positions: list[AccountPositionSnapshot] = []

    async def save_process_state(self, state) -> None:
        return None

    async def save_reconciliation_snapshot(self, **kwargs) -> None:
        self.positions.extend(kwargs["positions"])


class _NoopClient:
    pass


def _config(observed_at: datetime) -> ExecutionAccountSyncConfig:
    return ExecutionAccountSyncConfig(
        environment="live",
        account_label="primary",
        expected_multi_assets_mode=False,
        expected_hedge_mode=True,
        observed_at=observed_at,
        historical_fill_reconciliation_interval_seconds=60,
    )


def _position(
    amt: str, *, entry: str, observed_at: datetime
) -> AccountPositionSnapshot:
    return AccountPositionSnapshot(
        environment="live",
        account_label="primary",
        symbol="龙虾USDT",
        position_side="LONG",
        position_amt=Decimal(amt),
        entry_price=Decimal(entry),
        mark_price=Decimal("0.21"),
        unrealized_pnl=Decimal("-1"),
        notional=Decimal("10"),
        leverage=5,
        margin_type="CROSSED",
        observed_at=observed_at,
        raw_payload={},
    )


async def _publish(service, positions, observed_at):
    from crypto_momentum_lab.domain.account import AccountConfigSnapshot
    from crypto_momentum_lab.domain.account.snapshot_models import AccountSnapshot
    from crypto_momentum_lab.execution_account.binance.user_data_parser import (
        parse_user_data_event,
    )

    config = AccountConfigSnapshot(
        environment="live", account_label="primary", multi_assets_mode=False,
        hedge_mode=True, fee_tier=0, observed_at=observed_at, raw_payload={},
    )
    event = parse_user_data_event(
        {"e": "ACCOUNT_UPDATE", "E": int(observed_at.timestamp() * 1000),
         "a": {"B": [], "P": []}}, received_at=observed_at,
    )
    result = await service.persist_user_data_event(
        snapshot=AccountSnapshot(config=config, balances=(), positions=positions, open_orders=()),
        event=event,
    )
    assert result.snapshot.positions == positions


async def test_identical_position_events_preserve_live_view_without_repeating_history():
    repo = _RecordingRepo()
    t0 = datetime(2026, 9, 16, 23, 45, tzinfo=UTC)
    service = ExecutionAccountSyncService(client=_NoopClient(), repository=repo, config=_config(t0))
    first = _position("266", entry="0.2317431", observed_at=t0)
    await _publish(service, (first,), t0)
    for offset in (1, 10, 50, 1000):
        at = t0 + timedelta(milliseconds=offset)
        await _publish(service, (_position("266", entry="0.2317431", observed_at=at),), at)
    assert repo.positions == [first]
    at = t0 + timedelta(seconds=3)
    later = _position("266", entry="0.2317431", observed_at=at)
    await _publish(service, (later,), at)
    assert repo.positions == [first, later]


async def test_zero_event_records_a_real_close_without_creating_unknown_positions():
    repo = _RecordingRepo()
    t0 = datetime(2026, 9, 16, 23, 45, tzinfo=UTC)
    service = ExecutionAccountSyncService(client=_NoopClient(), repository=repo, config=_config(t0))
    await _publish(service, (_position("0", entry="0", observed_at=t0),), t0)
    assert repo.positions == []
    opening = _position("266", entry="0.23", observed_at=t0)
    await _publish(service, (opening,), t0)
    at = t0 + timedelta(seconds=8)
    closing = _position("0", entry="0", observed_at=at)
    await _publish(service, (closing,), at)
    assert repo.positions == [opening, closing]


async def test_partial_close_is_persisted_even_within_the_history_coalescing_window():
    repo = _RecordingRepo()
    t0 = datetime(2026, 9, 16, 23, 45, tzinfo=UTC)
    service = ExecutionAccountSyncService(client=_NoopClient(), repository=repo, config=_config(t0))
    opening = _position("266", entry="0.23", observed_at=t0)
    await _publish(service, (opening,), t0)
    at = t0 + timedelta(milliseconds=50)
    partial = _position("17", entry="0.23", observed_at=at)
    await _publish(service, (partial,), at)
    assert repo.positions == [opening, partial]
