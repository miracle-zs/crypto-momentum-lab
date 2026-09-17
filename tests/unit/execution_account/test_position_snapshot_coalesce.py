"""Position snapshots must not stamp identical views in a fill burst."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountPositionSnapshot,
)
from crypto_momentum_lab.execution_account.sync import (
    ExecutionAccountSyncConfig,
    ExecutionAccountSyncService,
)


class _RecordingRepo:
    def __init__(self) -> None:
        self.positions: list[AccountPositionSnapshot] = []

    async def save_process_state(self, state) -> None:
        return None

    async def save_balance_position_snapshot(self, *, balances, positions) -> None:
        self.positions.extend(positions)

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


def _position(amt: str, *, entry: str, observed_at: datetime) -> AccountPositionSnapshot:
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


def test_identical_position_snapshots_coalesce_within_window() -> None:
    repo = _RecordingRepo()
    service = ExecutionAccountSyncService(
        client=_NoopClient(),  # type: ignore[arg-type]
        repository=repo,  # type: ignore[arg-type]
        config=_config(datetime(2026, 9, 16, 23, 45, tzinfo=UTC)),
    )
    t0 = datetime(2026, 9, 16, 23, 45, 0, tzinfo=UTC)
    burst = tuple(
        _position("266", entry="0.2317431", observed_at=t0 + timedelta(milliseconds=i))
        for i in range(10)
    )
    kept = service._positions_to_persist(burst, observed_at=t0)
    assert len(kept) == 1

    service._remember_position_signatures(kept, observed_at=t0)
    later = _position(
        "266",
        entry="0.2317431",
        observed_at=t0 + timedelta(seconds=1),
    )
    assert service._positions_to_persist((later,), observed_at=later.observed_at) == ()

    after_window = _position(
        "266",
        entry="0.2317431",
        observed_at=t0 + timedelta(seconds=3),
    )
    assert len(
        service._positions_to_persist((after_window,), observed_at=after_window.observed_at)
    ) == 1


def test_zero_position_persisted_only_as_close_transition() -> None:
    repo = _RecordingRepo()
    service = ExecutionAccountSyncService(
        client=_NoopClient(),  # type: ignore[arg-type]
        repository=repo,  # type: ignore[arg-type]
        config=_config(datetime(2026, 9, 16, 23, 45, tzinfo=UTC)),
    )
    t0 = datetime(2026, 9, 16, 23, 45, 0, tzinfo=UTC)
    # Never seen this symbol: a zero row is noise.
    assert service._positions_to_persist(
        (_position("0", entry="0", observed_at=t0),),
        observed_at=t0,
    ) == ()

    open_pos = _position("266", entry="0.23", observed_at=t0)
    assert service._positions_to_persist((open_pos,), observed_at=t0) == (open_pos,)
    service._remember_position_signatures((open_pos,), observed_at=t0)

    closed = _position("0", entry="0", observed_at=t0 + timedelta(seconds=8))
    assert service._positions_to_persist((closed,), observed_at=closed.observed_at) == (
        closed,
    )


def test_changed_amount_is_always_persisted() -> None:
    repo = _RecordingRepo()
    service = ExecutionAccountSyncService(
        client=_NoopClient(),  # type: ignore[arg-type]
        repository=repo,  # type: ignore[arg-type]
        config=_config(datetime(2026, 9, 16, 23, 45, tzinfo=UTC)),
    )
    t0 = datetime(2026, 9, 16, 23, 45, 0, tzinfo=UTC)
    open_266 = _position("266", entry="0.23", observed_at=t0)
    service._remember_position_signatures((open_266,), observed_at=t0)
    partial = _position("17", entry="0.23", observed_at=t0 + timedelta(milliseconds=50))
    assert service._positions_to_persist(
        (partial,), observed_at=partial.observed_at
    ) == (partial,)
