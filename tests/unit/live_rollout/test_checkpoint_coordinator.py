from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.strategy import StrategyCheckpoint
from crypto_momentum_lab.live_rollout.checkpoint_coordinator import (
    LiveCheckpointCoordinator,
)
from crypto_momentum_lab.live_rollout.checkpoint_writer import CheckpointWriter

NOW = datetime(2026, 7, 3, 23, 59, tzinfo=UTC)


class _Strategy:
    def __init__(self) -> None:
        self.checkpoint_calls = 0

    def checkpoint(
        self,
        *,
        include_market_state_buffers: bool = True,
    ) -> StrategyCheckpoint:
        self.checkpoint_calls += 1
        return StrategyCheckpoint(
            last_processed_at_by_symbol={"ETHUSDT": NOW},
            warmup_buckets_by_symbol={},
            cooldown_buckets_remaining_by_symbol={},
            payload={
                "include_market_state_buffers": include_market_state_buffers,
            },
        )


def _state(symbol: str = "BTCUSDT") -> MarketState15s:
    return MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        symbol=symbol,
        bucket_start=NOW,
        bucket_end=NOW + timedelta(seconds=15),
        open_price=Decimal("100"),
        high_price=Decimal("101"),
        low_price=Decimal("99"),
        close_price=Decimal("100"),
        trade_count=1,
        trade_notional=Decimal("100"),
        aggressive_buy_notional=Decimal("50"),
        aggressive_sell_notional=Decimal("50"),
        last_bid_price=Decimal("99.9"),
        last_ask_price=Decimal("100.1"),
        spread=Decimal("0.2"),
        midpoint=Decimal("100"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("100"),
        closed_kline_count=0,
        source_event_count=1,
        first_received_at=NOW,
        last_received_at=NOW,
    )


async def test_coordinator_tracks_progress_and_flushes_final_checkpoint() -> None:
    persisted: list[tuple[str, StrategyCheckpoint, datetime]] = []
    strategy = _Strategy()
    writer = CheckpointWriter(
        run_id="run-1",
        persist=lambda run_id, checkpoint, saved_at: _persist(
            persisted,
            run_id,
            checkpoint,
            saved_at,
        ),
    )
    coordinator = LiveCheckpointCoordinator(
        writer=writer,
        strategy=strategy,
        checkpoint_every_states=10,
    )

    await coordinator.start()
    assert coordinator.last_processed_at("ETHUSDT") == NOW
    coordinator.record_processed_state(_state(), saved_at=NOW)

    assert coordinator.last_processed_at("BTCUSDT") == NOW
    assert await coordinator.save_final() is True
    await coordinator.stop()

    assert [run_id for run_id, _checkpoint, _saved_at in persisted] == ["run-1"]
    assert persisted[0][2] == NOW


async def test_coordinator_submits_periodic_checkpoint_and_forgets_gap_symbol() -> None:
    persisted: list[tuple[str, StrategyCheckpoint, datetime]] = []
    strategy = _Strategy()
    writer = CheckpointWriter(
        run_id="run-1",
        persist=lambda run_id, checkpoint, saved_at: _persist(
            persisted,
            run_id,
            checkpoint,
            saved_at,
        ),
    )
    coordinator = LiveCheckpointCoordinator(
        writer=writer,
        strategy=strategy,
        checkpoint_every_states=1,
    )

    await coordinator.start()
    coordinator.forget_symbol("ETHUSDT")
    assert coordinator.last_processed_at("ETHUSDT") is None
    coordinator.record_processed_state(_state(), saved_at=NOW)
    await writer.flush()
    await coordinator.stop()

    assert len(persisted) == 1
    assert persisted[0][1].payload == {
        "include_market_state_buffers": False,
    }


async def _persist(
    persisted: list[tuple[str, StrategyCheckpoint, datetime]],
    run_id: str,
    checkpoint: StrategyCheckpoint,
    saved_at: datetime,
) -> None:
    persisted.append((run_id, checkpoint, saved_at))
