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


async def test_coordinator_persists_hub_cursor_with_compact_checkpoint() -> None:
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
        hub_cursor_provider=lambda: {
            "stream_id": "stream-a",
            "sequence": 17,
        },
    )

    await coordinator.start()
    coordinator.record_processed_state(_state(), saved_at=NOW)
    await writer.flush()
    await coordinator.stop()

    assert persisted[0][1].payload["market_state_hub_cursor"] == {
        "stream_id": "stream-a",
        "sequence": 17,
    }


async def test_coordinator_advances_watermark_for_recovered_market_state() -> None:
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
    recovered = _state("BTCUSDT")
    coordinator.record_recovered_state(recovered, saved_at=NOW)

    assert coordinator.last_processed_at("BTCUSDT") == NOW
    assert await coordinator.save_final() is True
    await coordinator.stop()

    assert persisted[0][1].payload == {
        "include_market_state_buffers": False,
    }


async def test_coordinator_periodic_phase_alignment_and_deduplication() -> None:
    persisted: list[tuple[str, StrategyCheckpoint, datetime]] = []
    strategy = _Strategy()
    writer = CheckpointWriter(
        run_id="run-phase-1",
        persist=lambda run_id, checkpoint, saved_at: _persist(
            persisted,
            run_id,
            checkpoint,
            saved_at,
        ),
    )
    # Configure 60s period with 15s phase, high state threshold
    coordinator = LiveCheckpointCoordinator(
        writer=writer,
        strategy=strategy,
        checkpoint_every_states=1000,
        checkpoint_every_seconds=60.0,
        checkpoint_phase_seconds=15.0,
    )
    await coordinator.start()

    # Base time: 2026-07-03 23:59:00 (second 00)
    base = datetime(2026, 7, 3, 23, 59, 0, tzinfo=UTC)

    # 1. Bucket ending at :00 (phase mismatch, should NOT submit)
    state_00 = MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        symbol="BTCUSDT",
        bucket_start=base - timedelta(seconds=15),
        bucket_end=base,
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
        first_received_at=base,
        last_received_at=base,
    )
    coordinator.record_processed_state(state_00, saved_at=base)
    await writer.flush()
    assert len(persisted) == 0

    # 2. Bucket ending at :15 (phase MATCH, SHOULD submit)
    t_15 = base + timedelta(seconds=15)
    state_15_btc = MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        symbol="BTCUSDT",
        bucket_start=base,
        bucket_end=t_15,
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
        first_received_at=t_15,
        last_received_at=t_15,
    )
    coordinator.record_processed_state(state_15_btc, saved_at=t_15)
    await writer.flush()
    assert len(persisted) == 1
    assert persisted[0][2] == t_15

    # 3. Second symbol (ETHUSDT) in same :15 bucket -> deduplicated, does NOT double-submit
    state_15_eth = MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="live",
        symbol="ETHUSDT",
        bucket_start=base,
        bucket_end=t_15,
        open_price=Decimal("2000"),
        high_price=Decimal("2001"),
        low_price=Decimal("1999"),
        close_price=Decimal("2000"),
        trade_count=1,
        trade_notional=Decimal("200"),
        aggressive_buy_notional=Decimal("100"),
        aggressive_sell_notional=Decimal("100"),
        last_bid_price=Decimal("1999.9"),
        last_ask_price=Decimal("2000.1"),
        spread=Decimal("0.2"),
        midpoint=Decimal("2000"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("2000"),
        closed_kline_count=0,
        source_event_count=1,
        first_received_at=t_15,
        last_received_at=t_15,
    )
    coordinator.record_processed_state(state_15_eth, saved_at=t_15)
    await writer.flush()
    assert len(persisted) == 1

    # 4. Buckets at :30 and :45 -> should NOT submit
    t_30 = base + timedelta(seconds=30)
    coordinator.record_processed_state(
        MarketState15s(
            schema_version=1,
            exchange="binance-usdm",
            environment="live",
            symbol="BTCUSDT",
            bucket_start=t_15,
            bucket_end=t_30,
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
            first_received_at=t_30,
            last_received_at=t_30,
        ),
        saved_at=t_30,
    )
    await writer.flush()
    assert len(persisted) == 1

    # 5. Next cycle at :15 (second 75 = 1 min 15s) -> SHOULD submit again
    t_next_15 = base + timedelta(seconds=75)
    coordinator.record_processed_state(
        MarketState15s(
            schema_version=1,
            exchange="binance-usdm",
            environment="live",
            symbol="BTCUSDT",
            bucket_start=t_next_15 - timedelta(seconds=15),
            bucket_end=t_next_15,
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
            first_received_at=t_next_15,
            last_received_at=t_next_15,
        ),
        saved_at=t_next_15,
    )
    await writer.flush()
    assert len(persisted) == 2
    assert persisted[1][2] == t_next_15

    await coordinator.stop()


def test_coordinator_phase_validation() -> None:
    import pytest

    writer = CheckpointWriter(
        run_id="run-val",
        persist=lambda *args: None,  # type: ignore[arg-type]
    )
    strategy = _Strategy()

    with pytest.raises(ValueError, match="checkpoint_every_seconds must be positive"):
        LiveCheckpointCoordinator(
            writer=writer,
            strategy=strategy,
            checkpoint_every_seconds=0.0,
        )

    with pytest.raises(ValueError, match="checkpoint_phase_seconds must be in"):
        LiveCheckpointCoordinator(
            writer=writer,
            strategy=strategy,
            checkpoint_every_seconds=60.0,
            checkpoint_phase_seconds=-1.0,
        )

    with pytest.raises(ValueError, match="checkpoint_phase_seconds must be in"):
        LiveCheckpointCoordinator(
            writer=writer,
            strategy=strategy,
            checkpoint_every_seconds=60.0,
            checkpoint_phase_seconds=60.0,
        )


async def _persist(
    persisted: list[tuple[str, StrategyCheckpoint, datetime]],
    run_id: str,
    checkpoint: StrategyCheckpoint,
    saved_at: datetime,
) -> None:
    persisted.append((run_id, checkpoint, saved_at))
