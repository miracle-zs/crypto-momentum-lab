import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.strategy import StrategyDecision
from crypto_momentum_lab.live_rollout.signal_recorder import (
    LiveStrategySignalRecorder,
)
from crypto_momentum_lab.live_rollout.volume import QuoteVolume24hSnapshot
from tests.unit.shadow_operation.test_service import (
    _signal,
    _state,
)


class FakeQuoteVolumeProvider:
    def __init__(self, snapshot: QuoteVolume24hSnapshot) -> None:
        self._snapshot = snapshot

    def snapshot(
        self,
        symbol: str,
        *,
        as_of: datetime,
    ) -> QuoteVolume24hSnapshot | None:
        del symbol, as_of
        return self._snapshot


@pytest.mark.asyncio
async def test_records_24h_quote_volume_and_signal_context() -> None:
    persisted: list[dict[str, object]] = []

    async def persist(batch) -> None:
        persisted.extend(batch)

    detected_at = datetime(2026, 7, 4, 0, 0, 20, tzinfo=UTC)
    volume = QuoteVolume24hSnapshot(
        symbol="BTCUSDT",
        quote_volume=Decimal("123456.78"),
        source_at=detected_at - timedelta(seconds=2),
        fetched_at=detected_at - timedelta(seconds=1),
    )
    recorder = LiveStrategySignalRecorder(
        run_id="run-1",
        account_label="primary",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="a" * 64,
        code_commit="b" * 40,
        quote_volume_provider=FakeQuoteVolumeProvider(volume),
        persist=persist,
    )
    await recorder.start()
    recorder.record_decision(
        decision=StrategyDecision(
            signals=(replace(_signal(), detected_at=detected_at),),
            candidates=(),
            rejections=(),
        ),
        state=_state(),
        recorded_at=detected_at + timedelta(milliseconds=10),
        account_context={"account_state": "ready_readonly"},
        filter_context={
            "entry_enabled": True,
            "ema5": Decimal("99"),
            "universe": {
                "snapshot_id": "snapshot-1",
                "snapshot_observed_at": detected_at - timedelta(seconds=5),
                "utc_day_return": Decimal("0.123"),
                "gainer_rank": 7,
            },
            "effective_entry_candidates": {
                "candidate-1": {
                    "effective_limit_price": Decimal("100"),
                    "effective_expires_at": detected_at
                    + timedelta(minutes=15),
                },
            },
        },
    )
    await recorder.stop()

    assert len(persisted) == 1
    row = persisted[0]
    assert row["quote_volume_24h"] == Decimal("123456.78")
    assert row["quote_volume_24h_quote_asset"] == "USDT"
    assert row["quote_volume_24h_source"] == "binance_fapi_ticker_24hr"
    assert row["quote_volume_24h_age_ms"] == 1000
    assert row["filter_context"]["entry_enabled"] is True
    assert row["market_context"]["trade_notional"] == "100"
    assert row["filter_context"]["universe"]["utc_day_return"] == "0.123"
    assert (
        row["filter_context"]["universe"]["snapshot_observed_at"]
        == (detected_at - timedelta(seconds=5)).isoformat()
    )
    assert (
        row["filter_context"]["effective_entry_candidates"]["candidate-1"][
            "effective_limit_price"
        ]
        == "100"
    )


@pytest.mark.asyncio
async def test_recorder_is_best_effort_when_queue_is_full_or_persist_fails() -> None:
    persisted: list[dict[str, object]] = []

    async def persist(batch) -> None:
        del batch
        raise TimeoutError("observability unavailable")

    signal = _signal()
    second_signal = replace(signal, signal_id="signal-2")
    recorder = LiveStrategySignalRecorder(
        run_id="run-1",
        account_label="primary",
        strategy_name="compression_breakout",
        strategy_version="v1",
        config_hash="a" * 64,
        code_commit="b" * 40,
        persist=persist,
        queue_size=1,
    )
    await recorder.start()
    recorder.record_decision(
        decision=StrategyDecision(
            signals=(signal, second_signal),
            candidates=(),
            rejections=(),
        ),
        state=_state(),
        recorded_at=datetime(2026, 7, 4, 0, 0, 21, tzinfo=UTC),
        account_context={},
        filter_context={},
    )
    await recorder.stop()

    assert not persisted
    assert recorder.recorded_count == 2
    assert recorder.dropped_count == 1
    assert recorder.persist_failure_count == 1
    assert recorder.build_failure_count == 0
    await asyncio.sleep(0)
