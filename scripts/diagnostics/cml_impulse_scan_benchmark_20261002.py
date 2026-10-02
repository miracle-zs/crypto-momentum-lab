"""Offline comparison of the volume-eligible scan and its previous bound."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from statistics import median
from time import perf_counter

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.strategies.order_flow_impulse import event_study
from crypto_momentum_lab.strategies.order_flow_impulse.runtime import (
    _latest_event_for_state,
)


def main() -> None:
    config = event_study.OrderFlowImpulseConfig(
        impulse_window_buckets=2,
        baseline_window_buckets=4,
        breakout_window_buckets=4,
        min_return_pct=Decimal("0.0075"),
        min_aggressive_imbalance=Decimal("0.30"),
        min_notional_intensity=Decimal("3"),
        confirmation_buckets=1,
        cooldown_buckets=0,
        forward_horizon_buckets=(1,),
        min_notional_5m_vs_30m=Decimal("1.25"),
    )
    origin = datetime(2026, 10, 2, tzinfo=UTC)
    base = MarketState15s(
        schema_version=1,
        exchange="binance-usdm",
        environment="research",
        symbol="BTCUSDT",
        bucket_start=origin,
        bucket_end=origin + timedelta(seconds=15),
        open_price=Decimal("100"),
        high_price=Decimal("100"),
        low_price=Decimal("100"),
        close_price=Decimal("100"),
        trade_count=10,
        trade_notional=Decimal("100"),
        aggressive_buy_notional=Decimal("50"),
        aggressive_sell_notional=Decimal("50"),
        last_bid_price=Decimal("99.99"),
        last_ask_price=Decimal("100.01"),
        spread=Decimal("0.02"),
        midpoint=Decimal("100"),
        liquidation_count=0,
        liquidation_notional=Decimal("0"),
        mark_price=Decimal("100"),
        closed_kline_count=0,
        source_event_count=10,
        first_received_at=origin,
        last_received_at=origin,
    )
    states = tuple(
        replace(
            base,
            bucket_start=origin + timedelta(seconds=i * 15),
            bucket_end=origin + timedelta(seconds=(i + 1) * 15),
        )
        for i in range(156)
    )
    current_bound = event_study._first_candidate_index

    def legacy_bound(cfg: event_study.OrderFlowImpulseConfig) -> int:
        return max(
            cfg.baseline_window_buckets + cfg.impulse_window_buckets - 1,
            cfg.breakout_window_buckets,
        )

    timings: dict[str, list[float]] = {"legacy": [], "optimized": []}
    outputs = {}
    try:
        for repetition in range(7):
            order = (
                ("legacy", "optimized")
                if repetition % 2 == 0
                else ("optimized", "legacy")
            )
            for name in order:
                event_study._first_candidate_index = (
                    legacy_bound if name == "legacy" else current_bound
                )
                t0 = perf_counter()
                for _ in range(430):
                    outputs[name] = _latest_event_for_state(states, config, states[-1])
                timings[name].append((perf_counter() - t0) * 1000 / 430)
    finally:
        event_study._first_candidate_index = current_bound
    assert outputs["legacy"] == outputs["optimized"]
    medians = {name: median(times) for name, times in timings.items()}
    print(
        json.dumps(
            {
                "states": len(states),
                "iterations_per_repeat": 430,
                "repeats": 7,
                "same_result": True,
                "candidate_indices_legacy": len(states) - legacy_bound(config),
                "candidate_indices_optimized": len(states) - current_bound(config),
                "median_ms_per_symbol": medians,
                "speedup": medians["legacy"] / medians["optimized"],
                "timings_ms_per_symbol": timings,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
