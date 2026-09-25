"""Tests for Phase 2 dynamic price-grid collision exits and decoupled RawOpportunity."""

from datetime import UTC, datetime, timedelta

from local_optimization.opportunity import RawOpportunity
from local_optimization.simulation_ledger import SimulationLedger


def make_test_opportunity(
    opp_id: str = "opp_dyn_1",
    symbol: str = "BTCUSDT",
    entry_time: datetime | None = None,
    entry_price: float = 100.0,
    exit_time: datetime | None = None,
    exit_price: float | None = None,
) -> RawOpportunity:
    if entry_time is None:
        entry_time = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    detected_at = entry_time - timedelta(seconds=15)
    return RawOpportunity(
        opportunity_id=opp_id,
        symbol=symbol,
        direction="LONG",
        detected_at=detected_at,
        detected_epoch=detected_at.timestamp(),
        entry_eligible_at=entry_time,
        entry_reference_price=entry_price,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=0.01,
        aggressive_imbalance=0.5,
        confirmation_min_imbalance=0.5,
        notional_intensity=2.0,
        volume_ratio=1.2,
        exit_time=exit_time,
        exit_price=exit_price,
    )


def test_fallback_to_static_exit_when_no_dynamic_params() -> None:
    ledger = SimulationLedger()
    t0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    t_exit = t0 + timedelta(minutes=15)
    opp = make_test_opportunity(
        entry_time=t0,
        entry_price=100.0,
        exit_time=t_exit,
        exit_price=102.0,
    )

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.005,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
    }

    res, _ = ledger.simulate_window(
        opportunities=[opp],
        params=params,
        window_start=t0 - timedelta(minutes=1),
        window_end=t_exit + timedelta(minutes=1),
        price_series=None,
    )

    assert len(res.admitted_trades) == 1
    t = res.admitted_trades[0]
    assert t.exit_time == t_exit
    assert t.exit_price == 102.0


def test_dynamic_stop_loss_exit() -> None:
    ledger = SimulationLedger()
    t0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    opp = make_test_opportunity(
        entry_time=t0,
        entry_price=100.0,
        exit_time=None,  # Decoupled exit time!
        exit_price=None,
    )

    # 15s price series: 100.0 -> 99.5 -> 98.0 (drops below 2% stop loss) -> 97.0
    ts = [
        t0.timestamp(),
        (t0 + timedelta(seconds=15)).timestamp(),
        (t0 + timedelta(seconds=30)).timestamp(),
        (t0 + timedelta(seconds=45)).timestamp(),
    ]
    ps = [100.0, 99.5, 98.0, 97.0]
    price_series = {"BTCUSDT": (ts, ps)}

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.005,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "stop_loss_pct": 0.02,  # 2% stop loss: 98.0
    }

    res, _ = ledger.simulate_window(
        opportunities=[opp],
        params=params,
        window_start=t0 - timedelta(minutes=1),
        window_end=t0 + timedelta(hours=1),
        price_series=price_series,
    )

    assert len(res.admitted_trades) == 1
    t = res.admitted_trades[0]
    assert t.exit_time == t0 + timedelta(seconds=30)
    assert t.exit_price == 98.0  # Hit stop loss exactly


def test_dynamic_take_profit_exit() -> None:
    ledger = SimulationLedger()
    t0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    opp = make_test_opportunity(
        entry_time=t0,
        entry_price=100.0,
        exit_time=None,
        exit_price=None,
    )

    # 15s price series: 100.0 -> 101.5 -> 103.5 (crosses 3% take profit)
    ts = [
        t0.timestamp(),
        (t0 + timedelta(seconds=15)).timestamp(),
        (t0 + timedelta(seconds=30)).timestamp(),
    ]
    ps = [100.0, 101.5, 103.5]
    price_series = {"BTCUSDT": (ts, ps)}

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.005,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "take_profit_pct": 0.03,  # 3% take profit: 103.0
    }

    res, _ = ledger.simulate_window(
        opportunities=[opp],
        params=params,
        window_start=t0 - timedelta(minutes=1),
        window_end=t0 + timedelta(hours=1),
        price_series=price_series,
    )

    assert len(res.admitted_trades) == 1
    t = res.admitted_trades[0]
    assert t.exit_time == t0 + timedelta(seconds=30)
    assert t.exit_price == 103.0  # Take profit hit


def test_dynamic_max_holding_seconds_exit() -> None:
    ledger = SimulationLedger()
    t0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    opp = make_test_opportunity(
        entry_time=t0,
        entry_price=100.0,
        exit_time=None,
        exit_price=None,
    )

    # 15s price series: steady 100.5
    ts = [
        t0.timestamp(),
        (t0 + timedelta(seconds=30)).timestamp(),
        (t0 + timedelta(seconds=60)).timestamp(),
        (t0 + timedelta(seconds=90)).timestamp(),
    ]
    ps = [100.0, 100.5, 101.0, 101.2]
    price_series = {"BTCUSDT": (ts, ps)}

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.005,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "max_holding_seconds": 60,  # 60 seconds time stop
    }

    res, _ = ledger.simulate_window(
        opportunities=[opp],
        params=params,
        window_start=t0 - timedelta(minutes=1),
        window_end=t0 + timedelta(hours=1),
        price_series=price_series,
    )

    assert len(res.admitted_trades) == 1
    t = res.admitted_trades[0]
    assert t.exit_time == t0 + timedelta(seconds=60)
    assert t.exit_price == 101.0


def test_dynamic_exit_with_close_on_third_signal() -> None:
    ledger = SimulationLedger()
    t0 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    t1 = t0 + timedelta(minutes=1)
    t2 = t0 + timedelta(minutes=2)

    # 3 sequential opportunities for same symbol with long timeout
    opp1 = make_test_opportunity("opp1", "BTCUSDT", t0, 100.0)
    opp2 = make_test_opportunity("opp2", "BTCUSDT", t1, 101.0)
    opp3 = make_test_opportunity("opp3", "BTCUSDT", t2, 102.0)

    # Price series with 1h duration so dynamic timeout doesn't trigger early
    ts = [(t0 + timedelta(seconds=15 * k)).timestamp() for k in range(300)]
    ps = [100.0 + k * 0.01 for k in range(300)]
    price_series = {"BTCUSDT": (ts, ps)}

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.005,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "max_holding_seconds": 3600,
    }

    # slots = 2, close_on_third_signal = True
    res, _ = ledger.simulate_window(
        opportunities=[opp1, opp2, opp3],
        params=params,
        window_start=t0 - timedelta(minutes=1),
        window_end=t0 + timedelta(hours=2),
        price_series=price_series,
        max_concurrency=2,
        close_on_third_signal=True,
    )

    # First 2 are admitted, 3rd triggers flattening of both at t2!
    assert len(res.admitted_trades) == 2
    for t in res.admitted_trades:
        assert t.exit_time == t2

