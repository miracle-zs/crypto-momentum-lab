from datetime import UTC, datetime, timedelta

from local_optimization.simulation_ledger import SimulationLedger
from local_optimization.tests.test_opportunity_and_wfa_repair import (
    make_mock_opportunity,
)


def test_per_symbol_slots_allows_concurrent_different_symbols() -> None:
    ledger = SimulationLedger(notional_usdt=100.0, leverage=5.0)
    t0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    t_end = t0 + timedelta(hours=2)

    # Two concurrent opportunities for different symbols
    opp_btc = make_mock_opportunity(
        "opp_1",
        "BTCUSDT",
        t0 - timedelta(seconds=15),
        t0,
        100.0,
        t_end,
        101.0,
        net_pnl=0.9,
    )
    opp_eth = make_mock_opportunity(
        "opp_2",
        "ETHUSDT",
        t0 + timedelta(minutes=4, seconds=45),
        t0 + timedelta(minutes=5),
        100.0,
        t_end,
        101.0,
        net_pnl=0.9,
    )

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
    }

    # slots = 1 (per symbol), margin_cap = 280
    result, _ = ledger.simulate_window(
        opportunities=[opp_btc, opp_eth],
        params=params,
        window_start=t0 - timedelta(minutes=1),
        window_end=t_end + timedelta(minutes=1),
        max_concurrency=1,
        margin_cap=280.0,
    )

    # Both symbols should be admitted under per-symbol slots limit
    assert len(result.admitted_trades) == 2
    admitted_syms = {t.symbol for t in result.admitted_trades}
    assert admitted_syms == {"BTCUSDT", "ETHUSDT"}


def test_per_symbol_slots_rejects_excess_same_symbol() -> None:
    ledger = SimulationLedger(notional_usdt=100.0, leverage=5.0)
    t0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    t_end = t0 + timedelta(hours=2)

    # Two concurrent opportunities for the SAME symbol
    opp1 = make_mock_opportunity(
        "opp_1",
        "BTCUSDT",
        t0 - timedelta(seconds=15),
        t0,
        100.0,
        t_end,
        101.0,
        net_pnl=0.9,
    )
    opp2 = make_mock_opportunity(
        "opp_2",
        "BTCUSDT",
        t0 + timedelta(minutes=4, seconds=45),
        t0 + timedelta(minutes=5),
        100.0,
        t_end,
        101.0,
        net_pnl=0.9,
    )

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
    }

    # With slots=1, second should be rejected
    res1, _ = ledger.simulate_window(
        opportunities=[opp1, opp2],
        params=params,
        window_start=t0 - timedelta(minutes=1),
        window_end=t_end + timedelta(minutes=1),
        max_concurrency=1,
        margin_cap=280.0,
    )
    assert len(res1.admitted_trades) == 1
    assert res1.admitted_trades[0].trade_id == "opp_1"

    # With slots=2, both should be admitted
    res2, _ = ledger.simulate_window(
        opportunities=[opp1, opp2],
        params=params,
        window_start=t0 - timedelta(minutes=1),
        window_end=t_end + timedelta(minutes=1),
        max_concurrency=2,
        margin_cap=280.0,
    )
    assert len(res2.admitted_trades) == 2


def test_margin_cap_enforced_across_different_symbols() -> None:
    # margin per trade = 20 USDT
    ledger = SimulationLedger(notional_usdt=100.0, leverage=5.0)
    t0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    t_end = t0 + timedelta(hours=2)

    opp_btc = make_mock_opportunity(
        "opp_1",
        "BTCUSDT",
        t0 - timedelta(seconds=15),
        t0,
        100.0,
        t_end,
        101.0,
        net_pnl=0.9,
    )
    opp_eth = make_mock_opportunity(
        "opp_2",
        "ETHUSDT",
        t0 + timedelta(minutes=4, seconds=45),
        t0 + timedelta(minutes=5),
        100.0,
        t_end,
        101.0,
        net_pnl=0.9,
    )

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
    }

    # margin_cap = 30.0 (less than 20 + 20 = 40)
    result, _ = ledger.simulate_window(
        opportunities=[opp_btc, opp_eth],
        params=params,
        window_start=t0 - timedelta(minutes=1),
        window_end=t_end + timedelta(minutes=1),
        max_concurrency=2,
        margin_cap=30.0,
    )

    # First trade admitted (margin=20 <= 30), second rejected (20+20 = 40 > 30)
    assert len(result.admitted_trades) == 1
    assert result.admitted_trades[0].symbol == "BTCUSDT"


def test_batch_exit_submitted_releases_slot_before_fill() -> None:
    """Verifies that submitting an exit order frees the active batch slot,

    allowing a new batch of the same symbol to open even if the exit order
    is still pending fill.
    """
    ledger = SimulationLedger(notional_usdt=100.0, leverage=5.0)
    t0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    t_exit_sub = t0 + timedelta(minutes=15)  # 15m bearish candle triggers exit
    t_fill = t0 + timedelta(hours=2)  # Limit order filled or timeout 2h later

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
    }

    # Batch 1: First order at t0
    opp1 = make_mock_opportunity(
        "opp_b1_1",
        "BTCUSDT",
        t0 - timedelta(seconds=15),
        t0,
        100.0,
        exit_at=t_fill,
        exit_price=101.0,
        exit_submitted_at=t_exit_sub,
    )

    # Batch 1: Second order at t0 + 5m (before exit submission)
    opp2 = make_mock_opportunity(
        "opp_b1_2",
        "BTCUSDT",
        t0 + timedelta(minutes=4, seconds=45),
        t0 + timedelta(minutes=5),
        100.0,
        exit_at=t_fill,
        exit_price=101.0,
        exit_submitted_at=t_exit_sub,
    )

    # Batch 1: Third order at t0 + 10m (before exit submission, slots=2 exceeded)
    opp3 = make_mock_opportunity(
        "opp_b1_3",
        "BTCUSDT",
        t0 + timedelta(minutes=9, seconds=45),
        t0 + timedelta(minutes=10),
        100.0,
        exit_at=t_fill,
        exit_price=101.0,
        exit_submitted_at=t_exit_sub,
    )

    # Batch 2: First order at t0 + 20m (AFTER exit submission at t0+15m,
    # but BEFORE fill at t0+2h)
    opp4 = make_mock_opportunity(
        "opp_b2_1",
        "BTCUSDT",
        t0 + timedelta(minutes=19, seconds=45),
        t0 + timedelta(minutes=20),
        100.0,
        exit_at=t_fill + timedelta(hours=1),
        exit_price=102.0,
        exit_submitted_at=t_fill,
    )

    # Batch 2: Second order at t0 + 25m
    opp5 = make_mock_opportunity(
        "opp_b2_2",
        "BTCUSDT",
        t0 + timedelta(minutes=24, seconds=45),
        t0 + timedelta(minutes=25),
        100.0,
        exit_at=t_fill + timedelta(hours=1),
        exit_price=102.0,
        exit_submitted_at=t_fill,
    )

    # Batch 2: Third order at t0 + 28m (slots=2 in Batch 2 exceeded)
    opp6 = make_mock_opportunity(
        "opp_b2_3",
        "BTCUSDT",
        t0 + timedelta(minutes=27, seconds=45),
        t0 + timedelta(minutes=28),
        100.0,
        exit_at=t_fill + timedelta(hours=1),
        exit_price=102.0,
        exit_submitted_at=t_fill,
    )

    result, _ = ledger.simulate_window(
        opportunities=[opp1, opp2, opp3, opp4, opp5, opp6],
        params=params,
        window_start=t0 - timedelta(minutes=1),
        window_end=t_fill + timedelta(hours=2),
        max_concurrency=2,
        margin_cap=280.0,
    )

    admitted_ids = [t.trade_id for t in result.admitted_trades]
    # opp1, opp2 admitted (Batch 1, slots=2)
    # opp3 rejected (Batch 1 3rd order)
    # opp4, opp5 admitted (Batch 2, slots=2)
    # opp6 rejected (Batch 2 3rd order)
    assert admitted_ids == ["opp_b1_1", "opp_b1_2", "opp_b2_1", "opp_b2_2"]


def test_batch_exit_submitted_margin_still_occupied_until_fill() -> None:
    """Verifies that while slot is freed upon exit submission, margin remains

    occupied until the order actually fills, properly enforcing margin_cap.
    """
    ledger = SimulationLedger(
        notional_usdt=100.0, leverage=5.0
    )  # margin = 20 USDT per trade
    t0 = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    t_exit_sub = t0 + timedelta(minutes=15)
    t_fill = t0 + timedelta(hours=2)

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 0.0,
        "cooldown_buckets": 0,
    }

    # Batch 1: Order at t0, margin=20, exit submitted at t0+15m, fills at t0+2h
    opp1 = make_mock_opportunity(
        "opp_b1",
        "BTCUSDT",
        t0 - timedelta(seconds=15),
        t0,
        100.0,
        exit_at=t_fill,
        exit_price=101.0,
        exit_submitted_at=t_exit_sub,
    )

    # Batch 2: Order at t0+20m, margin=20
    opp2 = make_mock_opportunity(
        "opp_b2",
        "BTCUSDT",
        t0 + timedelta(minutes=19, seconds=45),
        t0 + timedelta(minutes=20),
        100.0,
        exit_at=t_fill + timedelta(hours=1),
        exit_price=102.0,
        exit_submitted_at=t_fill,
    )

    # If margin_cap = 30.0: Opp 1 occupies 20. At t0+20m Opp 1 has not filled,
    # so required margin is 20 + 20 = 40 > 30. Opp 2 must be rejected!
    res_tight, _ = ledger.simulate_window(
        opportunities=[opp1, opp2],
        params=params,
        window_start=t0 - timedelta(minutes=1),
        window_end=t_fill + timedelta(hours=2),
        max_concurrency=2,
        margin_cap=30.0,
    )
    assert len(res_tight.admitted_trades) == 1
    assert res_tight.admitted_trades[0].trade_id == "opp_b1"

    # If margin_cap = 50.0: Opp 2 is admitted!
    res_ample, _ = ledger.simulate_window(
        opportunities=[opp1, opp2],
        params=params,
        window_start=t0 - timedelta(minutes=1),
        window_end=t_fill + timedelta(hours=2),
        max_concurrency=2,
        margin_cap=50.0,
    )
    assert len(res_ample.admitted_trades) == 2
