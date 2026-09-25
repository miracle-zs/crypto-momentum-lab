"""Unit tests for scheduled risk window simulation and ledger integration."""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
    ScheduledRiskWindowConfig,
)
from local_optimization.opportunity import OpportunityStatus, RawOpportunity
from local_optimization.simulation_ledger import SimulationLedger

SH_TZ = ZoneInfo("Asia/Shanghai")


def test_scheduled_risk_window_entry_allowed() -> None:
    config = ScheduledRiskWindowConfig(
        timezone="Asia/Shanghai",
        entry_stop_at=time(7, 45),
        flatten_start_at=time(7, 45),
        reopen_at=time(9, 0),
    )

    # 07:30 Beijing time -> allowed
    t1 = datetime(2026, 9, 10, 7, 30, tzinfo=SH_TZ).astimezone(UTC)
    assert config.is_entry_allowed(t1) is True

    # 07:45:00 Beijing time -> blocked
    t2 = datetime(2026, 9, 10, 7, 45, 0, tzinfo=SH_TZ).astimezone(UTC)
    assert config.is_entry_allowed(t2) is False

    # 08:30:00 Beijing time -> blocked
    t3 = datetime(2026, 9, 10, 8, 30, 0, tzinfo=SH_TZ).astimezone(UTC)
    assert config.is_entry_allowed(t3) is False

    # 08:59:59 Beijing time -> blocked
    t4 = datetime(2026, 9, 10, 8, 59, 59, tzinfo=SH_TZ).astimezone(UTC)
    assert config.is_entry_allowed(t4) is False

    # 09:00:00 Beijing time -> allowed
    t5 = datetime(2026, 9, 10, 9, 0, 0, tzinfo=SH_TZ).astimezone(UTC)
    assert config.is_entry_allowed(t5) is True

    # 15:00:00 Beijing time -> allowed
    t6 = datetime(2026, 9, 10, 15, 0, 0, tzinfo=SH_TZ).astimezone(UTC)
    assert config.is_entry_allowed(t6) is True


def test_scheduled_risk_window_next_flatten_time() -> None:
    config = ScheduledRiskWindowConfig(
        timezone="Asia/Shanghai",
        entry_stop_at=time(7, 45),
        flatten_start_at=time(7, 45),
        reopen_at=time(9, 0),
    )

    # Entry at 03:00 Beijing time -> next flatten is 07:45 today
    t_entry = datetime(2026, 9, 10, 3, 0, tzinfo=SH_TZ)
    expected_flat = datetime(2026, 9, 10, 7, 45, tzinfo=SH_TZ)
    assert config.next_flatten_time(t_entry) == expected_flat

    # Entry in UTC at 23:30 (07:30 Beijing time next morning)
    t_utc = datetime(2026, 9, 9, 23, 30, tzinfo=UTC)
    flat_utc = config.next_flatten_time(t_utc)
    assert flat_utc.astimezone(SH_TZ) == datetime(2026, 9, 10, 7, 45, tzinfo=SH_TZ)
    assert flat_utc.tzinfo == UTC

    # Entry at 09:30 Beijing time -> next flatten is 07:45 tomorrow morning
    t_entry_post = datetime(2026, 9, 10, 9, 30, tzinfo=SH_TZ)
    expected_flat_next_day = datetime(2026, 9, 11, 7, 45, tzinfo=SH_TZ)
    assert config.next_flatten_time(t_entry_post) == expected_flat_next_day


def test_simulation_ledger_scheduled_risk_window_entry_rejection() -> None:
    config = ScheduledRiskWindowConfig(
        timezone="Asia/Shanghai",
        entry_stop_at=time(7, 45),
        flatten_start_at=time(7, 45),
        reopen_at=time(9, 0),
    )
    ledger = SimulationLedger(initial_cash=1000.0, leverage=5.0, notional_usdt=100.0)

    # Opportunity inside risk window (08:00 Beijing time)
    t_inside = datetime(2026, 9, 10, 8, 0, tzinfo=SH_TZ).astimezone(UTC)
    opp_inside = RawOpportunity(
        opportunity_id="opp_inside_1",
        symbol="BTCUSDT",
        direction="LONG",
        detected_at=t_inside - timedelta(seconds=15),
        detected_epoch=(t_inside - timedelta(seconds=15)).timestamp(),
        entry_eligible_at=t_inside,
        entry_reference_price=50000.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=1.0,
        aggressive_imbalance=0.5,
        confirmation_min_imbalance=0.5,
        notional_intensity=3.5,
        volume_ratio=1.5,
        exit_time=t_inside + timedelta(minutes=15),
        exit_price=50500.0,
    )

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 1.0,
        "cooldown_buckets": 0,
        "max_open_positions": 2,
    }

    # With scheduled risk window: REJECTED
    res_with_window, _ = ledger.simulate_window(
        opportunities=[opp_inside],
        params=params,
        window_start=datetime(2026, 9, 10, 0, 0, tzinfo=UTC),
        window_end=datetime(2026, 9, 11, 0, 0, tzinfo=UTC),
        scheduled_risk_window=config,
    )
    assert len(res_with_window.admitted_trades) == 0
    assert len(res_with_window.executions) == 1
    assert res_with_window.executions[0].status == OpportunityStatus.ENTRY_REJECTED

    # Without scheduled risk window: ENTERED
    res_without_window, _ = ledger.simulate_window(
        opportunities=[opp_inside],
        params=params,
        window_start=datetime(2026, 9, 10, 0, 0, tzinfo=UTC),
        window_end=datetime(2026, 9, 11, 0, 0, tzinfo=UTC),
        scheduled_risk_window=None,
    )
    assert len(res_without_window.admitted_trades) == 1
    assert res_without_window.executions[0].status == OpportunityStatus.ENTERED


def test_simulation_ledger_scheduled_flatten_with_price_series() -> None:
    config = ScheduledRiskWindowConfig(
        timezone="Asia/Shanghai",
        entry_stop_at=time(7, 45),
        flatten_start_at=time(7, 45),
        reopen_at=time(9, 0),
    )
    ledger = SimulationLedger(initial_cash=1000.0, leverage=5.0, notional_usdt=100.0)

    # Opportunity entering at 07:00 Beijing time, normal exit would be 08:30
    t_entry = datetime(2026, 9, 10, 7, 0, tzinfo=SH_TZ).astimezone(UTC)
    t_normal_exit = datetime(2026, 9, 10, 8, 30, tzinfo=SH_TZ).astimezone(UTC)
    t_flatten = datetime(2026, 9, 10, 7, 45, tzinfo=SH_TZ).astimezone(UTC)

    opp = RawOpportunity(
        opportunity_id="opp_flatten_1",
        symbol="ETHUSDT",
        direction="LONG",
        detected_at=t_entry - timedelta(seconds=15),
        detected_epoch=(t_entry - timedelta(seconds=15)).timestamp(),
        entry_eligible_at=t_entry,
        entry_reference_price=2000.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=1.0,
        aggressive_imbalance=0.5,
        confirmation_min_imbalance=0.5,
        notional_intensity=3.5,
        volume_ratio=1.5,
        exit_time=t_normal_exit,
        exit_price=2050.0,  # normal exit price at 08:30
    )

    # Price series with price at 07:45 = 2020.0
    price_series = {
        "ETHUSDT": (
            [t_entry.timestamp(), t_flatten.timestamp(), t_normal_exit.timestamp()],
            [2000.0, 2020.0, 2050.0],
        )
    }

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 1.0,
        "cooldown_buckets": 0,
        "max_open_positions": 2,
    }

    res, _ = ledger.simulate_window(
        opportunities=[opp],
        params=params,
        window_start=datetime(2026, 9, 9, 0, 0, tzinfo=UTC),
        window_end=datetime(2026, 9, 11, 0, 0, tzinfo=UTC),
        price_series=price_series,
        scheduled_risk_window=config,
    )

    assert len(res.admitted_trades) == 1
    trade = res.admitted_trades[0]

    # Verify exit time was truncated to 07:45:00 UTC
    assert trade.exit_time == t_flatten
    # Verify exit price was queried from price_series at 07:45:00
    assert trade.exit_price == 2020.0

    # Verify net PnL: notional = 100, price 2000 -> 2020 (+1%), fees & slippage
    # gross = 100 * (2020/2000 - 1) = 1.00
    # fee = 100 * 0.0005 + 101 * 0.0005 = 0.05 + 0.0505 = 0.1005
    # slip = 100 * 0.0002 + 101 * 0.0002 = 0.02 + 0.0202 = 0.0402
    # net = 1.00 - 0.1005 - 0.0402 = 0.8593
    assert pytest.approx(trade.calculated_net_pnl, rel=1e-3) == 0.8593


def test_simulation_ledger_slot_release_after_flatten() -> None:
    config = ScheduledRiskWindowConfig(
        timezone="Asia/Shanghai",
        entry_stop_at=time(7, 45),
        flatten_start_at=time(7, 45),
        reopen_at=time(9, 0),
    )
    ledger = SimulationLedger(initial_cash=1000.0, leverage=5.0, notional_usdt=100.0)

    # Slot limit = 1 per symbol
    # Trade 1: Enters 07:00 Beijing, normal exit 10:00 (crosses 07:45)
    t1_ent = datetime(2026, 9, 10, 7, 0, tzinfo=SH_TZ).astimezone(UTC)
    t1_exit = datetime(2026, 9, 10, 10, 0, tzinfo=SH_TZ).astimezone(UTC)

    # Trade 2: Enters 09:15 Beijing (after reopening)
    t2_ent = datetime(2026, 9, 10, 9, 15, tzinfo=SH_TZ).astimezone(UTC)
    t2_exit = datetime(2026, 9, 10, 10, 15, tzinfo=SH_TZ).astimezone(UTC)

    opp1 = RawOpportunity(
        opportunity_id="opp_1",
        symbol="SOLUSDT",
        direction="LONG",
        detected_at=t1_ent - timedelta(seconds=15),
        detected_epoch=(t1_ent - timedelta(seconds=15)).timestamp(),
        entry_eligible_at=t1_ent,
        entry_reference_price=100.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=1.0,
        aggressive_imbalance=0.5,
        confirmation_min_imbalance=0.5,
        notional_intensity=3.5,
        volume_ratio=1.5,
        exit_time=t1_exit,
        exit_price=102.0,
    )

    opp2 = RawOpportunity(
        opportunity_id="opp_2",
        symbol="SOLUSDT",
        direction="LONG",
        detected_at=t2_ent - timedelta(seconds=15),
        detected_epoch=(t2_ent - timedelta(seconds=15)).timestamp(),
        entry_eligible_at=t2_ent,
        entry_reference_price=103.0,
        impulse_window_buckets=2,
        confirmation_buckets=1,
        impulse_return_pct=1.0,
        aggressive_imbalance=0.5,
        confirmation_min_imbalance=0.5,
        notional_intensity=3.5,
        volume_ratio=1.5,
        exit_time=t2_exit,
        exit_price=105.0,
    )

    params = {
        "impulse_window_buckets": 2,
        "confirmation_buckets": 1,
        "min_return_pct": 0.5,
        "min_imbalance": 0.3,
        "min_intensity": 1.5,
        "min_volume_ratio": 1.0,
        "cooldown_buckets": 0,
        "max_open_positions": 1,  # Strict 1 slot!
    }

    # WITH scheduled window: opp1 flattened at 07:45, freeing slot,
    # so opp2 is ADMITTED at 09:15
    res, _ = ledger.simulate_window(
        opportunities=[opp1, opp2],
        params=params,
        window_start=datetime(2026, 9, 9, 0, 0, tzinfo=UTC),
        window_end=datetime(2026, 9, 11, 0, 0, tzinfo=UTC),
        max_concurrency=1,
        scheduled_risk_window=config,
    )
    assert len(res.admitted_trades) == 2
    assert res.admitted_trades[0].exit_time == datetime(
        2026, 9, 10, 7, 45, tzinfo=SH_TZ
    ).astimezone(UTC)
    assert res.admitted_trades[1].entry_time == t2_ent

    # WITHOUT scheduled window: opp1 holds until 10:00, blocking opp2 at 09:15
    # (slot full)
    res_no_win, _ = ledger.simulate_window(
        opportunities=[opp1, opp2],
        params=params,
        window_start=datetime(2026, 9, 9, 0, 0, tzinfo=UTC),
        window_end=datetime(2026, 9, 11, 0, 0, tzinfo=UTC),
        max_concurrency=1,
        scheduled_risk_window=None,
    )
    assert len(res_no_win.admitted_trades) == 1
    assert res_no_win.executions[1].status == OpportunityStatus.ENTRY_REJECTED
