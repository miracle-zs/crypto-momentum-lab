from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from crypto_momentum_lab.next_gen.dashboard import NextGenDashboardQueries


def test_exit_path_uses_decision_recorded_before_each_order():
    started_at = datetime(2026, 10, 22, 1, 0, tzinfo=UTC)
    decisions = {
        "batch-1": [
            (started_at, "candle_15m_bearish"),
            (started_at + timedelta(minutes=10), "scheduled_risk_window"),
        ]
    }
    limit_order = SimpleNamespace(
        batch_id="batch-1",
        created_at=started_at + timedelta(seconds=1),
        order_type="LIMIT",
    )
    market_order = SimpleNamespace(
        batch_id="batch-1",
        created_at=started_at + timedelta(minutes=10, seconds=1),
        order_type="MARKET",
    )
    prior_limit = SimpleNamespace(
        order_type="LIMIT",
        created_at=limit_order.created_at,
        status="CANCELED",
    )

    limit_reason = NextGenDashboardQueries._exit_reason_for_order(
        limit_order, decisions
    )
    market_reason = NextGenDashboardQueries._exit_reason_for_order(
        market_order, decisions
    )

    assert (
        NextGenDashboardQueries._close_path(
            limit_order, [prior_limit], limit_reason, 7200
        )
        == "宽限期内止盈"
    )
    assert (
        NextGenDashboardQueries._close_path(
            market_order, [prior_limit], market_reason, 7200
        )
        == "定时清仓"
    )
