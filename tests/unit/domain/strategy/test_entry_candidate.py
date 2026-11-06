from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.strategy import (
    EntryType,
    OrderIntentCandidate,
    StrategySide,
)
from crypto_momentum_lab.domain.strategy.entry_candidate import prepare_entry_candidate
from tests.unit.persistence.postgres.test_runtime_state_repository import fixture_state


@pytest.mark.parametrize(
    ("side", "ask", "close", "mark", "midpoint", "existing", "expected"),
    [
        (StrategySide.LONG, "101", "100", "99", "98", None, "100"),
        (StrategySide.LONG, "99", "100", "98", "97", None, "99"),
        (StrategySide.LONG, "101", None, "99", "98", None, "101"),
        (StrategySide.SHORT, "101", "100", "99", "98", None, "100"),
        (StrategySide.LONG, None, None, "99", "98", None, "99"),
        (StrategySide.LONG, None, None, None, "98", None, "98"),
        (StrategySide.LONG, None, None, None, None, None, None),
        (StrategySide.LONG, "101", "100", "99", "98", "97", "97"),
    ],
)
def test_prepared_limit_price_preserves_priority(
    side, ask, close, mark, midpoint, existing, expected
) -> None:
    now = datetime(2026, 7, 3, tzinfo=UTC)
    candidate = OrderIntentCandidate(
        candidate_id="candidate",
        signal_id="signal",
        run_id="run",
        strategy_name="orderflow_impulse",
        strategy_version="v0",
        config_hash="hash",
        symbol="BTCUSDT",
        side=side,
        entry_type=EntryType.MARKET,
        limit_price=Decimal(existing) if existing else None,
        desired_notional=Decimal("100"),
        reduce_only=False,
        expires_at=now + timedelta(seconds=30),
        created_at=now,
        reason="test",
        features={},
    )
    state = replace(
        fixture_state("BTCUSDT", 0),
        last_ask_price=Decimal(ask) if ask else None,
        close_price=Decimal(close) if close else None,
        mark_price=Decimal(mark) if mark else None,
        midpoint=Decimal(midpoint) if midpoint else None,
    )
    prepared = prepare_entry_candidate(
        candidate,
        state=state,
        execution_now=now,
        entry_order_type=EntryType.LIMIT,
        limit_ttl_seconds=900,
    )
    assert prepared.limit_price == (Decimal(expected) if expected else None)
    assert prepared.expires_at == now + timedelta(seconds=900)
