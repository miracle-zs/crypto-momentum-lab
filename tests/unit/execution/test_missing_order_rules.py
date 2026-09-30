"""Operator resolution safety rules independent of CLI and database setup."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.execution.missing_order_rules import (
    validate_missing_order_resolution,
)


def test_guard_accepts_confirmed_absent_reduce_only_order() -> None:
    validate_missing_order_resolution(
        state="unknown_pending_reconciliation",
        reduce_only=True,
        exchange_order_id=None,
        created_at=datetime(2026, 8, 31, 4, 0, tzinfo=UTC),
        now=datetime(2026, 8, 31, 4, 20, tzinfo=UTC),
        order_quantity=Decimal("2159.3"),
        executed_quantity=Decimal("0"),
        position_quantity=Decimal("2159.3"),
        exchange_order_found=False,
        matching_open_order_found=False,
        min_missing_age_seconds=600,
    )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"state": "filled"}, "not pending reconciliation"),
        ({"exchange_order_id": "recorded"}, "already recorded"),
        ({"executed_quantity": Decimal("1")}, "non-zero"),
        ({"reduce_only": False}, "reduce-only"),
        ({"exchange_order_found": True}, "still exists"),
        ({"matching_open_order_found": True}, "open order"),
        ({"position_quantity": Decimal("2000")}, "position quantity changed"),
        (
            {"now": datetime(2026, 8, 31, 4, 5, tzinfo=UTC)},
            "younger than",
        ),
    ],
)
def test_missing_order_resolution_guard_fails_closed(
    overrides: dict[str, object],
    message: str,
) -> None:
    values: dict[str, object] = {
        "state": "unknown_pending_reconciliation",
        "reduce_only": True,
        "exchange_order_id": None,
        "created_at": datetime(2026, 8, 31, 4, 0, tzinfo=UTC),
        "now": datetime(2026, 8, 31, 4, 20, tzinfo=UTC),
        "order_quantity": Decimal("2159.3"),
        "executed_quantity": Decimal("0"),
        "position_quantity": Decimal("2159.3"),
        "exchange_order_found": False,
        "matching_open_order_found": False,
        "min_missing_age_seconds": 600,
    }
    values.update(overrides)

    with pytest.raises(RuntimeError, match=message):
        validate_missing_order_resolution(**values)  # type: ignore[arg-type]
