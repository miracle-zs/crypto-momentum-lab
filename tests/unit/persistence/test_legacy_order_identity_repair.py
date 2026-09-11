from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from crypto_momentum_lab.persistence.postgres.legacy_order_identity_repair import (
    build_legacy_identity_report,
)


def test_legacy_identity_report_reconstructs_each_exchange_attempt() -> None:
    first_at = datetime(2026, 9, 8, 0, 28, 47, tzinfo=UTC)
    second_at = first_at + timedelta(seconds=1)
    orders = [
        SimpleNamespace(
            client_order_id="reused-exit",
            symbol="龙虾USDT",
            side="SELL",
            reduce_only=True,
            exchange_order_id="778660681",
            quantity=Decimal("1371"),
            executed_quantity=Decimal("1371"),
        )
    ]
    events = [
        SimpleNamespace(
            client_order_id="reused-exit",
            exchange_order_id="778660371",
            occurred_at=first_at,
            details={"executed_quantity": "1371"},
        ),
        SimpleNamespace(
            client_order_id="reused-exit",
            exchange_order_id="778660681",
            occurred_at=second_at,
            details={"executed_quantity": "415"},
        ),
    ]
    fills = [
        SimpleNamespace(order_id="778660371", quantity=Decimal("1371")),
        SimpleNamespace(order_id="778660681", quantity=Decimal("415")),
    ]

    report = build_legacy_identity_report(
        orders,
        events,
        fills,
        run_id="live-b1-long-100u-5x-v1",
        account_label="primary",
        environment="live",
    )

    assert len(report) == 1
    assert report[0]["reconstructible"] is True
    assert report[0]["exchange_order_ids"] == ["778660371", "778660681"]
    assert [
        attempt["reconstructed_quantity"]
        for attempt in report[0]["attempts"]
    ] == ["1371", "415"]


def test_legacy_identity_report_marks_missing_attempt_quantity_unresolved() -> None:
    orders = [
        SimpleNamespace(
            client_order_id="reused-exit",
            symbol="龙虾USDT",
            side="SELL",
            reduce_only=True,
            exchange_order_id="exchange-b",
            quantity=Decimal("100"),
            executed_quantity=Decimal("100"),
        )
    ]
    events = [
        SimpleNamespace(
            client_order_id="reused-exit",
            exchange_order_id="exchange-a",
            occurred_at=datetime(2026, 9, 8, tzinfo=UTC),
            details={},
        ),
        SimpleNamespace(
            client_order_id="reused-exit",
            exchange_order_id="exchange-b",
            occurred_at=datetime(2026, 9, 8, 0, 0, 1, tzinfo=UTC),
            details={"executed_quantity": "100"},
        ),
    ]

    report = build_legacy_identity_report(
        orders,
        events,
        [],
        run_id="run",
        account_label="primary",
        environment="live",
    )

    assert report[0]["reconstructible"] is False
