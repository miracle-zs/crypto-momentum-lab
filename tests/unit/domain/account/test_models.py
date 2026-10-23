from datetime import datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account.models import (
    AccountPositionSnapshot,
)
from crypto_momentum_lab.persistence.postgres.execution_head_migration_snapshot import (
    stable_snapshot_anchor_id,
)


def test_account_position_snapshot_rejects_empty_symbol() -> None:
    with pytest.raises(ValueError, match="symbol must not be empty"):
        AccountPositionSnapshot(
            environment="live",
            account_label="primary",
            symbol=" ",
            position_side="BOTH",
            position_amt=Decimal("0"),
            entry_price=Decimal("0"),
            mark_price=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            notional=Decimal("0"),
            leverage=None,
            margin_type=None,
            observed_at=datetime.now().astimezone(),
            raw_payload={},
        )


def test_hedge_position_snapshots_have_distinct_migration_anchors() -> None:
    observed_at = datetime.now().astimezone()
    common = {
        "environment": "live",
        "account_label": "primary",
        "symbol": "BTCUSDT",
        "position_amt": Decimal("0.01"),
        "entry_price": Decimal("60000"),
        "mark_price": Decimal("60100"),
        "unrealized_pnl": Decimal("1"),
        "notional": Decimal("601"),
        "leverage": 1,
        "margin_type": "cross",
        "observed_at": observed_at,
        "raw_payload": {},
    }

    long_anchor = stable_snapshot_anchor_id(
        AccountPositionSnapshot(position_side="LONG", **common)
    )
    short_anchor = stable_snapshot_anchor_id(
        AccountPositionSnapshot(position_side="SHORT", **common)
    )

    assert long_anchor != short_anchor
