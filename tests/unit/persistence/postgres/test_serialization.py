from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.persistence.postgres.serialization import jsonable


class _State(StrEnum):
    READY = "ready"


def test_jsonable_preserves_persistence_encoding_contract() -> None:
    value = jsonable(
        {
            "state": _State.READY,
            "amount": Decimal("100.5000"),
            "observed_at": datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
            "items": (Decimal("2"),),
        }
    )

    assert value == {
        "state": "ready",
        "amount": "100.5",
        "observed_at": "2026-07-04T00:00:00+00:00",
        "items": ["2"],
    }
