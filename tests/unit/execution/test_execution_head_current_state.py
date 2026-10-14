"""Execution observations persist one current state per position."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution.command_models import ExecutionScope
from crypto_momentum_lab.domain.execution.evidence_models import ExecutionEvidence
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.execution_head import (
    decode_execution_head_state,
    is_supported_execution_head_schema,
)
from crypto_momentum_lab.domain.execution.observation_models import Applied, Duplicate
from crypto_momentum_lab.domain.execution.ports import ExecutionHeadSnapshot
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec

START = datetime(2026, 9, 29, tzinfo=UTC)


def _fill(index: int) -> AccountFillEvent:
    return AccountFillEvent(
        environment="live",
        account_label="primary",
        symbol="BTCUSDT",
        trade_id=str(index),
        order_id=f"order-{index}",
        side="BUY",
        price=Decimal("65000.25"),
        quantity=Decimal("0.001"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0"),
        fee_asset="USDT",
        trade_at=START + timedelta(seconds=index),
        raw_payload={"positionSide": "BOTH"},
    )


class _FakeTx:
    def __init__(self) -> None:
        self.head: ExecutionHeadSnapshot | None = None
        self.persist_count = 0

    async def load_head(self, _key: Any) -> ExecutionHeadSnapshot | None:
        return self.head

    async def load_checkpoint_from_head(self, **_kwargs: Any) -> None:
        return None

    async def record_evidence(self, **_kwargs: Any) -> bool:
        return True

    async def record_trade(self, **_kwargs: Any) -> bool:
        return True

    async def persist_watermark(self, **_kwargs: Any) -> None:
        return None

    async def persist_head(self, **kwargs: Any) -> int:
        self.persist_count += 1
        revision = kwargs["expected_revision"] + 1
        self.head = ExecutionHeadSnapshot(
            revision=revision,
            stream_id=kwargs["stream_id"],
            stream_epoch=kwargs["stream_epoch"],
            projection_version=kwargs["projection_version"],
            state_payload=kwargs["state_payload"],
        )
        return revision


class _FakeUow:
    def __init__(self) -> None:
        self.tx = _FakeTx()

    @asynccontextmanager
    async def transaction(self, _key: Any):
        yield self.tx


async def test_observation_advances_current_head_without_fill_history() -> None:
    uow = _FakeUow()
    book = ExecutionBook(execution_unit_of_work=uow)
    book._persistence_failed = False
    scope = ExecutionScope(
        environment="live", account_label="primary", symbol="BTCUSDT"
    )

    first = await book.observe(
        ExecutionEvidence(
            evidence_id="ev-1",
            scope=scope,
            observed_at=START,
            fill=_fill(1),
            stream_id="trade-source",
            stream_epoch="one",
            sequence=1,
        )
    )
    assert isinstance(first, Applied)
    payload = uow.tx.head.state_payload
    assert payload["schema_version"] == 4
    first_position_state = payload["position_state"]
    assert "active_batches" not in first_position_state["projection"]
    assert "total_active_quantity" not in first_position_state["projection"]
    first_projection = PositionRecoveryCodec.decode_projection(
        first_position_state["projection"]
    )
    assert first_projection.total_active_quantity == Decimal("0.001")
    assert (
        PositionRecoveryCodec.decode_facts(first_position_state["verification"]).fills
        == ()
    )

    legacy_payload = dict(payload)
    legacy_payload["schema_version"] = 3
    assert not is_supported_execution_head_schema(3)
    with pytest.raises(ValueError, match="unsupported execution head schema"):
        decode_execution_head_state(
            legacy_payload,
            key=first_projection.position_key,
            scope=AccountFactStreamScope.for_position_key(
                first_projection.position_key,
                stream_id="trade-source",
                stream_epoch="one",
            ),
        )

    second = await book.observe(
        ExecutionEvidence(
            evidence_id="ev-2",
            scope=scope,
            observed_at=START + timedelta(seconds=2),
            fill=_fill(2),
            stream_id="trade-source",
            stream_epoch="one",
            sequence=2,
        )
    )
    assert isinstance(second, Applied)
    second_state = uow.tx.head.state_payload["position_state"]
    second_projection = PositionRecoveryCodec.decode_projection(
        second_state["projection"]
    )
    assert second_projection.total_active_quantity == Decimal("0.002")
    assert PositionRecoveryCodec.decode_facts(second_state["verification"]).fills == ()
    assert uow.tx.persist_count == 2

    repeated = await book.observe(
        ExecutionEvidence(
            evidence_id="ev-3",
            scope=scope,
            observed_at=START + timedelta(seconds=3),
            fill=_fill(2),
            stream_id="trade-source",
            stream_epoch="one",
            sequence=3,
        )
    )
    assert isinstance(repeated, (Applied, Duplicate))
    assert uow.tx.persist_count == 2
    assert uow.tx.head.state_payload["position_state"] == second_state
