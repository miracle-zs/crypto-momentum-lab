"""Account health reads the current execution head only."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql

from crypto_momentum_lab.domain.account.models import AccountFillEvent
from crypto_momentum_lab.domain.execution.execution_head import (
    EXECUTION_HEAD_SCHEMA_VERSION,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.projection_codec import encode_position_key
from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts
from crypto_momentum_lab.operator_dashboard.fact_integrity_queries import (
    load_fact_integrity,
)

NOW = datetime(2026, 10, 14, 0, 0, tzinfo=UTC)


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _Session:
    def __init__(self, heads):
        self.heads = heads
        self.statement = None

    async def scalars(self, statement):
        self.statement = statement
        return _Result(self.heads)


class _Sessions:
    def __init__(self, session):
        self.session = session

    def __call__(self):
        return self

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, *_args):
        return None


def _head(
    *, account: str, quantity: str, schema_version: int = EXECUTION_HEAD_SCHEMA_VERSION
):
    key = PositionKey("live", account, "BTCUSDT")
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="account-event-hub", stream_epoch="epoch-1"
    )
    at = NOW
    facts = AccountFacts(
        position_key=key,
        fills=(
            AccountFillEvent(
                environment="live",
                account_label=account,
                symbol="BTCUSDT",
                trade_id="trade-1",
                order_id="order-1",
                side="BUY",
                price=Decimal("100"),
                quantity=Decimal(quantity),
                realized_pnl=Decimal("0"),
                fee=Decimal("0"),
                fee_asset="USDT",
                trade_at=at,
                raw_payload={"positionSide": "BOTH"},
            ),
        ),
        stream_scope=scope,
    )
    payload = {
        "schema_version": schema_version,
        "position_key": encode_position_key(key),
        "stream_scope": {
            "stream_id": scope.stream_id,
            "stream_epoch": scope.stream_epoch,
        },
        "position_state": PositionBook.from_facts(facts).encode_current_state(),
        "active_reservation_ids": [],
        "recovery_command_ids": [],
        "external_recovery_ids": [],
    }
    return SimpleNamespace(
        account_label=account,
        symbol="BTCUSDT",
        position_side="BOTH",
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        updated_at=at,
        state_payload=payload,
    )


def test_current_exposure_without_fresh_coverage_is_unknown() -> None:
    session = _Session([_head(account="primary", quantity="1")])
    result = __import__("asyncio").run(
        load_fact_integrity(
            _Sessions(session),
            account_position_counts={"primary": 1},
            now=NOW,
        )
    )

    assert result["primary"].gaps is None
    assert result["primary"].reasons == ("coverage_missing",)
    compiled = session.statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert "execution_book_heads" in sql
    compiled_values = tuple(str(value) for value in compiled.params.values())
    assert "active_episode" in compiled_values
    assert {
        "active_reservation_ids",
        "recovery_command_ids",
        "external_recovery_ids",
    } <= set(compiled_values)


def test_legacy_head_fails_closed_when_account_has_exposure() -> None:
    session = _Session([_head(account="primary", quantity="1", schema_version=1)])
    result = __import__("asyncio").run(
        load_fact_integrity(
            _Sessions(session),
            account_position_counts={"primary": 1},
            now=NOW,
        )
    )

    assert result["primary"].gaps is None
    assert "active_position_count_mismatch" in result["primary"].reasons


def test_missing_head_is_unknown_only_when_exchange_reports_a_position() -> None:
    session = _Session([])
    result = __import__("asyncio").run(
        load_fact_integrity(
            _Sessions(session),
            account_position_counts={"flat": 0, "open": 1},
            now=NOW,
        )
    )

    assert result["flat"].gaps == 0
    assert result["open"].gaps is None
    assert "active_position_count_mismatch" in result["open"].reasons


@pytest.mark.asyncio
async def test_empty_position_counts_do_not_open_a_session() -> None:
    class _NeverOpen:
        def __call__(self):
            raise AssertionError("empty position counts should return immediately")

    assert (
        await load_fact_integrity(_NeverOpen(), account_position_counts={}, now=NOW)
        == {}
    )
