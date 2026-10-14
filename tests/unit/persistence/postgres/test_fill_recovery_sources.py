from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from crypto_momentum_lab.domain.account import AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.domain.execution.execution_head import (
    EXECUTION_HEAD_SCHEMA_VERSION,
)
from crypto_momentum_lab.domain.execution.position_book import PositionBook
from crypto_momentum_lab.domain.execution.projection_codec import encode_position_key
from crypto_momentum_lab.domain.execution.recovery_models import AccountFacts
from crypto_momentum_lab.persistence.postgres import fill_recovery_sources


async def test_fill_recovery_anchor_comes_from_current_head():
    now = datetime(2026, 10, 14, tzinfo=UTC)
    key = PositionKey("live", "primary", "BTCUSDT", FuturesPositionSide.LONG)
    scope = AccountFactStreamScope.for_position_key(
        key, stream_id="hub", stream_epoch="current"
    )
    snapshot = AccountPositionSnapshot(
        environment=key.environment,
        account_label=key.account_label,
        symbol=key.symbol,
        position_side=key.position_side.value,
        position_amt=Decimal("0"),
        entry_price=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        mark_price=Decimal("1"),
        notional=Decimal("0"),
        leverage=5,
        margin_type="cross",
        observed_at=now,
        raw_payload={"positionSide": "LONG"},
    )
    facts = AccountFacts(position_key=key, stream_scope=scope, snapshots=(snapshot,))
    head = SimpleNamespace(
        symbol=key.symbol,
        position_side=key.position_side.value,
        stream_id=scope.stream_id,
        stream_epoch=scope.stream_epoch,
        state_payload={
            "schema_version": EXECUTION_HEAD_SCHEMA_VERSION,
            "position_key": encode_position_key(key),
            "stream_scope": {
                "stream_id": scope.stream_id,
                "stream_epoch": scope.stream_epoch,
            },
            "position_state": PositionBook.from_facts(facts).encode_current_state(),
        },
    )
    session = AsyncMock()
    session.scalars.return_value = Mock(all=Mock(return_value=[head]))
    manager = AsyncMock()
    manager.__aenter__.return_value = session
    sources = await fill_recovery_sources.load_fill_recovery_sources(
        Mock(return_value=manager), environment="live", account_label="primary"
    )
    anchor = sources[("BTCUSDT", "LONG")]
    assert anchor is not None
    assert anchor.event_cut == now
    assert anchor.zero_snapshot == snapshot
    session.scalars.assert_awaited_once()
