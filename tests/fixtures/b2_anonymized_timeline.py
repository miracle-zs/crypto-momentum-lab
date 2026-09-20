"""B2USDT anonymized real-incident event timeline fixture.

Captures the complete sequence of events from 2026-09-19 to 2026-09-20:
- Multi-batch scaling entries
- System exits
- External manual full close (Zero-crossing)
- Post-zero new entry (New batch life-cycle)
- Subsequent exits and dust remaining
- External dust close

Used as an authoritative verification fixture for PositionLedger and batch reconstruction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import AccountFillEvent
from crypto_momentum_lab.domain.execution import (
    ExchangeOrderState,
    FuturesPositionSide,
    PositionObservation,
    PositionOrderFact,
)
from crypto_momentum_lab.domain.strategy import StrategySide


@dataclass(frozen=True, slots=True)
class B2TimelineEntry:
    timestamp: datetime
    event_type: str  # "fill" | "snapshot"
    side: str  # "BUY" | "SELL" | "LONG"
    quantity: Decimal
    price: Decimal
    order_id: str
    is_system_order: bool
    position_after: Decimal
    note: str


B2_TIMELINE: tuple[B2TimelineEntry, ...] = (
    # Phase 1: Entry scaling
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 19, 11, 37, 13, tzinfo=UTC),
        event_type="fill",
        side="BUY",
        quantity=Decimal("120"),
        price=Decimal("1.00"),
        order_id="b2_sys_ord_1",
        is_system_order=True,
        position_after=Decimal("120"),
        note="Initial long entry",
    ),
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 19, 12, 54, 33, tzinfo=UTC),
        event_type="fill",
        side="BUY",
        quantity=Decimal("127"),
        price=Decimal("1.02"),
        order_id="b2_sys_ord_2",
        is_system_order=True,
        position_after=Decimal("247"),
        note="Scaling add",
    ),
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 19, 13, 5, 14, tzinfo=UTC),
        event_type="fill",
        side="BUY",
        quantity=Decimal("127"),
        price=Decimal("1.05"),
        order_id="b2_sys_ord_3",
        is_system_order=True,
        position_after=Decimal("374"),
        note="Scaling add; total 374",
    ),
    # Phase 2: First system partial exit
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 19, 14, 0, 5, tzinfo=UTC),
        event_type="fill",
        side="SELL",
        quantity=Decimal("120"),
        price=Decimal("1.08"),
        order_id="b2_sys_ord_4",
        is_system_order=True,
        position_after=Decimal("254"),
        note="System exit 120; remaining 254",
    ),
    # Phase 3: External manual close -> Zero crossing!
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 19, 14, 47, 3, tzinfo=UTC),
        event_type="fill",
        side="SELL",
        quantity=Decimal("254"),
        price=Decimal("1.07"),
        order_id="b2_ext_ord_823890995",
        is_system_order=False,
        position_after=Decimal("0"),
        note="External manual sell 254; position zeroed out",
    ),
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 19, 14, 47, 5, tzinfo=UTC),
        event_type="snapshot",
        side="LONG",
        quantity=Decimal("0"),
        price=Decimal("0"),
        order_id="",
        is_system_order=False,
        position_after=Decimal("0"),
        note="Position snapshot reaches 0",
    ),
    # Phase 4: Post-zero new entry -> Brand new lifecycle!
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 19, 15, 59, 30, tzinfo=UTC),
        event_type="fill",
        side="BUY",
        quantity=Decimal("172"),
        price=Decimal("1.01"),
        order_id="b2_sys_ord_5",
        is_system_order=True,
        position_after=Decimal("172"),
        note="Post-zero new entry; must not inherit pre-zero batches",
    ),
    # Phase 5: System exits immediately after
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 19, 15, 59, 32, tzinfo=UTC),
        event_type="fill",
        side="SELL",
        quantity=Decimal("112"),
        price=Decimal("1.015"),
        order_id="b2_sys_ord_6",
        is_system_order=True,
        position_after=Decimal("60"),
        note="System partial exit 112",
    ),
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 19, 15, 59, 33, tzinfo=UTC),
        event_type="fill",
        side="SELL",
        quantity=Decimal("22"),
        price=Decimal("1.015"),
        order_id="b2_sys_ord_7",
        is_system_order=True,
        position_after=Decimal("38"),
        note="System partial exit 22; remaining 38",
    ),
    # Phase 6: Next day add
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 20, 0, 29, 0, tzinfo=UTC),
        event_type="fill",
        side="BUY",
        quantity=Decimal("174"),
        price=Decimal("1.03"),
        order_id="b2_sys_ord_8",
        is_system_order=True,
        position_after=Decimal("212"),
        note="Re-entry add 174; total 212",
    ),
    # Phase 7: Subsequent exits leaving dust
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 20, 1, 0, 1, tzinfo=UTC),
        event_type="fill",
        side="SELL",
        quantity=Decimal("167"),
        price=Decimal("1.04"),
        order_id="b2_sys_ord_9",
        is_system_order=True,
        position_after=Decimal("45"),
        note="System exit 167; remaining 45",
    ),
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 20, 1, 59, 31, tzinfo=UTC),
        event_type="fill",
        side="SELL",
        quantity=Decimal("38"),
        price=Decimal("1.02"),
        order_id="b2_sys_ord_10",
        is_system_order=True,
        position_after=Decimal("7"),
        note="System exit 38; remaining 7 (dust lot)",
    ),
    # Phase 8: External dust close
    B2TimelineEntry(
        timestamp=datetime(2026, 9, 20, 8, 20, 28, tzinfo=UTC),
        event_type="fill",
        side="SELL",
        quantity=Decimal("7"),
        price=Decimal("0.99"),
        order_id="b2_ext_ord_837473466",
        is_system_order=False,
        position_after=Decimal("0"),
        note="External dust sell 7; position zeroed again",
    ),
)


def get_b2_account_fill_events(
    *,
    environment: str = "live",
    account_label: str = "account-3",
    symbol: str = "B2USDT",
) -> tuple[AccountFillEvent, ...]:
    """Generate chronological AccountFillEvent stream for B2USDT timeline."""
    fills: list[AccountFillEvent] = []
    trade_idx = 1000
    for entry in B2_TIMELINE:
        if entry.event_type != "fill":
            continue
        trade_idx += 1
        fills.append(
            AccountFillEvent(
                environment=environment,
                account_label=account_label,
                symbol=symbol,
                trade_id=f"t_{trade_idx}",
                order_id=entry.order_id,
                side=entry.side,
                price=entry.price,
                quantity=entry.quantity,
                realized_pnl=Decimal("0.0"),
                fee=Decimal("0.01"),
                fee_asset="USDT",
                trade_at=entry.timestamp,
                raw_payload={
                    "order_id": entry.order_id,
                    "is_system": entry.is_system_order,
                    "note": entry.note,
                },
            )
        )
    return tuple(fills)


def get_b2_system_order_facts(
    *,
    symbol: str = "B2USDT",
) -> tuple[PositionOrderFact, ...]:
    """Generate PositionOrderFact for system orders in B2USDT timeline.

    External orders are intentionally excluded, representing the real-world condition
    where exchange_orders table only contains orders initiated by the trading daemon.
    """
    facts: list[PositionOrderFact] = []
    for entry in B2_TIMELINE:
        if entry.event_type != "fill" or not entry.is_system_order:
            continue
        facts.append(
            PositionOrderFact(
                symbol=symbol,
                position_side=FuturesPositionSide.BOTH,
                side=entry.side,
                reduce_only=(entry.side == "SELL"),
                order_type="MARKET",
                quantity=entry.quantity,
                executed_quantity=entry.quantity,
                state=ExchangeOrderState.FILLED,
                client_order_id=f"c_{entry.order_id}",
                exchange_order_id=entry.order_id,
                created_at=entry.timestamp,
                updated_at=entry.timestamp,
                price=entry.price,
                plan=None,
                exit_batch_id=None,
                legacy_exit_attribution=False,
            )
        )
    return tuple(facts)


def get_b2_position_observation(
    *,
    symbol: str = "B2USDT",
    position_amt: Decimal,
    entry_price: Decimal = Decimal("1.02"),
) -> PositionObservation:
    """Create a PositionObservation snapshot."""
    return PositionObservation(
        symbol=symbol,
        side=StrategySide.LONG if position_amt > 0 else StrategySide.FLAT,
        position_side=FuturesPositionSide.BOTH,
        position_amt=position_amt,
        entry_price=entry_price,
    )
