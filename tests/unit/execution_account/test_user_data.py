import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.execution_account.binance.user_data import (
    BinancePayloadError,
    parse_user_data_event,
)
from crypto_momentum_lab.execution_account.sync import AccountSnapshot
from crypto_momentum_lab.execution_account.user_data_sync import (
    AccountUserDataState,
)


def test_parse_user_data_event_is_deterministic_and_preserves_payload() -> None:
    payload = {
        "e": "ACCOUNT_UPDATE",
        "E": 1783123200123,
        "T": 1783123200000,
        "a": {"m": "ORDER", "B": [], "P": []},
    }

    first = parse_user_data_event(
        json.dumps(payload),
        received_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )
    second = parse_user_data_event(
        json.dumps(payload, separators=(",", ":")),
        received_at=datetime(2026, 7, 4, 0, 0, 0, 1, tzinfo=UTC),
    )

    assert first.event_type == "ACCOUNT_UPDATE"
    assert first.event_at == datetime(2026, 7, 4, 0, 0, 0, 123000, tzinfo=UTC)
    assert first.payload["a"] == payload["a"]
    assert first.event_id == second.event_id


def test_parse_user_data_event_accepts_listen_key_expired() -> None:
    event = parse_user_data_event(
        {"e": "listenKeyExpired", "E": 1783123200000},
        received_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )

    assert event.event_type == "listenKeyExpired"


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"e": "ACCOUNT_UPDATE"},
        {"e": "ACCOUNT_UPDATE", "E": "not-a-timestamp"},
        {"e": "ACCOUNT_UPDATE", "E": 1783123200000, "a": []},
    ],
)
def test_parse_user_data_event_rejects_malformed_payload(payload) -> None:
    with pytest.raises(BinancePayloadError):
        parse_user_data_event(payload)


def test_account_user_data_state_merges_partial_account_update() -> None:
    received_at = datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC)
    state = AccountUserDataState(_initial_snapshot())

    update = state.apply(
        parse_user_data_event(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 1783123201000,
                "T": 1783123201000,
                "a": {
                    "m": "ORDER",
                    "B": [{"a": "USDT", "wb": "120", "cw": "95", "bc": "20"}],
                    "P": [
                        {
                            "s": "BTCUSDT",
                            "pa": "0.002",
                            "ep": "50000",
                            "up": "1.25",
                            "mt": "cross",
                            "ps": "BOTH",
                        }
                    ],
                },
            },
            received_at=received_at,
        )
    )

    assert update.needs_reconciliation is False
    assert update.changed is True
    assert update.fills == ()
    balance = update.snapshot.balances[0]
    position = update.snapshot.positions[0]
    assert balance.wallet_balance == Decimal("120")
    assert balance.available_balance == Decimal("80")
    assert position.position_amt == Decimal("0.002")
    assert position.unrealized_pnl == Decimal("1.25")
    assert position.mark_price == Decimal("51000")
    assert position.notional == Decimal("102")
    assert {item.observed_at for item in update.snapshot.positions} == {received_at}


def test_account_user_data_state_deduplicates_trade_event_and_closes_order() -> None:
    state = AccountUserDataState(_initial_snapshot())
    payload = {
        "e": "ORDER_TRADE_UPDATE",
        "E": 1783123201000,
        "T": 1783123201000,
        "o": {
            "s": "BTCUSDT",
            "c": "entry-1",
            "S": "BUY",
            "o": "LIMIT",
            "q": "0.002",
            "p": "50000",
            "x": "TRADE",
            "X": "PARTIALLY_FILLED",
            "i": 1001,
            "t": 5001,
            "z": "0.001",
            "l": "0.001",
            "L": "50100",
            "rp": "0.1",
            "n": "0.01",
            "N": "USDT",
            "T": 1783123201000,
            "R": False,
        },
    }

    first = state.apply(
        parse_user_data_event(
            payload,
            received_at=datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC),
        )
    )
    duplicate = state.apply(
        parse_user_data_event(
            payload,
            received_at=datetime(2026, 7, 4, 0, 0, 2, tzinfo=UTC),
        )
    )

    assert len(first.fills) == 1
    assert first.fills[0].trade_id == "5001"
    assert first.fills[0].quantity == Decimal("0.001")
    assert first.snapshot.open_orders[0].status == "PARTIALLY_FILLED"
    assert duplicate.changed is False
    assert duplicate.fills == ()

    closed = state.apply(
        parse_user_data_event(
            {
                **payload,
                "E": 1783123203000,
                "o": {
                    **payload["o"],
                    "E": 1783123203000,
                    "x": "CANCELED",
                    "X": "CANCELED",
                },
            },
            received_at=datetime(2026, 7, 4, 0, 0, 3, tzinfo=UTC),
        )
    )
    assert closed.snapshot.open_orders == ()


def test_account_user_data_state_requests_reconcile_for_unknown_position() -> None:
    state = AccountUserDataState(_initial_snapshot())

    update = state.apply(
        parse_user_data_event(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 1783123201000,
                "a": {
                    "B": [],
                    "P": [
                        {
                            "s": "ETHUSDT",
                            "pa": "0.5",
                            "ep": "3000",
                            "up": "0",
                            "mt": "cross",
                            "ps": "BOTH",
                        }
                    ],
                },
            }
        )
    )

    assert update.needs_reconciliation is True
    assert update.reason == "unknown_position"


def _initial_snapshot() -> AccountSnapshot:
    observed_at = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    return AccountSnapshot(
        config=AccountConfigSnapshot(
            environment="live",
            account_label="primary",
            multi_assets_mode=False,
            hedge_mode=False,
            can_trade=True,
            fee_tier=0,
            observed_at=observed_at,
            raw_payload={},
        ),
        balances=(
            AccountBalanceSnapshot(
                environment="live",
                account_label="primary",
                asset="USDT",
                wallet_balance=Decimal("100"),
                available_balance=Decimal("80"),
                unrealized_pnl=Decimal("0"),
                observed_at=observed_at,
                raw_payload={},
            ),
        ),
        positions=(
            AccountPositionSnapshot(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                position_side="BOTH",
                position_amt=Decimal("0.002"),
                entry_price=Decimal("50000"),
                mark_price=Decimal("51000"),
                unrealized_pnl=Decimal("2"),
                notional=Decimal("102"),
                leverage=5,
                margin_type="cross",
                observed_at=observed_at,
                raw_payload={},
            ),
        ),
        open_orders=(
            AccountOpenOrderSnapshot(
                environment="live",
                account_label="primary",
                symbol="BTCUSDT",
                order_id="1001",
                client_order_id="entry-1",
                side="BUY",
                order_type="LIMIT",
                status="NEW",
                price=Decimal("50000"),
                original_quantity=Decimal("0.002"),
                executed_quantity=Decimal("0"),
                reduce_only=False,
                observed_at=observed_at,
                raw_payload={},
            ),
        ),
    )
