import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone, tzinfo
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.execution_account.binance.user_data_parser import (
    BinancePayloadError,
    parse_user_data_event,
)
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectation,
    AccountPositionExpectationRegistry,
)
from crypto_momentum_lab.execution_account.snapshot_changes import (
    apply_account_snapshot_delta,
    build_account_snapshot,
)
from crypto_momentum_lab.execution_account.snapshot_models import (
    AccountSnapshot,
)
from crypto_momentum_lab.execution_account.user_data_models import (
    UserDataStateError,
)
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
    assert first.exchange_event_at == datetime(
        2026,
        7,
        4,
        0,
        0,
        tzinfo=UTC,
    )
    assert first.payload["a"] == payload["a"]
    assert first.event_id == second.event_id


def test_parse_user_data_event_preserves_optional_exchange_update_watermark() -> None:
    event = parse_user_data_event(
        {
            "e": "ACCOUNT_UPDATE",
            "E": 1783123200000,
            "T": 1783123200000,
            "u": 42,
            "pu": 41,
            "a": {"B": [], "P": []},
        },
        received_at=datetime(2026, 7, 4, 0, 0, tzinfo=UTC),
    )

    assert event.exchange_update_id == 42
    assert event.exchange_previous_update_id == 41


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
    assert update.delta is not None
    assert update.delta.balances[0].wallet_balance == Decimal("120")
    assert update.delta.positions[0].position_amt == Decimal("0.002")
    assert apply_account_snapshot_delta(_initial_snapshot(), update.delta) == (
        update.snapshot
    )


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
    assert closed.delta is not None
    assert closed.delta.removed_open_orders == (("BTCUSDT", "1001"),)


def test_account_user_data_state_uses_exchange_time_for_order_ordering() -> None:
    state = AccountUserDataState(_initial_snapshot())
    first = {
        "e": "ORDER_TRADE_UPDATE",
        "E": 1783123203000,
        "T": 1783123203000,
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
            "t": 5002,
            "z": "0.001",
            "l": "0.001",
            "L": "50100",
            "rp": "0.1",
            "n": "0.01",
            "N": "USDT",
            "R": False,
        },
    }
    state.apply(
        parse_user_data_event(
            first,
            received_at=datetime(2026, 7, 4, 0, 0, 3, tzinfo=UTC),
        )
    )

    stale = state.apply(
        parse_user_data_event(
            {
                **first,
                "E": 1783123202000,
                "T": 1783123202000,
                "o": {**first["o"], "X": "CANCELED", "x": "CANCELED"},
            },
            # The local receive time is newer, but the exchange transaction
            # time is older and must not overwrite the newer order state.
            received_at=datetime(2026, 7, 4, 0, 0, 4, tzinfo=UTC),
        )
    )

    assert stale.needs_reconciliation is True
    assert stale.reason == "stale_exchange_event"
    assert stale.snapshot.open_orders[0].status == "PARTIALLY_FILLED"


def test_account_user_data_state_rejects_exchange_update_gap() -> None:
    state = AccountUserDataState(_initial_snapshot())
    base = {
        "e": "ACCOUNT_UPDATE",
        "E": 1783123201000,
        "T": 1783123201000,
        "a": {"B": [], "P": []},
    }
    state.apply(parse_user_data_event({**base, "u": 10}))

    with pytest.raises(UserDataStateError, match="not contiguous"):
        state.apply(
            parse_user_data_event(
                {
                    **base,
                    "E": 1783123202000,
                    "T": 1783123202000,
                    "u": 12,
                    "pu": 9,
                }
            )
        )


def test_account_user_data_trade_deduplication_cache_is_bounded() -> None:
    state = AccountUserDataState(_initial_snapshot())

    for index in range(state._SEEN_TRADE_CACHE_SIZE + 1):
        state._remember_trade(("BTCUSDT", str(index)))

    assert len(state._seen_trade_ids) == state._SEEN_TRADE_CACHE_SIZE
    assert ("BTCUSDT", "0") not in state._seen_trade_id_set
    assert ("BTCUSDT", str(state._SEEN_TRADE_CACHE_SIZE)) in (state._seen_trade_id_set)


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


def test_account_user_data_state_accepts_registered_entry_position() -> None:
    observed_at = datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC)
    registry = AccountPositionExpectationRegistry(
        environment="live",
        account_label="primary",
        clock=lambda: observed_at,
    )
    registry.register(
        AccountPositionExpectation(
            environment="live",
            account_label="primary",
            symbol="ETHUSDT",
            position_side="BOTH",
            client_order_id="entry-eth-1",
            side="BUY",
            quantity=Decimal("0.5"),
            created_at=observed_at,
            expires_at=datetime(2026, 7, 4, 0, 1, tzinfo=UTC),
        )
    )
    state = AccountUserDataState(
        _initial_snapshot(),
        expected_position_registry=registry,
    )

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
            },
            received_at=observed_at,
        )
    )

    assert update.needs_reconciliation is False
    assert update.reason is None
    position = next(
        item for item in update.snapshot.positions if item.symbol == "ETHUSDT"
    )
    assert position.mark_price == Decimal("3000")
    assert position.notional == Decimal("1500")


def test_no_fill_terminal_order_discards_registered_entry() -> None:
    observed_at = datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC)
    registry = AccountPositionExpectationRegistry(
        environment="live",
        account_label="primary",
        clock=lambda: observed_at,
    )
    registry.register(
        AccountPositionExpectation(
            environment="live",
            account_label="primary",
            symbol="ETHUSDT",
            position_side="BOTH",
            client_order_id="entry-cancelled",
            side="BUY",
            quantity=Decimal("0.5"),
            created_at=observed_at,
            expires_at=observed_at + timedelta(minutes=15),
        )
    )
    state = AccountUserDataState(
        _initial_snapshot(),
        expected_position_registry=registry,
    )

    update = state.apply(
        parse_user_data_event(
            {
                "e": "ORDER_TRADE_UPDATE",
                "E": 1783123201000,
                "a": {},
                "o": {
                    "s": "ETHUSDT",
                    "i": 2001,
                    "c": "entry-cancelled",
                    "S": "BUY",
                    "o": "LIMIT",
                    "q": "0.5",
                    "p": "3000",
                    "x": "CANCELED",
                    "X": "CANCELED",
                    "z": "0",
                    "R": False,
                },
            },
            received_at=observed_at,
        )
    )

    assert update.changed is True
    assert registry.pending_count == 0


def _initial_snapshot() -> AccountSnapshot:
    observed_at = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    return AccountSnapshot(
        config=AccountConfigSnapshot(
            environment="live",
            account_label="primary",
            multi_assets_mode=False,
            hedge_mode=False,
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


@pytest.mark.parametrize("zone", [UTC, timezone(timedelta(hours=8))])
def test_snapshot_builder_orders_values_and_preserves_original_records(zone):
    source = _initial_snapshot()
    balance = source.balances[0]
    position = source.positions[0]
    order = source.open_orders[0]
    balances = [balance, replace(balance, asset="BTC")]
    positions = [
        replace(position, symbol="ETHUSDT"),
        replace(position, position_side="SHORT"),
        replace(position, position_side="LONG"),
    ]
    orders = [
        replace(order, symbol="ETHUSDT"),
        replace(order, order_id="2"),
        replace(order, order_id="10"),
    ]
    original = (tuple(balances), tuple(positions), tuple(orders))
    observed_at = datetime(2026, 10, 1, tzinfo=zone)
    result = build_account_snapshot(
        source.config,
        balances=iter(balances),
        positions=iter(positions),
        open_orders=iter(orders),
        observed_at=observed_at,
    )
    assert [item.asset for item in result.balances] == ["BTC", "USDT"]
    assert [(item.symbol, item.position_side) for item in result.positions] == [
        ("BTCUSDT", "LONG"),
        ("BTCUSDT", "SHORT"),
        ("ETHUSDT", "BOTH"),
    ]
    assert [(item.symbol, item.order_id) for item in result.open_orders] == [
        ("BTCUSDT", "10"),
        ("BTCUSDT", "2"),
        ("ETHUSDT", "1001"),
    ]
    assert result.config.observed_at == observed_at
    for item in (*result.balances, *result.positions, *result.open_orders):
        assert item.observed_at == observed_at
        assert item.observed_at.tzinfo is zone
    assert result.positions[0].position_amt == position.position_amt
    assert result.open_orders[0].original_quantity == order.original_quantity
    assert result.balances[1].raw_payload is balance.raw_payload
    assert result.config is not source.config
    assert (tuple(balances), tuple(positions), tuple(orders)) == original
    assert source.config.observed_at == datetime(2026, 7, 4, tzinfo=UTC)


def test_snapshot_builder_accepts_empty_values():
    source = _initial_snapshot()
    observed_at = datetime(2026, 10, 1, tzinfo=UTC)
    result = build_account_snapshot(
        source.config,
        balances=(),
        positions=(),
        open_orders=(),
        observed_at=observed_at,
    )
    assert result.balances == result.positions == result.open_orders == ()
    assert result.config.observed_at == observed_at


class _NoUtcOffset(tzinfo):
    def utcoffset(self, dt):
        return None


@pytest.mark.parametrize("zone", [None, _NoUtcOffset()])
def test_snapshot_builder_rejects_missing_timezone_before_consuming_values(zone):
    def unexpected_values():
        raise AssertionError("invalid time must fail before consuming account values")
        yield

    with pytest.raises(ValueError, match="observed_at must be timezone-aware"):
        build_account_snapshot(
            _initial_snapshot().config,
            balances=unexpected_values(),
            positions=unexpected_values(),
            open_orders=unexpected_values(),
            observed_at=datetime(2026, 10, 1, tzinfo=zone),
        )


@pytest.mark.parametrize("replace_baseline", [False, True])
@pytest.mark.parametrize(
    "balance_time,position_time,expected_balance,expected_position",
    [
        (1783123202000, 1783123202000, "100", "0.002"),
        (1783123202000, 1783123200000, "100", "0.003"),
        (1783123200000, 1783123202000, "90", "0.002"),
        (1783123200000, 1783123200000, "90", "0.003"),
    ],
)
def test_rest_entity_times_prevent_delayed_ws_rollback(
    replace_baseline,
    balance_time,
    position_time,
    expected_balance,
    expected_position,
) -> None:
    original = _initial_snapshot()
    baseline = replace(
        original,
        balances=(
            replace(original.balances[0], raw_payload={"updateTime": balance_time}),
        ),
        positions=(
            replace(original.positions[0], raw_payload={"updateTime": position_time}),
        ),
    )
    state = AccountUserDataState(original if replace_baseline else baseline)
    if replace_baseline:
        state.replace_snapshot(baseline)
    event = parse_user_data_event(
        {
            "e": "ACCOUNT_UPDATE",
            "E": 1783123203000,
            "T": 1783123201000,
            "a": {
                "B": [{"a": "USDT", "wb": "90", "cw": "70"}],
                "P": [
                    {
                        "s": "BTCUSDT",
                        "ps": "BOTH",
                        "pa": "0.003",
                        "ep": "49000",
                        "up": "3",
                        "mt": "cross",
                    }
                ],
            },
        },
        received_at=original.config.observed_at + timedelta(seconds=4),
    )
    update = state.apply(event)
    assert update.snapshot.balances[0].wallet_balance == Decimal(expected_balance)
    assert update.snapshot.positions[0].position_amt == Decimal(expected_position)
    assert update.snapshot.balances[0].available_balance == Decimal("80")
    assert not update.needs_reconciliation
    assert update.changed == (expected_balance == "90" or expected_position == "0.003")
    assert not state.apply(event).changed


@pytest.mark.parametrize("baseline_time", [None, 0, True, "1783123202000", -1])
def test_invalid_rest_time_does_not_suppress_account_event(baseline_time) -> None:
    original = _initial_snapshot()
    state = AccountUserDataState(
        replace(
            original,
            balances=(
                replace(
                    original.balances[0],
                    raw_payload={"updateTime": baseline_time},
                ),
            ),
        )
    )
    update = state.apply(
        parse_user_data_event(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 1783123203000,
                "T": 1783123201000,
                "a": {"B": [{"a": "USDT", "wb": "90"}], "P": []},
            },
            received_at=original.config.observed_at + timedelta(seconds=4),
        )
    )
    assert update.snapshot.balances[0].wallet_balance == Decimal("90")


def test_event_time_fallback_is_not_rest_coverage_evidence() -> None:
    original = _initial_snapshot()
    state = AccountUserDataState(
        replace(
            original,
            balances=(
                replace(
                    original.balances[0],
                    raw_payload={"updateTime": 1783123202000},
                ),
            ),
        )
    )
    update = state.apply(
        parse_user_data_event(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 1783123201000,
                "a": {"B": [{"a": "USDT", "wb": "90"}], "P": []},
            },
            received_at=original.config.observed_at + timedelta(seconds=4),
        )
    )
    assert update.snapshot.balances[0].wallet_balance == Decimal("90")


@pytest.mark.parametrize(
    "field,value,drift",
    [
        ("wallet_balance", Decimal("99"), True),
        ("available_balance", Decimal("70"), False),
        ("unrealized_pnl", Decimal("3"), False),
    ],
)
def test_periodic_check_compares_wallet_not_rest_only_balance_fields(
    field, value, drift
):
    from crypto_momentum_lab.execution_account.snapshot_changes import (
        account_ws_state_matches,
    )

    live = _initial_snapshot()
    rest = replace(live, balances=(replace(live.balances[0], **{field: value}),))
    assert account_ws_state_matches(live, rest) == (not drift)


@pytest.mark.parametrize(
    "field,value,drift",
    [
        ("position_amt", Decimal("0.003"), True),
        ("entry_price", Decimal("49000"), True),
        ("margin_type", "isolated", True),
        ("leverage", 10, True),
        ("mark_price", Decimal("52000"), False),
        ("unrealized_pnl", Decimal("4"), False),
        ("notional", Decimal("104"), False),
    ],
)
def test_periodic_check_ignores_market_valuation_changes(field, value, drift):
    from crypto_momentum_lab.execution_account.snapshot_changes import (
        account_ws_state_matches,
    )

    live = _initial_snapshot()
    rest = replace(live, positions=(replace(live.positions[0], **{field: value}),))
    assert account_ws_state_matches(live, rest) == (not drift)


def test_periodic_check_normalizes_zero_rows_but_detects_orders_and_config():
    from crypto_momentum_lab.execution_account.snapshot_changes import (
        account_ws_state_matches,
    )

    live = _initial_snapshot()
    rest = replace(
        live,
        balances=(
            *live.balances,
            replace(live.balances[0], asset="BNB", wallet_balance=Decimal("0")),
        ),
        positions=(
            *live.positions,
            replace(live.positions[0], symbol="ETHUSDT", position_amt=Decimal("0")),
        ),
    )
    assert account_ws_state_matches(live, rest)
    assert not account_ws_state_matches(live, replace(rest, open_orders=()))
    assert not account_ws_state_matches(
        live,
        replace(
            rest,
            open_orders=(
                replace(
                    rest.open_orders[0],
                    executed_quantity=Decimal("0.001"),
                ),
            ),
        ),
    )
    assert not account_ws_state_matches(
        live, replace(rest, config=replace(rest.config, hedge_mode=True))
    )
