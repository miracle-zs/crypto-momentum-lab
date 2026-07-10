from crypto_momentum_lab.execution_account.orders.ids import (
    BINANCE_CLIENT_ORDER_ID_MAX_LENGTH,
    deterministic_client_order_id,
)


def test_client_order_id_is_deterministic() -> None:
    first = deterministic_client_order_id("run-1", "intent-1")
    second = deterministic_client_order_id("run-1", "intent-1")

    assert first == second


def test_client_order_id_changes_with_intent_id() -> None:
    first = deterministic_client_order_id("run-1", "intent-1")
    second = deterministic_client_order_id("run-1", "intent-2")

    assert first != second


def test_client_order_id_is_short_enough_for_binance() -> None:
    client_order_id = deterministic_client_order_id("run-1", "intent-1")

    assert len(client_order_id) <= BINANCE_CLIENT_ORDER_ID_MAX_LENGTH
    assert len(client_order_id) == 36
