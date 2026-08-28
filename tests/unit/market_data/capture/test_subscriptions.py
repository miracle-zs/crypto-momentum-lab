from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
)
from crypto_momentum_lab.market_data.capture.subscriptions import (
    GLOBAL_BOOK_TICKER_STREAM_NAME,
    Subscription,
    build_subscription_groups,
    plan_subscription_change,
)


def test_stream_routes_and_names_follow_binance_contract() -> None:
    agg = Subscription.for_symbol(CaptureStream.AGG_TRADE, "BTCUSDT")
    book = Subscription.for_symbol(CaptureStream.BOOK_TICKER, "BTCUSDT")

    assert agg.route is CaptureRoute.MARKET
    assert agg.binance_name == "btcusdt@aggTrade"
    assert book.route is CaptureRoute.PUBLIC
    assert book.binance_name == "btcusdt@bookTicker"


def test_global_book_ticker_subscription_uses_binance_all_stream() -> None:
    subscription = Subscription.global_book_ticker()

    assert subscription.route is CaptureRoute.PUBLIC
    assert subscription.stream is CaptureStream.BOOK_TICKER
    assert subscription.binance_name == GLOBAL_BOOK_TICKER_STREAM_NAME


def test_groups_are_stable_and_capped() -> None:
    subscriptions = frozenset(
        Subscription.for_symbol(CaptureStream.AGG_TRADE, f"S{i:03d}USDT")
        for i in range(205)
    )

    groups = build_subscription_groups(
        subscriptions,
        max_per_connection=100,
    )

    assert [len(group.subscriptions) for group in groups] == [100, 100, 5]
    assert groups == tuple(sorted(groups, key=lambda item: item.group_id))


def test_groups_support_stream_specific_limits() -> None:
    subscriptions = frozenset(
        Subscription.for_symbol(CaptureStream.BOOK_TICKER, f"S{i:03d}USDT")
        for i in range(125)
    )

    groups = build_subscription_groups(
        subscriptions,
        max_per_connection=100,
        max_per_connection_by_stream={CaptureStream.BOOK_TICKER: 50},
    )

    assert [len(group.subscriptions) for group in groups] == [50, 50, 25]


def test_group_id_is_stable_for_same_route_stream_and_chunk() -> None:
    btc_group = build_subscription_groups(
        frozenset(
            {
                Subscription.for_symbol(
                    CaptureStream.AGG_TRADE,
                    "BTCUSDT",
                )
            }
        ),
        max_per_connection=100,
    )[0]
    eth_group = build_subscription_groups(
        frozenset(
            {
                Subscription.for_symbol(
                    CaptureStream.AGG_TRADE,
                    "ETHUSDT",
                )
            }
        ),
        max_per_connection=100,
    )[0]

    assert btc_group.group_id == eth_group.group_id


def test_change_plan_adds_before_removing() -> None:
    old = frozenset(
        {
            Subscription.for_symbol(
                CaptureStream.AGG_TRADE,
                "BTCUSDT",
            ),
        }
    )
    new = frozenset(
        {
            Subscription.for_symbol(
                CaptureStream.AGG_TRADE,
                "ETHUSDT",
            ),
        }
    )

    plan = plan_subscription_change(old, new, generation=2)

    assert [step.method for step in plan.steps] == [
        "SUBSCRIBE",
        "UNSUBSCRIBE",
    ]
