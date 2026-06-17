from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
)
from crypto_momentum_lab.market_data.binance.connection_pool import (
    BinanceConnectionPool,
)
from crypto_momentum_lab.market_data.binance.websocket import (
    should_replace_connection,
)
from crypto_momentum_lab.market_data.capture.subscriptions import (
    SubscriptionGroup,
)


class FakeConnection:
    def __init__(
        self,
        group: SubscriptionGroup,
        events: list[tuple[str, tuple[str, ...]]],
    ) -> None:
        self.group = group
        self.events = events
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def subscribe(
        self,
        names: tuple[str, ...],
        *,
        generation: int,
    ) -> None:
        self.events.append(("SUBSCRIBE", names))

    async def unsubscribe(
        self,
        names: tuple[str, ...],
        *,
        generation: int,
    ) -> None:
        self.events.append(("UNSUBSCRIBE", names))

    async def stop(self) -> None:
        self.stopped = True


def build_pool() -> tuple[
    BinanceConnectionPool,
    list[FakeConnection],
    list[tuple[str, tuple[str, ...]]],
]:
    connections: list[FakeConnection] = []
    events: list[tuple[str, tuple[str, ...]]] = []

    def factory(group: SubscriptionGroup) -> FakeConnection:
        connection = FakeConnection(group, events)
        connections.append(connection)
        return connection

    return (
        BinanceConnectionPool(
            connection_factory=factory,
            max_subscriptions_per_connection=100,
            control_messages_per_second=5,
        ),
        connections,
        events,
    )


async def test_pool_starts_one_connection_per_route_stream_group() -> None:
    pool, connections, _ = build_pool()

    await pool.apply_symbols(
        frozenset({"BTCUSDT"}),
        streams=(
            CaptureStream.AGG_TRADE,
            CaptureStream.BOOK_TICKER,
            CaptureStream.KLINE_1M,
        ),
        generation=1,
    )

    assert {
        (connection.group.route, connection.group.stream)
        for connection in connections
    } == {
        (CaptureRoute.MARKET, CaptureStream.AGG_TRADE),
        (CaptureRoute.MARKET, CaptureStream.KLINE_1M),
        (CaptureRoute.PUBLIC, CaptureStream.BOOK_TICKER),
    }
    assert all(connection.started for connection in connections)


async def test_pool_reuses_group_and_adds_before_removing() -> None:
    pool, connections, events = build_pool()

    await pool.apply_symbols(
        frozenset({"BTCUSDT"}),
        streams=(CaptureStream.AGG_TRADE,),
        generation=1,
    )
    events.clear()

    await pool.apply_symbols(
        frozenset({"ETHUSDT"}),
        streams=(CaptureStream.AGG_TRADE,),
        generation=2,
    )

    assert len(connections) == 1
    assert events == [
        ("SUBSCRIBE", ("ethusdt@aggTrade",)),
        ("UNSUBSCRIBE", ("btcusdt@aggTrade",)),
    ]


def test_connection_is_replaced_before_lifetime() -> None:
    assert should_replace_connection(
        opened_at=100.0,
        now=82900.0,
        lifetime_seconds=82800.0,
    )
