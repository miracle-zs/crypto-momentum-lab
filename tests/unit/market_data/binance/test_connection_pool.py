import pytest

from crypto_momentum_lab.domain.market.models import CaptureStream
from crypto_momentum_lab.market_data.binance.connection_pool import (
    BinanceConnectionPool,
)
from crypto_momentum_lab.market_data.binance.websocket import (
    should_replace_connection,
)


class FakeConnection:
    def __init__(self) -> None:
        self.methods: list[str] = []

    async def start(self) -> None:
        return None

    async def subscribe(
        self,
        names: tuple[str, ...],
        *,
        generation: int,
    ) -> None:
        self.methods.append("SUBSCRIBE")

    async def unsubscribe(
        self,
        names: tuple[str, ...],
        *,
        generation: int,
    ) -> None:
        self.methods.append("UNSUBSCRIBE")

    async def stop(self) -> None:
        return None


@pytest.fixture
def fake_connection() -> FakeConnection:
    return FakeConnection()


async def test_pool_applies_additions_before_removals(
    fake_connection: FakeConnection,
) -> None:
    pool = BinanceConnectionPool(
        connection_factory=lambda group: fake_connection,
        max_subscriptions_per_connection=100,
        control_messages_per_second=5,
    )
    await pool.apply_symbols(
        frozenset({"BTCUSDT"}),
        streams=(CaptureStream.AGG_TRADE,),
        generation=1,
    )
    await pool.apply_symbols(
        frozenset({"ETHUSDT"}),
        streams=(CaptureStream.AGG_TRADE,),
        generation=2,
    )

    assert fake_connection.methods == [
        "SUBSCRIBE",
        "SUBSCRIBE",
        "UNSUBSCRIBE",
    ]


def test_connection_is_replaced_before_lifetime() -> None:
    assert should_replace_connection(
        opened_at=100.0,
        now=82900.0,
        lifetime_seconds=82800.0,
    )
