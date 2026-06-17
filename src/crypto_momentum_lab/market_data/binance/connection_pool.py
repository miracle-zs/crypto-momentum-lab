from collections.abc import Callable
from typing import Protocol

from crypto_momentum_lab.domain.market.models import CaptureStream
from crypto_momentum_lab.market_data.capture.subscriptions import (
    Subscription,
    SubscriptionGroup,
    build_subscription_groups,
    plan_subscription_change,
)


class PoolConnection(Protocol):
    async def start(self) -> None: ...

    async def subscribe(
        self,
        names: tuple[str, ...],
        *,
        generation: int,
    ) -> None: ...

    async def unsubscribe(
        self,
        names: tuple[str, ...],
        *,
        generation: int,
    ) -> None: ...

    async def stop(self) -> None: ...


class BinanceConnectionPool:
    def __init__(
        self,
        *,
        connection_factory: Callable[[SubscriptionGroup], PoolConnection],
        max_subscriptions_per_connection: int,
        control_messages_per_second: float,
    ) -> None:
        self._connection_factory = connection_factory
        self._max_subscriptions_per_connection = max_subscriptions_per_connection
        self._control_messages_per_second = control_messages_per_second
        self._active_subscriptions: frozenset[Subscription] = frozenset()
        self._connections: dict[str, PoolConnection] = {}

    async def start(self) -> None:
        return None

    async def apply_symbols(
        self,
        symbols: frozenset[str],
        *,
        streams: tuple[CaptureStream, ...],
        generation: int,
    ) -> None:
        desired = frozenset(
            Subscription.for_symbol(stream, symbol)
            for symbol in symbols
            for stream in streams
        )
        groups = build_subscription_groups(
            desired,
            max_per_connection=self._max_subscriptions_per_connection,
        )
        connection = await self._ensure_connection(groups)
        plan = plan_subscription_change(
            self._active_subscriptions,
            desired,
            generation=generation,
        )
        if connection is not None:
            for step in plan.steps:
                if step.method == "SUBSCRIBE":
                    await connection.subscribe(
                        step.names,
                        generation=step.generation,
                    )
                elif step.method == "UNSUBSCRIBE":
                    await connection.unsubscribe(
                        step.names,
                        generation=step.generation,
                    )
        self._active_subscriptions = desired

    async def stop(self) -> None:
        for connection in tuple(self._connections.values()):
            await connection.stop()
        self._connections.clear()
        self._active_subscriptions = frozenset()

    async def _ensure_connection(
        self,
        groups: tuple[SubscriptionGroup, ...],
    ) -> PoolConnection | None:
        if not groups:
            return next(iter(self._connections.values()), None)
        group = groups[0]
        connection = self._connections.get(group.group_id)
        if connection is None:
            connection = self._connection_factory(group)
            self._connections[group.group_id] = connection
            await connection.start()
        return connection
