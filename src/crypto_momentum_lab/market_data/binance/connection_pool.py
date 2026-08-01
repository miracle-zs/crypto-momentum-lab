import asyncio
from collections.abc import Callable
from typing import Protocol

from crypto_momentum_lab.domain.market.models import CaptureStream
from crypto_momentum_lab.market_data.capture.subscriptions import (
    Subscription,
    SubscriptionChangePlan,
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
        self._subscription_connections: dict[Subscription, str] = {}

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
        desired_groups = {group.group_id: group for group in groups}
        desired_owners = {
            subscription: group.group_id
            for group in groups
            for subscription in group.subscriptions
        }
        new_group_ids = await self._ensure_connections(groups)

        for group in groups:
            if group.group_id in new_group_ids:
                continue
            connection = self._connections[group.group_id]
            current = frozenset(
                subscription
                for subscription in self._active_subscriptions
                if self._subscription_connections.get(subscription)
                == group.group_id
            )
            plan = plan_subscription_change(
                current,
                frozenset(group.subscriptions),
                generation=generation,
            )
            await self._apply_plan(connection, plan)

        for group_id in sorted(set(self._connections) - set(desired_groups)):
            connection = self._connections[group_id]
            current = frozenset(
                subscription
                for subscription in self._active_subscriptions
                if self._subscription_connections.get(subscription) == group_id
            )
            plan = plan_subscription_change(
                current,
                frozenset(),
                generation=generation,
            )
            await self._apply_plan(connection, plan)
            await connection.stop()
            del self._connections[group_id]
        self._active_subscriptions = desired
        self._subscription_connections = desired_owners

    async def stop(self) -> None:
        connections = tuple(self._connections.values())
        try:
            results = await asyncio.gather(
                *(connection.stop() for connection in connections),
                return_exceptions=True,
            )
        finally:
            self._connections.clear()
            self._active_subscriptions = frozenset()
            self._subscription_connections.clear()
        for result in results:
            if isinstance(result, BaseException):
                raise result

    async def _ensure_connections(
        self,
        groups: tuple[SubscriptionGroup, ...],
    ) -> set[str]:
        new_group_ids: set[str] = set()
        for group in groups:
            connection = self._connections.get(group.group_id)
            if connection is not None:
                continue
            connection = self._connection_factory(group)
            self._connections[group.group_id] = connection
            new_group_ids.add(group.group_id)
            await connection.start()
        return new_group_ids

    async def _apply_plan(
        self,
        connection: PoolConnection,
        plan: SubscriptionChangePlan,
    ) -> None:
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
