from collections.abc import Mapping
from dataclasses import dataclass

from crypto_momentum_lab.domain.market.models import (
    CaptureRoute,
    CaptureStream,
)

_ROUTES = {
    CaptureStream.AGG_TRADE: CaptureRoute.MARKET,
    CaptureStream.BOOK_TICKER: CaptureRoute.PUBLIC,
    CaptureStream.FORCE_ORDER: CaptureRoute.MARKET,
    CaptureStream.MARK_PRICE: CaptureRoute.MARKET,
    CaptureStream.KLINE_1M: CaptureRoute.MARKET,
}

GLOBAL_BOOK_TICKER_STREAM_NAME = "!bookTicker"


@dataclass(frozen=True, slots=True, order=True)
class Subscription:
    route: CaptureRoute
    stream: CaptureStream
    symbol: str
    binance_name: str

    @classmethod
    def for_symbol(
        cls,
        stream: CaptureStream,
        symbol: str,
    ) -> "Subscription":
        normalized = symbol.upper()
        return cls(
            route=_ROUTES[stream],
            stream=stream,
            symbol=normalized,
            binance_name=f"{normalized.lower()}@{stream.value}",
        )

    @classmethod
    def global_book_ticker(cls) -> "Subscription":
        return cls(
            route=CaptureRoute.PUBLIC,
            stream=CaptureStream.BOOK_TICKER,
            symbol="*",
            binance_name=GLOBAL_BOOK_TICKER_STREAM_NAME,
        )


@dataclass(frozen=True, slots=True)
class SubscriptionGroup:
    group_id: str
    route: CaptureRoute
    stream: CaptureStream
    subscriptions: tuple[Subscription, ...]


@dataclass(frozen=True, slots=True)
class SubscriptionCommand:
    method: str
    names: tuple[str, ...]
    generation: int


@dataclass(frozen=True, slots=True)
class SubscriptionChangePlan:
    generation: int
    desired: frozenset[Subscription]
    steps: tuple[SubscriptionCommand, ...]


def build_subscription_groups(
    subscriptions: frozenset[Subscription],
    *,
    max_per_connection: int,
    max_per_connection_by_stream: Mapping[CaptureStream, int] | None = None,
) -> tuple[SubscriptionGroup, ...]:
    if max_per_connection <= 0:
        raise ValueError("max_per_connection must be positive")
    stream_limits = dict(max_per_connection_by_stream or {})
    if any(limit <= 0 for limit in stream_limits.values()):
        raise ValueError("stream subscription limits must be positive")

    buckets: dict[
        tuple[CaptureRoute, CaptureStream],
        list[Subscription],
    ] = {}
    for item in sorted(subscriptions):
        buckets.setdefault((item.route, item.stream), []).append(item)

    groups: list[SubscriptionGroup] = []
    for (route, stream), items in sorted(
        buckets.items(),
        key=lambda item: (item[0][0].value, item[0][1].value),
    ):
        stream_limit = stream_limits.get(stream, max_per_connection)
        for chunk_index, offset in enumerate(
            range(0, len(items), stream_limit)
        ):
            chunk = tuple(items[offset : offset + stream_limit])
            groups.append(
                SubscriptionGroup(
                    group_id=(
                        f"{route.value}:{stream.value}:{chunk_index:04d}"
                    ),
                    route=route,
                    stream=stream,
                    subscriptions=chunk,
                )
            )
    return tuple(sorted(groups, key=lambda item: item.group_id))


def plan_subscription_change(
    active: frozenset[Subscription],
    desired: frozenset[Subscription],
    *,
    generation: int,
) -> SubscriptionChangePlan:
    additions = tuple(sorted(desired - active))
    removals = tuple(sorted(active - desired))
    steps: list[SubscriptionCommand] = []
    if additions:
        steps.append(
            SubscriptionCommand(
                method="SUBSCRIBE",
                names=tuple(item.binance_name for item in additions),
                generation=generation,
            )
        )
    if removals:
        steps.append(
            SubscriptionCommand(
                method="UNSUBSCRIBE",
                names=tuple(item.binance_name for item in removals),
                generation=generation,
            )
        )
    return SubscriptionChangePlan(generation, desired, tuple(steps))
