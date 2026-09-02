"""In-memory ownership hints for positions created by the live strategy.

The Binance account stream can deliver ``ACCOUNT_UPDATE`` before the matching
``ORDER_TRADE_UPDATE``.  A position key is therefore not evidence of an
accounting failure by itself.  The live order process registers an expected
entry before it calls Binance; the execution-account process consumes that
single-use hint when the first non-zero position arrives.

The registry is intentionally in memory.  It is a fast-path correlation aid,
not an authoritative position store.  A missing, expired, or mismatched hint
still follows the existing REST recovery path.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.execution import OrderExecutionPlan

_DEFAULT_MARKET_EXPECTATION_TTL_SECONDS = 30.0
_MAX_EXPECTATIONS = 4096


@dataclass(frozen=True, slots=True)
class AccountPositionExpectation:
    environment: str
    account_label: str
    symbol: str
    position_side: str
    client_order_id: str
    side: str
    quantity: Decimal
    created_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        for value, field_name in (
            (self.environment, "environment"),
            (self.account_label, "account_label"),
            (self.symbol, "symbol"),
            (self.position_side, "position_side"),
            (self.client_order_id, "client_order_id"),
            (self.side, "side"),
        ):
            if not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        for timestamp_value, field_name in (
            (self.created_at, "created_at"),
            (self.expires_at, "expires_at"),
        ):
            if (
                timestamp_value.tzinfo is None
                or timestamp_value.utcoffset() is None
            ):
                raise ValueError(f"{field_name} must be timezone-aware")
        if self.expires_at <= self.created_at:
            raise ValueError("expires_at must be after created_at")

        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(
            self,
            "position_side",
            self.position_side.strip().upper(),
        )
        object.__setattr__(self, "side", self.side.strip().upper())
        if self.side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")

    @classmethod
    def from_plan(
        cls,
        plan: OrderExecutionPlan,
        *,
        environment: str,
        account_label: str,
        registered_at: datetime,
    ) -> "AccountPositionExpectation":
        if plan.reduce_only:
            raise ValueError("reduce-only orders cannot create position expectations")
        if registered_at.tzinfo is None or registered_at.utcoffset() is None:
            raise ValueError("registered_at must be timezone-aware")
        expires_at = plan.expires_at or (
            registered_at
            + timedelta(seconds=_DEFAULT_MARKET_EXPECTATION_TTL_SECONDS)
        )
        position_side = getattr(plan.position_side, "value", plan.position_side)
        return cls(
            environment=environment,
            account_label=account_label,
            symbol=plan.symbol,
            position_side=str(position_side),
            client_order_id=plan.client_order_id,
            side=plan.side,
            quantity=plan.quantity,
            created_at=registered_at,
            expires_at=expires_at,
        )


class AccountPositionExpectationRegistry:
    """Bounded, single-use registry for strategy-owned entry expectations."""

    def __init__(
        self,
        *,
        environment: str,
        account_label: str,
        clock: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
    ) -> None:
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        self._environment = environment
        self._account_label = account_label
        self._clock = clock
        self._expectations: dict[str, AccountPositionExpectation] = {}

    @property
    def pending_count(self) -> int:
        self._expire(self._now())
        return len(self._expectations)

    def register(self, expectation: AccountPositionExpectation) -> None:
        self._require_scope(expectation)
        now = self._now()
        self._expire(now)
        if expectation.expires_at <= now:
            raise ValueError("account position expectation is already expired")
        if (
            expectation.client_order_id not in self._expectations
            and len(self._expectations) >= _MAX_EXPECTATIONS
        ):
            raise RuntimeError("account position expectation registry is full")
        self._expectations[expectation.client_order_id] = expectation

    def discard(self, client_order_id: str) -> None:
        self._expectations.pop(client_order_id.strip(), None)

    def consume(
        self,
        *,
        symbol: str,
        position_side: str,
        position_amt: Decimal,
        observed_at: datetime,
    ) -> AccountPositionExpectation | None:
        """Consume the oldest matching hint for a newly non-zero position."""
        if position_amt == 0:
            return None
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        now = self._now()
        comparison_time = max(now, observed_at)
        self._expire(now)
        normalized_symbol = symbol.strip().upper()
        normalized_position_side = position_side.strip().upper()
        candidates = sorted(
            (
                expectation
                for expectation in self._expectations.values()
                if expectation.symbol == normalized_symbol
                and expectation.position_side == normalized_position_side
                and _position_amount_matches_side(
                    position_amt,
                    expectation.side,
                )
                and expectation.created_at <= observed_at
                and expectation.expires_at > comparison_time
            ),
            key=lambda item: (item.created_at, item.client_order_id),
        )
        if not candidates:
            return None
        expectation = candidates[0]
        self._expectations.pop(expectation.client_order_id, None)
        return expectation

    def _require_scope(self, expectation: AccountPositionExpectation) -> None:
        if (
            expectation.environment != self._environment
            or expectation.account_label != self._account_label
        ):
            raise ValueError("account position expectation scope does not match")

    def _expire(self, now: datetime) -> None:
        expired = tuple(
            client_order_id
            for client_order_id, expectation in self._expectations.items()
            if expectation.expires_at <= now
        )
        for client_order_id in expired:
            self._expectations.pop(client_order_id, None)

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return timezone-aware datetime")
        return value


def _position_amount_matches_side(position_amt: Decimal, side: str) -> bool:
    if side == "BUY":
        return position_amt > 0
    if side == "SELL":
        return position_amt < 0
    return False
