"""Pure entry-eligibility policy shared by paper and live adapters.

The policy deliberately accepts an immutable, already-prepared snapshot.  It
does not load account state, query the universe, fetch candles, or submit an
order.  Those concerns remain in the host application and are responsible
for translating their source data into this contract.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from crypto_momentum_lab.domain.strategy.models import StrategySide

DEFAULT_EMA_MAX_AGE = timedelta(minutes=15)


class EmaPolicyStatus(StrEnum):
    DISABLED = "disabled"
    UNAVAILABLE = "unavailable"
    VALID = "valid"


@dataclass(frozen=True, slots=True)
class EmaSnapshot:
    """Closed-candle EMA values prepared by an external cache or adapter."""

    symbol: str
    observed_at: datetime
    entry_price: Decimal
    ema5: Decimal | None = None
    ema10: Decimal | None = None
    snapshot_id: str | None = None
    config_hash: str | None = None

    def __post_init__(self) -> None:
        normalized_symbol = _normalized_symbol(self.symbol)
        object.__setattr__(self, "symbol", normalized_symbol)
        _require_aware(self.observed_at, "observed_at")
        _require_positive(self.entry_price, "entry_price")
        if self.ema5 is not None:
            _require_positive(self.ema5, "ema5")
        if self.ema10 is not None:
            _require_positive(self.ema10, "ema10")
        if self.snapshot_id is not None:
            _require_text(self.snapshot_id, "snapshot_id")
        if self.config_hash is not None:
            _require_text(self.config_hash, "config_hash")


@dataclass(frozen=True, slots=True)
class EmaPolicyState:
    """Explicit three-state EMA policy input.

    ``disabled`` skips only the EMA predicate.  ``unavailable`` is a
    fail-closed state.  ``valid`` carries the immutable values and comparison
    switches used by the policy.
    """

    status: EmaPolicyStatus
    snapshot: EmaSnapshot | None = None
    require_price_above_ema5: bool = False
    require_price_above_ema10: bool = False
    max_age: timedelta | None = None

    def __post_init__(self) -> None:
        status = EmaPolicyStatus(self.status)
        object.__setattr__(self, "status", status)
        if status is EmaPolicyStatus.VALID:
            if self.snapshot is None:
                raise ValueError("valid EMA state requires a snapshot")
            if self.max_age is None or self.max_age <= timedelta(0):
                raise ValueError("valid EMA state requires a positive max_age")
        elif self.snapshot is not None:
            raise ValueError("disabled or unavailable EMA state cannot carry data")
        elif self.max_age is not None:
            raise ValueError("disabled or unavailable EMA state cannot carry max_age")

    @classmethod
    def disabled(cls) -> "EmaPolicyState":
        return cls(status=EmaPolicyStatus.DISABLED)

    @classmethod
    def unavailable(cls) -> "EmaPolicyState":
        return cls(status=EmaPolicyStatus.UNAVAILABLE)

    @classmethod
    def valid(
        cls,
        snapshot: EmaSnapshot,
        *,
        require_price_above_ema5: bool = False,
        require_price_above_ema10: bool = False,
        max_age: timedelta = DEFAULT_EMA_MAX_AGE,
    ) -> "EmaPolicyState":
        return cls(
            status=EmaPolicyStatus.VALID,
            snapshot=snapshot,
            require_price_above_ema5=require_price_above_ema5,
            require_price_above_ema10=require_price_above_ema10,
            max_age=max_age,
        )


@dataclass(frozen=True, slots=True)
class UniverseRankingEntry:
    """Normalized direction-aware ranking entry consumed by the policy."""

    symbol: str
    rank: int
    direction: StrategySide

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(self, "direction", StrategySide(self.direction))
        if self.rank <= 0:
            raise ValueError("rank must be positive")


@dataclass(frozen=True, slots=True)
class UniverseRankingSnapshot:
    """Immutable universe view with deterministic rank tie ordering."""

    snapshot_id: str
    observed_at: datetime
    entries: tuple[UniverseRankingEntry, ...]
    config_hash: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.snapshot_id, "snapshot_id")
        _require_aware(self.observed_at, "observed_at")
        if self.config_hash is not None:
            _require_text(self.config_hash, "config_hash")
        normalized_entries = tuple(self.entries)
        if any(
            not isinstance(entry, UniverseRankingEntry)
            for entry in normalized_entries
        ):
            raise TypeError("entries must contain UniverseRankingEntry values")
        seen: set[tuple[str, StrategySide]] = set()
        for entry in normalized_entries:
            identity = (entry.symbol, entry.direction)
            if identity in seen:
                raise ValueError(
                    "universe entries must not repeat a symbol and direction"
                )
            seen.add(identity)
        object.__setattr__(
            self,
            "entries",
            tuple(
                sorted(
                    normalized_entries,
                    key=lambda entry: (
                        entry.rank,
                        entry.symbol,
                        entry.direction.value,
                    ),
                )
            ),
        )

    def contains(self, symbol: str, direction: StrategySide) -> bool:
        normalized_symbol = _normalized_symbol(symbol)
        normalized_direction = StrategySide(direction)
        return any(
            entry.symbol == normalized_symbol
            and entry.direction is normalized_direction
            for entry in self.entries
        )


@dataclass(frozen=True, slots=True)
class EntryGateResult:
    """Environment-neutral result of the live/paper entry gate."""

    approved: bool
    reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        normalized_reasons = tuple(reason.strip() for reason in self.reasons)
        if any(not reason for reason in normalized_reasons):
            raise ValueError("entry gate reasons must not be empty")
        object.__setattr__(self, "reasons", normalized_reasons)


@dataclass(frozen=True, slots=True)
class PolicyInputSnapshot:
    """All data required for one deterministic entry-eligibility decision."""

    symbol: str
    observed_at: datetime
    candidate_expiry: datetime
    entry_gate_result: EntryGateResult
    direction: StrategySide
    universe_snapshot: UniverseRankingSnapshot | None
    ema_state: EmaPolicyState
    entry_enabled: bool = True
    allow_short: bool = True
    universe_required: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(self, "direction", StrategySide(self.direction))
        _require_aware(self.observed_at, "observed_at")
        _require_aware(self.candidate_expiry, "candidate_expiry")
        if not isinstance(self.entry_enabled, bool):
            raise TypeError("entry_enabled must be a bool")
        if not isinstance(self.allow_short, bool):
            raise TypeError("allow_short must be a bool")
        if not isinstance(self.universe_required, bool):
            raise TypeError("universe_required must be a bool")


@dataclass(frozen=True, slots=True)
class EntryEligibilityDecision:
    eligible: bool
    reasons: tuple[str, ...] = ()

    @property
    def approved(self) -> bool:
        """Alias useful when adapting the decision to an execution gate."""

        return self.eligible


class EntryEligibilityPolicy:
    """Evaluate entry predicates in a stable, side-effect-free order."""

    @staticmethod
    def evaluate(snapshot: PolicyInputSnapshot) -> EntryEligibilityDecision:
        reasons: list[str] = []
        if snapshot.candidate_expiry <= snapshot.observed_at:
            reasons.append("candidate_expired")
        if not snapshot.entry_gate_result.approved:
            reasons.extend(
                snapshot.entry_gate_result.reasons or ("entry_gate_blocked",)
            )
        if not snapshot.entry_enabled:
            reasons.append("entry_disabled")
        if not snapshot.allow_short and snapshot.direction is StrategySide.SHORT:
            reasons.append("short_entries_disabled")

        universe = snapshot.universe_snapshot
        if universe is None:
            if snapshot.universe_required:
                reasons.append("universe_unavailable")
        elif not universe.contains(snapshot.symbol, snapshot.direction):
            reasons.append("outside_entry_universe")

        _append_ema_reasons(snapshot, reasons)
        return EntryEligibilityDecision(
            eligible=not reasons,
            reasons=_deduplicate(reasons),
        )


def _append_ema_reasons(
    snapshot: PolicyInputSnapshot,
    reasons: list[str],
) -> None:
    state = snapshot.ema_state
    if state.status is EmaPolicyStatus.DISABLED:
        return
    if state.status is EmaPolicyStatus.UNAVAILABLE:
        reasons.append("ema_unavailable")
        return
    ema_snapshot = state.snapshot
    if ema_snapshot is None or state.max_age is None:
        reasons.append("ema_unavailable")
        return
    if ema_snapshot.symbol != snapshot.symbol:
        reasons.append("ema_snapshot_symbol_mismatch")
        return
    age = snapshot.observed_at - ema_snapshot.observed_at
    if age < timedelta(0):
        reasons.append("ema_snapshot_from_future")
        return
    if age > state.max_age:
        reasons.append("ema_stale")
        return
    failed = (
        state.require_price_above_ema5
        and (
            ema_snapshot.ema5 is None
            or ema_snapshot.entry_price <= ema_snapshot.ema5
        )
    ) or (
        state.require_price_above_ema10
        and (
            ema_snapshot.ema10 is None
            or ema_snapshot.entry_price <= ema_snapshot.ema10
        )
    )
    if failed:
        reasons.append("ema_filter_failed")


def _deduplicate(reasons: list[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(reasons))


def _normalized_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError("symbol must not be empty")
    return normalized


def _require_text(value: str, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be empty")


def _require_aware(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


def _require_positive(value: Decimal, field_name: str) -> None:
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")


__all__ = [
    "DEFAULT_EMA_MAX_AGE",
    "EmaPolicyState",
    "EmaPolicyStatus",
    "EmaSnapshot",
    "EntryEligibilityDecision",
    "EntryEligibilityPolicy",
    "EntryGateResult",
    "PolicyInputSnapshot",
    "UniverseRankingEntry",
    "UniverseRankingSnapshot",
]
