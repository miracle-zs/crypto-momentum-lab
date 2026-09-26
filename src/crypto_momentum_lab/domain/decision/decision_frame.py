"""Authoritative DecisionFrame pinning multi-window revisions and account context.

Obeys Astra Architecture Blueprint 2026-09-25:
- DecisionFrame binds real market window refs, position view token, universe ref,
  policy code/parameter/state digests, clock event, and cash balance;
- Cross-domain vector clock verification enforces clock skew budgets;
- Deterministic frame digest enables tamper-evident decision replay.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.market.revision_models import MarketRevisionRef


def _require_aware(dt: datetime, name: str) -> datetime:
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware (UTC)")
    return dt.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class ClockEvent:
    """Explicit external clock tick driving deterministic strategy evaluation."""

    timestamp: datetime
    sequence: int
    event_type: str = "bucket_close"

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "timestamp", _require_aware(self.timestamp, "timestamp")
        )
        if self.sequence < 0:
            raise ValueError("sequence must be non-negative")


@dataclass(frozen=True, slots=True)
class DecisionFrame:
    """Execution input frame pinning all inputs for reproducible decisioning (R2/R3)."""

    scope: str
    symbol: str
    market_refs: tuple[MarketRevisionRef, ...]
    position_view_token: str
    clock_event: ClockEvent
    universe_version: str = "default"
    risk_config_version: str = "v1"
    policy_code_digest: str = ""
    policy_parameters_digest: str = ""
    policy_state_digest: str = ""
    risk_plan_digest: str = ""
    cash_balance: Decimal = Decimal("0")
    max_clock_skew: timedelta = timedelta(seconds=60)
    frame_digest: str = ""

    def __post_init__(self) -> None:
        if not self.scope.strip():
            raise ValueError("scope must not be empty")
        if not self.symbol.strip():
            raise ValueError("symbol must not be empty")
        if not self.market_refs:
            raise ValueError("market_refs must not be empty")
        if not self.position_view_token.strip():
            raise ValueError("position_view_token must not be empty")
        if not self.universe_version.strip():
            raise ValueError("universe_version must not be empty")
        if not self.risk_config_version.strip():
            raise ValueError("risk_config_version must not be empty")
        if self.cash_balance < Decimal("0"):
            raise ValueError("cash_balance must not be negative")

        # Validate vector clock skew between market time and clock event
        latest_market_time = max(r.bucket_end for r in self.market_refs)
        skew = abs(self.clock_event.timestamp - latest_market_time)
        if skew > self.max_clock_skew:
            raise ValueError(
                f"Clock skew between clock_event "
                f"({self.clock_event.timestamp.isoformat()}) and latest "
                f"market_time ({latest_market_time.isoformat()}) is {skew}, "
                f"which exceeds max allowed {self.max_clock_skew}"
            )

        if not self.frame_digest:
            object.__setattr__(self, "frame_digest", self.compute_frame_digest())

    def compute_frame_digest(self) -> str:
        payload = {
            "scope": self.scope,
            "symbol": self.symbol,
            "market_refs": [
                {
                    "revision_id": r.revision_id,
                    "content_hash": r.content_hash,
                    "published_at": r.published_at.isoformat(),
                }
                for r in self.market_refs
            ],
            "position_view_token": self.position_view_token,
            "universe_version": self.universe_version,
            "risk_config_version": self.risk_config_version,
            "policy_code_digest": self.policy_code_digest,
            "policy_parameters_digest": self.policy_parameters_digest,
            "policy_state_digest": self.policy_state_digest,
            "risk_plan_digest": self.risk_plan_digest,
            "clock_timestamp": self.clock_event.timestamp.isoformat(),
            "clock_sequence": self.clock_event.sequence,
            "cash_balance": str(self.cash_balance),
        }
        dumped = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(dumped.encode("utf-8")).hexdigest()
