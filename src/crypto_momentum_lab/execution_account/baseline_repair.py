"""The evidence required to reuse a REST baseline fetched alongside WS events.

An event counter proves absence of concurrent events, not an exchange sequence.
Without an exchange-wide REST cut, a changed counter cannot be safely rebased.
"""

from dataclasses import dataclass
from datetime import datetime


def baseline_is_fresh(observed_at: datetime, *, now: datetime) -> bool:
    return 0 <= (now - observed_at).total_seconds() <= 180.0


@dataclass(frozen=True, slots=True)
class BaselineRepairAttempt:
    event_generation: int
    stream_token: int
    baseline_observed_at: datetime

    def can_serve_live(self, *, stream_token: int | None, now: datetime) -> bool:
        """WS may update the old baseline while this scan is in flight."""
        return stream_token == self.stream_token and baseline_is_fresh(
            self.baseline_observed_at, now=now
        )

    def can_commit(
        self,
        *,
        event_generation: int,
        stream_token: int | None,
        recovery_required: bool,
        candidate_observed_at: datetime | None,
        now: datetime,
    ) -> bool:
        """Validate under the projection lock after draining queued work.

        A result without a snapshot may carry a halt/failure decision: preserve
        it if continuity is intact rather than treating it as a fresh baseline.
        """
        return (
            not recovery_required
            and stream_token == self.stream_token
            and event_generation == self.event_generation
            and (
                candidate_observed_at is None
                or baseline_is_fresh(candidate_observed_at, now=now)
            )
        )
