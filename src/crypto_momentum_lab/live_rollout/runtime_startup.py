"""Small, explicit startup boundary for the live runtime.

The orchestration module owns object assembly. This module owns only the
startup invariants and observability markers that precede that assembly.
"""

from time import perf_counter

import structlog

from crypto_momentum_lab.health import LocalHealthWriter
from crypto_momentum_lab.live_rollout.readiness import LiveReadinessPublisher
from crypto_momentum_lab.live_rollout.runtime_config import LiveRuntimeConfig

log = structlog.get_logger()


def validate_live_runtime_endpoints(config: LiveRuntimeConfig) -> None:
    """Reject incomplete transport configuration before acquiring resources."""
    market = config.market
    if market.market_state_source not in {"hub", "postgres"}:
        raise ValueError("market_state_source must be 'hub' or 'postgres'")
    if market.market_state_source == "hub" and not market.market_state_hub_url.strip():
        raise ValueError("market_state_hub_url must not be empty in hub mode")
    if market.market_state_source == "hub" and not market.market_quote_hub_url.strip():
        raise ValueError("market_quote_hub_url must not be empty in hub mode")
    if (
        market.market_state_source == "hub"
        and not market.market_quote_volume_hub_url.strip()
    ):
        raise ValueError(
            "market_quote_volume_hub_url must not be empty in hub mode"
        )
    if (
        market.account_event_hub_url is not None
        and not market.account_event_hub_url.strip()
    ):
        raise ValueError("account_event_hub_url must not be empty when configured")


class LiveRuntimeStartupStatus:
    """Own startup phase timing and best-effort local health publication."""

    def __init__(
        self,
        health: LocalHealthWriter | None,
        *,
        logger: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        self.health = health
        self.readiness: LiveReadinessPublisher | None = None
        self._logger = log if logger is None else logger
        self._started_at = perf_counter()
        self._last_phase_at = self._started_at

    @classmethod
    def from_environment(cls) -> "LiveRuntimeStartupStatus":
        return cls(LocalHealthWriter.from_environment())

    def log_phase(self, phase: str) -> None:
        now = perf_counter()
        self._logger.info(
            "live_startup_phase",
            phase=phase,
            phase_elapsed_ms=round((now - self._last_phase_at) * 1000, 3),
            total_elapsed_ms=round((now - self._started_at) * 1000, 3),
        )
        self._last_phase_at = now

    def mark_database_ok(self) -> None:
        if self.health is None:
            return
        try:
            self.health.database_ok()
        except Exception:
            self._logger.exception("live_health_database_marker_failed")

    def mark_account_facts_timeout(self, reason: str) -> None:
        """Report that account facts did not arrive inside the startup budget.

        The process status becomes ``degraded`` with a concrete reason instead
        of advertising readiness; local health publication is best-effort and
        must never change trading behaviour.
        """

        if not reason.strip():
            raise ValueError("reason must not be empty")
        self._logger.error(
            "live_startup_account_facts_timeout",
            reason=reason,
            total_elapsed_ms=round((perf_counter() - self._started_at) * 1000, 3),
        )
        if self.health is None:
            return
        try:
            self.health.degraded()
        except Exception:
            self._logger.exception("live_health_degrade_marker_failed")

    def mark_ready(self) -> None:
        if self.health is None:
            return
        try:
            self.health.heartbeat(database_ok=True)
            if self.readiness is not None:
                self.readiness.publish()
        except Exception:
            self._logger.exception("live_health_marker_failed")
