"""Last-moment durable fencing before a live entry reaches the exchange."""

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Protocol

from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.domain.risk import RiskHalt, TradingLease
from crypto_momentum_lab.execution_account.orders.state_machine import (
    OrderPreSubmissionError,
)


class LiveRiskStateReader(Protocol):
    async def load_active_lease(
        self,
        environment: str,
        account_label: str,
        now: datetime,
    ) -> TradingLease | None: ...

    async def load_active_halts(
        self,
        environment: str,
        account_label: str,
    ) -> tuple[RiskHalt, ...]: ...


class LiveSubmissionFence:
    """Re-read durable control state immediately before an entry POST."""

    def __init__(
        self,
        *,
        risk_state: LiveRiskStateReader,
        environment: str,
        account_label: str,
        strategy_name: str,
        lease_owner: str,
        code_generation: str,
        active_lease: Callable[[], TradingLease | None] | None = None,
        entry_enabled: Callable[[], bool] | None = None,
        is_draining: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        if not environment.strip():
            raise ValueError("environment must not be empty")
        if not account_label.strip():
            raise ValueError("account_label must not be empty")
        if not strategy_name.strip():
            raise ValueError("strategy_name must not be empty")
        if not lease_owner.strip():
            raise ValueError("lease_owner must not be empty")
        if not code_generation.strip():
            raise ValueError("code_generation must not be empty")
        self._risk_state = risk_state
        self._environment = environment
        self._account_label = account_label
        self._strategy_name = strategy_name
        self._lease_owner = lease_owner
        self._code_generation = code_generation
        self._active_lease = active_lease
        self._entry_enabled = entry_enabled
        self._is_draining = is_draining

    async def validate(
        self,
        plan: OrderExecutionPlan,
        checked_at: datetime,
    ) -> None:
        """Raise ``OrderPreSubmissionError`` if an entry fence is stale."""

        if plan.reduce_only:
            return
        if self._entry_enabled is not None and not self._entry_enabled():
            raise OrderPreSubmissionError("live entry lane is disabled")
        if self._is_draining is not None and await self._is_draining():
            raise OrderPreSubmissionError("live session entries are disabled")

        current_lease = await self._risk_state.load_active_lease(
            self._environment,
            self._account_label,
            checked_at,
        )
        if current_lease is None:
            raise OrderPreSubmissionError("active lease disappeared")
        if current_lease.owner != self._lease_owner:
            raise OrderPreSubmissionError("active lease owner changed")
        if current_lease.strategy_name != self._strategy_name:
            raise OrderPreSubmissionError("active lease strategy changed")
        if current_lease.code_generation != self._code_generation:
            raise OrderPreSubmissionError("active lease code generation changed")
        expected_lease = (
            None if self._active_lease is None else self._active_lease()
        )
        if (
            expected_lease is not None
            and current_lease.lease_id != expected_lease.lease_id
        ):
            raise OrderPreSubmissionError("active lease fencing token changed")
        if await self._risk_state.load_active_halts(
            self._environment,
            self._account_label,
        ):
            raise OrderPreSubmissionError("active risk halt")


__all__ = ["LiveRiskStateReader", "LiveSubmissionFence"]
