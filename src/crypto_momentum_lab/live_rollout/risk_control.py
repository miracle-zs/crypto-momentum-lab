"""Durable execution of one-shot live risk-control commands.

The websocket event is only a low-latency trigger.  A worker must first claim
the matching command from PostgreSQL, then execute the already-existing live
controller seam, and finally record the outcome.  This keeps duplicate events
idempotent and prevents an unaudited publisher from creating exchange work.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.live_rollout import RollbackCommand
from crypto_momentum_lab.execution_account.risk_control_hub import (
    RiskControlAction,
    RiskControlEvent,
)
from crypto_momentum_lab.live_rollout.commands import (
    CANCEL_ALL_OPEN_ENTRIES_COMMAND,
    CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION,
    EMERGENCY_FLATTEN_COMMAND,
    EMERGENCY_FLATTEN_CONFIRMATION,
    require_authorized_command,
)

log = structlog.get_logger()


class RiskControlCommandRepository(Protocol):
    async def load_command(self, command_id: str) -> RollbackCommand | None: ...

    async def claim_command(
        self,
        command_id: str,
        *,
        account_label: str,
        strategy_name: str,
        session_id: str,
    ) -> RollbackCommand | None: ...

    async def complete_command(
        self,
        command_id: str,
        *,
        status: str,
        completed_at: datetime,
        failure_reason: str | None,
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class _RiskControlCommandSpec:
    command_type: str
    confirmation_text: str
    execute: Callable[[], Awaitable[str | None]]


class RiskControlCommandDispatcher:
    """Claim, authorize, execute, and complete a streamed control command."""

    def __init__(
        self,
        *,
        repository: RiskControlCommandRepository,
        account_label: str,
        strategy_name: str,
        session_id: str,
        cancel_all_open_entries: Callable[[], Awaitable[str | None]],
        request_flatten: Callable[[], Awaitable[str | None]],
        clock: Callable[[], datetime],
    ) -> None:
        self._repository = repository
        self._account_label = account_label
        self._strategy_name = strategy_name
        self._session_id = session_id
        self._clock = clock
        self._specs = {
            RiskControlAction.CANCEL_ALL_OPEN_ENTRIES: _RiskControlCommandSpec(
                command_type=CANCEL_ALL_OPEN_ENTRIES_COMMAND,
                confirmation_text=CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION,
                execute=cancel_all_open_entries,
            ),
            RiskControlAction.REQUEST_FLATTEN: _RiskControlCommandSpec(
                command_type=EMERGENCY_FLATTEN_COMMAND,
                confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
                execute=request_flatten,
            ),
        }

    async def dispatch(self, event: RiskControlEvent) -> str | None:
        """Execute a supported event, returning a durable failure reason."""

        spec = self._specs.get(event.action)
        if spec is None:
            return None
        if event.details.get("command_type") != spec.command_type:
            return "risk_control_command_type_mismatch"

        command = await self._repository.claim_command(
            event.command_id,
            account_label=self._account_label,
            strategy_name=self._strategy_name,
            session_id=self._session_id,
        )
        if command is None:
            # A duplicate event, an unknown command, or a command already
            # claimed by another worker must never trigger exchange work.
            existing = await self._repository.load_command(event.command_id)
            if existing is None:
                return "risk_control_command_not_found"
            if existing.status == "completed":
                return None
            if existing.status == "failed":
                return existing.failure_reason or "risk_control_command_failed"
            return "risk_control_command_already_claimed"

        failure: str | None = None
        try:
            require_authorized_command(
                command,
                command_type=spec.command_type,
                confirmation_text=spec.confirmation_text,
            )
            failure = await spec.execute()
        except Exception as error:
            failure = f"risk_control_action_failed:{type(error).__name__}"
            log.exception(
                "live_risk_control_command_failed",
                action=event.action.value,
                command_id=event.command_id,
                error_type=type(error).__name__,
            )

        status = "failed" if failure is not None else "completed"
        try:
            completed = await self._repository.complete_command(
                command.command_id,
                status=status,
                completed_at=self._clock(),
                failure_reason=failure,
            )
        except Exception as error:
            log.exception(
                "live_risk_control_command_completion_failed",
                action=event.action.value,
                command_id=event.command_id,
                error_type=type(error).__name__,
            )
            return failure or "risk_control_command_completion_failed"
        if not completed:
            return failure or "risk_control_command_completion_not_recorded"
        return failure


__all__ = [
    "RiskControlCommandDispatcher",
    "RiskControlCommandRepository",
]
