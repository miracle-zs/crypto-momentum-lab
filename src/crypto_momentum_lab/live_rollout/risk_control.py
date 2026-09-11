"""Durable execution of one-shot live risk-control commands.

The websocket event is only a low-latency trigger.  A worker must first claim
the matching command from PostgreSQL, then execute the already-existing live
controller seam, and finally record the outcome.  This keeps duplicate events
idempotent and prevents an unaudited publisher from creating exchange work.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Protocol

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

if TYPE_CHECKING:
    from crypto_momentum_lab.live_rollout.telemetry import LiveTelemetrySink


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


class LiveRiskControlRuntime:
    """Own the live risk-control stream state and durable recovery loop."""

    _ACTION_BLOCKING = frozenset(
        {
            RiskControlAction.DISABLE_ENTRIES,
            RiskControlAction.DRAIN,
            RiskControlAction.HALT,
            RiskControlAction.CANCEL_ALL_OPEN_ENTRIES,
            RiskControlAction.REQUEST_FLATTEN,
        }
    )
    _ONE_SHOT_ACTIONS = frozenset(
        {
            RiskControlAction.CANCEL_ALL_OPEN_ENTRIES,
            RiskControlAction.REQUEST_FLATTEN,
        }
    )

    def __init__(
        self,
        *,
        enabled: bool,
        session_id: str,
        load_durable_state: Callable[[], Awaitable[tuple[bool, bool]]],
        dispatch: Callable[[RiskControlEvent], Awaitable[str | None]],
        invalidate_contexts: Callable[[], None],
        refresh_entry_gate: Callable[[], None],
        telemetry: LiveTelemetrySink | None,
        clock: Callable[[], datetime],
    ) -> None:
        if not session_id.strip():
            raise ValueError("session_id must not be empty")
        if not isinstance(enabled, bool):
            raise TypeError("enabled must be a bool")
        self._enabled = enabled
        self._session_id = session_id
        self._load_durable_state = load_durable_state
        self._dispatch = dispatch
        self._invalidate_contexts = invalidate_contexts
        self._refresh_entry_gate = refresh_entry_gate
        self._telemetry = telemetry
        self._clock = clock
        self._stream_available = not enabled
        self._state_ready = not enabled
        self._entry_blocked = False
        self._entry_block_reason = "risk_control_clear"
        self._reconcile_task: asyncio.Task[None] | None = None

    @property
    def stream_available(self) -> bool:
        return self._stream_available

    @property
    def state_ready(self) -> bool:
        return self._state_ready

    @property
    def entry_blocked(self) -> bool:
        return self._entry_blocked

    @property
    def entry_block_reason(self) -> str:
        return self._entry_block_reason

    def entry_gate(self) -> tuple[bool, str]:
        if not self._enabled:
            return False, "risk_control_disabled"
        if not self._stream_available or not self._state_ready:
            return (
                True,
                "risk_control_stream_unavailable"
                if not self._stream_available
                else "risk_control_state_recovering",
            )
        return self._entry_blocked, self._entry_block_reason

    def on_connection_change(
        self,
        available: bool,
        reason: str | None,
    ) -> None:
        if not self._enabled:
            return
        was_available = self._stream_available
        self._stream_available = available
        self._state_ready = False
        if not available:
            self._entry_blocked = True
            self._entry_block_reason = (
                reason or "risk_control_stream_unavailable"
            )
            self._invalidate_contexts()
        else:
            self._invalidate_contexts()
            self._schedule_reconcile()
        if self._telemetry is not None:
            self._telemetry.consumer_health(
                consumer="risk_control_hub",
                available=available,
                occurred_at=self._clock(),
                reason=reason,
                recovery=available and not was_available,
                lag=_risk_control_reason_is_lag(reason),
            )
        self._refresh_entry_gate()
        log.warning(
            "live_risk_control_stream_state_changed",
            session_id=self._session_id,
            available=available,
            reason=reason,
        )

    async def on_event(self, event: RiskControlEvent) -> None:
        if not self._enabled:
            return
        self._state_ready = False
        if event.action in self._ACTION_BLOCKING:
            self._entry_blocked = True
            self._entry_block_reason = (
                f"risk_control_{event.action.value}:{event.reason}"
            )
        self._invalidate_contexts()
        self._refresh_entry_gate()
        if event.action in self._ONE_SHOT_ACTIONS:
            try:
                failure = await self._dispatch(event)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                failure = (
                    "risk_control_action_dispatch_failed:"
                    f"{type(error).__name__}"
                )
                log.exception(
                    "live_risk_control_action_dispatch_failed",
                    session_id=self._session_id,
                    action=event.action.value,
                    command_id=event.command_id,
                    error_type=type(error).__name__,
                )
            if failure is not None:
                self._state_ready = True
                self._entry_blocked = True
                self._entry_block_reason = (
                    f"risk_control_{event.action.value}_failed:{failure}"
                )
                self._refresh_entry_gate()
                log.error(
                    "live_risk_control_action_failed",
                    session_id=self._session_id,
                    action=event.action.value,
                    command_id=event.command_id,
                    reason=failure,
                )
                return
        self._schedule_reconcile()
        log.warning(
            "live_risk_control_event_received",
            session_id=self._session_id,
            action=event.action.value,
            command_id=event.command_id,
            sequence=event.sequence,
            reason=event.reason,
        )

    async def reconcile(self) -> None:
        try:
            draining, active_halt = await self._load_durable_state()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._state_ready = False
            self._entry_blocked = True
            self._entry_block_reason = "risk_control_state_reload_failed"
            log.warning(
                "live_risk_control_state_reload_failed",
                session_id=self._session_id,
                error_type=type(error).__name__,
            )
        else:
            self._entry_blocked = draining or active_halt
            self._entry_block_reason = (
                "session_draining"
                if draining
                else "active_risk_halt"
                if active_halt
                else "risk_control_clear"
            )
            self._state_ready = True
            self._invalidate_contexts()
        self._refresh_entry_gate()

    def _schedule_reconcile(self) -> None:
        task = self._reconcile_task
        if task is not None and not task.done():
            task.cancel()
        self._reconcile_task = asyncio.create_task(
            self.reconcile(),
            name=f"live-risk-control-reconcile:{self._session_id}",
        )

    async def close(self) -> None:
        task = self._reconcile_task
        if task is None:
            return
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._reconcile_task = None


def _risk_control_reason_is_lag(reason: str | None) -> bool:
    if reason is None:
        return False
    normalized = reason.lower()
    return any(
        marker in normalized
        for marker in (
            "lag",
            "overflow",
            "sequence_gap",
            "sequencegap",
            "replay_unavailable",
        )
    )


__all__ = [
    "RiskControlCommandDispatcher",
    "RiskControlCommandRepository",
    "LiveRiskControlRuntime",
]
