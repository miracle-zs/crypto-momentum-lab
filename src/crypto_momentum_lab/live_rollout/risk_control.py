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
from typing import Protocol

import structlog

from crypto_momentum_lab.domain.live_rollout import (
    RollbackCommand,
)
from crypto_momentum_lab.domain.live_rollout.authorization import (
    CANCEL_ALL_OPEN_ENTRIES_COMMAND,
    CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION,
    EMERGENCY_FLATTEN_COMMAND,
    EMERGENCY_FLATTEN_CONFIRMATION,
    require_authorized_command,
)
from crypto_momentum_lab.execution_account.risk_control_hub import (
    RiskControlAction,
    RiskControlEvent,
)
from crypto_momentum_lab.live_rollout.telemetry_ports import ConsumerHealthSink

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

    async def load_executing_commands_for_recovery(
        self,
        *,
        account_label: str,
        strategy_name: str,
        limit: int = 16,
    ) -> tuple[RollbackCommand, ...]: ...

    async def claim_executing_command_for_recovery(
        self,
        command_id: str,
        *,
        account_label: str,
        strategy_name: str,
        session_id: str,
    ) -> RollbackCommand | None: ...



@dataclass(frozen=True, slots=True)
class _RiskControlCommandSpec:
    command_type: str
    confirmation_text: str
    execute: Callable[[datetime], Awaitable[str | None]]


class RiskControlCommandDispatcher:
    """Claim, authorize, execute, and complete a streamed control command."""

    def __init__(
        self,
        *,
        repository: RiskControlCommandRepository,
        account_label: str,
        strategy_name: str,
        session_id: str,
        cancel_all_open_entries: Callable[[datetime], Awaitable[str | None]],
        request_flatten: Callable[[datetime], Awaitable[str | None]],
        clock: Callable[[], datetime],
    ) -> None:
        self._repository = repository
        self._account_label = account_label
        self._strategy_name = strategy_name
        self._session_id = session_id
        self._clock = clock
        self._recovered_command_ids: set[str] = set()
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
        # The action identity is stable for the command, so an action that is
        # resumed after a crash continues the same order identity instead of
        # creating a second flatten at a new time.
        identity_at = command.requested_at
        try:
            require_authorized_command(
                command,
                command_type=spec.command_type,
                confirmation_text=spec.confirmation_text,
            )
            failure = await spec.execute(identity_at)
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

    async def recover_executing_commands(self, *, limit: int = 16) -> int:
        """Continue operator work stranded in ``executing`` by a restart.

        A command is claimed (``executing``) before its action runs.  If the
        process dies anywhere between the claim and the completion record, the
        responsibility still exists: cancel-all and flatten are idempotent
        operator intents, so recovery re-verifies authorization, re-issues the
        action against the *current* durable state (never blindly re-executing a
        logged order plan) and then records the outcome.

        Returns the number of commands whose responsibility was concluded.  A
        command that is already completed between the load and the recovery is
        counted as concluded without repeating the action.
        """

        recovered = 0
        for command in await self._repository.load_executing_commands_for_recovery(
            account_label=self._account_label,
            strategy_name=self._strategy_name,
            limit=limit,
        ):
            if command.command_id in self._recovered_command_ids:
                continue
            spec = next(
                (
                    candidate
                    for candidate in self._specs.values()
                    if candidate.command_type == command.command_type
                ),
                None,
            )
            if spec is None:
                # Unknown operator responsibility: leave the row untouched and
                # visible instead of guessing an action.
                log.error(
                    "live_risk_control_executing_command_unsupported",
                    command_id=command.command_id,
                    command_type=command.command_type,
                )
                continue
            current = await self._repository.load_command(command.command_id)
            if current is None:
                continue
            if current.status != "executing":
                # Another worker concluded it while we were loading.
                self._recovered_command_ids.add(command.command_id)
                recovered += 1
                continue
            # Serialize recovery on the command record itself: only the
            # recoverer that takes the row over may continue its action.
            claimed = await self._repository.claim_executing_command_for_recovery(
                command.command_id,
                account_label=self._account_label,
                strategy_name=self._strategy_name,
                session_id=self._session_id,
            )
            if claimed is None:
                log.warning(
                    "live_risk_control_recovery_already_owned",
                    command_id=command.command_id,
                )
                continue
            failure: str | None = None
            try:
                require_authorized_command(
                    claimed,
                    command_type=spec.command_type,
                    confirmation_text=spec.confirmation_text,
                )
                # The command record is the recovery record: the stable
                # command_id plus the stable action identity continue the same
                # action instead of creating another flatten.  `complete_command`
                # only succeeds from `executing`, so exactly one consumer can
                # conclude it.
                failure = await spec.execute(claimed.requested_at)
            except Exception as error:
                failure = f"risk_control_action_failed:{type(error).__name__}"
                log.exception(
                    "live_risk_control_recovery_failed",
                    command_id=command.command_id,
                    error_type=type(error).__name__,
                )
            completed = await self._repository.complete_command(
                command.command_id,
                status="failed" if failure is not None else "completed",
                completed_at=self._clock(),
                failure_reason=failure,
            )
            if completed:
                self._recovered_command_ids.add(command.command_id)
                recovered += 1
            else:
                log.warning(
                    "live_risk_control_recovery_not_recorded",
                    command_id=command.command_id,
                    failure=failure,
                )
        return recovered


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
        telemetry: ConsumerHealthSink | None,
        clock: Callable[[], datetime],
        reconcile_retry_initial_seconds: float = 1.0,
        reconcile_retry_max_seconds: float = 30.0,
        database_mode: bool = False,
    ) -> None:
        if not session_id.strip():
            raise ValueError("session_id must not be empty")
        if not isinstance(enabled, bool):
            raise TypeError("enabled must be a bool")
        if reconcile_retry_initial_seconds <= 0:
            raise ValueError("reconcile_retry_initial_seconds must be positive")
        if reconcile_retry_max_seconds < reconcile_retry_initial_seconds:
            raise ValueError(
                "reconcile_retry_max_seconds must not be below "
                "reconcile_retry_initial_seconds"
            )
        self._enabled = enabled
        self._session_id = session_id
        self._load_durable_state = load_durable_state
        self._dispatch = dispatch
        self._invalidate_contexts = invalidate_contexts
        self._refresh_entry_gate = refresh_entry_gate
        self._telemetry = telemetry
        self._clock = clock
        self._database_mode = database_mode
        self._database_available = False if (enabled and database_mode) else True
        self._stream_available = True if database_mode else not enabled
        self._state_ready = not enabled
        self._entry_blocked = False
        self._entry_block_reason = (
            "risk_control_database_initializing"
            if (enabled and database_mode)
            else "risk_control_clear"
        )
        self._reconcile_task: asyncio.Task[None] | None = None
        self._retry_task: asyncio.Task[None] | None = None
        self._reconcile_lock = asyncio.Lock()
        self._reconcile_retry_initial_seconds = reconcile_retry_initial_seconds
        self._reconcile_retry_max_seconds = reconcile_retry_max_seconds

    @property
    def database_mode(self) -> bool:
        return self._database_mode

    @property
    def database_available(self) -> bool:
        return self._database_available

    @property
    def state_ready(self) -> bool:
        return self._state_ready

    @property
    def entry_block_reason(self) -> str:
        return self._entry_block_reason

    def mark_database_failed(
        self, reason: str = "risk_control_database_unavailable"
    ) -> None:
        """Explicitly record a database polling failure and block new entries fail-closed."""
        if not self._enabled:
            return
        self._database_available = False
        self._state_ready = False
        self._entry_blocked = True
        self._entry_block_reason = reason
        self._refresh_entry_gate()

    def entry_gate(self) -> tuple[bool, str]:
        if not self._enabled:
            return False, "risk_control_disabled"
        if self._database_mode:
            if not self._database_available or not self._state_ready:
                return (
                    True,
                    "risk_control_database_unavailable"
                    if not self._database_available
                    else "risk_control_state_recovering",
                )
            return self._entry_blocked, self._entry_block_reason
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
            self._entry_block_reason = reason or "risk_control_stream_unavailable"
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
            except Exception as error:
                failure = f"risk_control_action_dispatch_failed:{type(error).__name__}"
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
        async with self._reconcile_lock:
            try:
                draining, active_halt = await self._load_durable_state()
            except Exception as error:
                self._state_ready = False
                self._entry_blocked = True
                if self._database_mode:
                    self._database_available = False
                    self._entry_block_reason = "risk_control_database_unavailable"
                else:
                    self._entry_block_reason = "risk_control_state_reload_failed"
                log.warning(
                    "live_risk_control_state_reload_failed",
                    session_id=self._session_id,
                    error_type=type(error).__name__,
                )
                self._schedule_retry()
            else:
                if self._database_mode:
                    self._database_available = True
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

    def _schedule_retry(self) -> None:
        task = self._retry_task
        if task is not None and not task.done():
            return
        self._retry_task = asyncio.create_task(
            self._retry_until_ready(),
            name=f"live-risk-control-retry:{self._session_id}",
        )

    async def _retry_until_ready(self) -> None:
        delay = self._reconcile_retry_initial_seconds
        try:
            while self._enabled and self._stream_available and not self._state_ready:
                await asyncio.sleep(delay)
                if not self._enabled or not self._stream_available or self._state_ready:
                    return
                await self.reconcile()
                delay = min(delay * 2, self._reconcile_retry_max_seconds)
        finally:
            self._retry_task = None

    async def close(self) -> None:
        tasks = tuple(
            task
            for task in (self._reconcile_task, self._retry_task)
            if task is not None
        )
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._reconcile_task = None
        self._retry_task = None


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
