import asyncio
from dataclasses import replace
from datetime import UTC, datetime

from crypto_momentum_lab.domain.live_rollout import RollbackCommand
from crypto_momentum_lab.execution_account.risk_control_hub import (
    RiskControlAction,
    RiskControlEvent,
)
from crypto_momentum_lab.live_rollout.commands import (
    CANCEL_ALL_OPEN_ENTRIES_COMMAND,
    CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION,
)
from crypto_momentum_lab.live_rollout.risk_control import (
    LiveRiskControlRuntime,
    RiskControlCommandDispatcher,
)

NOW = datetime(2026, 9, 11, 6, 0, tzinfo=UTC)


class FakeCommandRepository:
    def __init__(self, command: RollbackCommand) -> None:
        self.command = command
        self.claim_count = 0
        self.completed: list[tuple[str, str, str | None]] = []

    async def load_command(self, command_id: str) -> RollbackCommand | None:
        if command_id != self.command.command_id:
            return None
        return self.command

    async def claim_command(
        self,
        command_id: str,
        *,
        account_label: str,
        strategy_name: str,
        session_id: str,
    ) -> RollbackCommand | None:
        if (
            self.command.command_id != command_id
            or self.command.account_label != account_label
            or self.command.strategy_name != strategy_name
            or self.command.session_id != session_id
            or self.command.status != "requested"
        ):
            return None
        self.claim_count += 1
        self.command = replace(self.command, status="executing")
        return self.command

    async def complete_command(
        self,
        command_id: str,
        *,
        status: str,
        completed_at: datetime,
        failure_reason: str | None,
    ) -> bool:
        del completed_at
        if (
            command_id != self.command.command_id
            or self.command.status != "executing"
        ):
            return False
        self.command = replace(self.command, status=status)
        self.completed.append((command_id, status, failure_reason))
        return True


def _command(
    *,
    confirmation_text: str = CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION,
) -> RollbackCommand:
    return RollbackCommand(
        command_id="command-1",
        command_type=CANCEL_ALL_OPEN_ENTRIES_COMMAND,
        requested_by="operator",
        confirmation_text=confirmation_text,
        requested_at=NOW,
        idempotency_key="cancel-1",
        account_label="primary",
        strategy_name="orderflow_impulse",
        session_id="live-1",
        status="requested",
        completed_at=None,
        failure_reason=None,
    )


def _event() -> RiskControlEvent:
    return RiskControlEvent(
        environment="live",
        account_label="primary",
        strategy_name="orderflow_impulse",
        session_id="live-1",
        action=RiskControlAction.CANCEL_ALL_OPEN_ENTRIES,
        event_id="command-1",
        command_id="command-1",
        reason="operator_cancelled_all_open_entries",
        issued_at=NOW,
        details={"command_type": CANCEL_ALL_OPEN_ENTRIES_COMMAND},
    )


async def test_dispatcher_claims_and_completes_one_shot_action_once() -> None:
    repository = FakeCommandRepository(_command())
    calls: list[str] = []
    dispatcher = RiskControlCommandDispatcher(
        repository=repository,
        account_label="primary",
        strategy_name="orderflow_impulse",
        session_id="live-1",
        cancel_all_open_entries=lambda: _record(calls, "cancel"),
        request_flatten=lambda: _record(calls, "flatten"),
        clock=lambda: NOW,
    )

    assert await dispatcher.dispatch(_event()) is None
    assert await dispatcher.dispatch(_event()) is None
    assert calls == ["cancel"]
    assert repository.claim_count == 1
    assert repository.completed == [("command-1", "completed", None)]


async def test_dispatcher_rejects_event_without_matching_command_type() -> None:
    repository = FakeCommandRepository(_command())
    calls: list[str] = []
    dispatcher = RiskControlCommandDispatcher(
        repository=repository,
        account_label="primary",
        strategy_name="orderflow_impulse",
        session_id="live-1",
        cancel_all_open_entries=lambda: _record(calls, "cancel"),
        request_flatten=lambda: _record(calls, "flatten"),
        clock=lambda: NOW,
    )

    event = replace(_event(), details={"command_type": "wrong"})

    assert await dispatcher.dispatch(event) == "risk_control_command_type_mismatch"
    assert calls == []
    assert repository.claim_count == 0


async def test_risk_control_runtime_reconciles_and_fails_closed_on_disconnect() -> None:
    refreshes: list[tuple[bool, str]] = []
    invalidations: list[bool] = []

    async def load_durable_state() -> tuple[bool, bool]:
        return False, True

    runtime = LiveRiskControlRuntime(
        enabled=True,
        session_id="live-1",
        load_durable_state=load_durable_state,
        dispatch=lambda _event: _record_failure(),
        invalidate_contexts=lambda: invalidations.append(True),
        refresh_entry_gate=lambda: refreshes.append(runtime.entry_gate()),
        telemetry=None,
        clock=lambda: NOW,
    )

    assert runtime.entry_gate() == (
        True,
        "risk_control_stream_unavailable",
    )
    runtime.on_connection_change(False, "risk_control_queue_overflow")
    assert runtime.entry_gate() == (
        True,
        "risk_control_stream_unavailable",
    )
    assert runtime.entry_block_reason == "risk_control_queue_overflow"

    await runtime.reconcile()

    assert runtime.entry_gate() == (
        True,
        "risk_control_stream_unavailable",
    )
    assert invalidations == [True, True]
    assert refreshes
    await runtime.close()


async def test_risk_control_runtime_dispatch_failure_keeps_entries_blocked() -> None:
    refreshes: list[tuple[bool, str]] = []
    dispatched: list[str] = []

    async def load_durable_state() -> tuple[bool, bool]:
        return False, False

    async def dispatch(event: RiskControlEvent) -> str:
        dispatched.append(event.command_id)
        return "command_failed"

    runtime = LiveRiskControlRuntime(
        enabled=True,
        session_id="live-1",
        load_durable_state=load_durable_state,
        dispatch=dispatch,
        invalidate_contexts=lambda: None,
        refresh_entry_gate=lambda: refreshes.append(runtime.entry_gate()),
        telemetry=None,
        clock=lambda: NOW,
    )
    runtime.on_connection_change(True, None)
    await asyncio.sleep(0)
    await runtime.close()

    await runtime.on_event(_event())

    assert dispatched == ["command-1"]
    assert runtime.entry_gate() == (
        True,
        "risk_control_cancel_all_open_entries_failed:command_failed",
    )
    assert refreshes


async def _record(calls: list[str], value: str) -> None:
    calls.append(value)
    return None


async def _record_failure() -> str:
    return "unused"
