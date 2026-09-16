"""Persist process-state transitions; refresh the same state on a timer."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from crypto_momentum_lab.domain.account import (
    ExecutionAccountProcessState,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.execution_account.sync import (
    ExecutionAccountSyncConfig,
    ExecutionAccountSyncService,
)


class _RecordingRepo:
    def __init__(self) -> None:
        self.states: list[ExecutionAccountProcessState] = []

    async def save_process_state(self, state: ExecutionAccountProcessState) -> None:
        self.states.append(state)


class _NoopClient:
    pass


def _config_at(observed_at: datetime) -> ExecutionAccountSyncConfig:
    return ExecutionAccountSyncConfig(
        environment="live",
        account_label="primary",
        expected_multi_assets_mode=False,
        expected_hedge_mode=True,
        observed_at=observed_at,
        historical_fill_reconciliation_interval_seconds=60,
    )


def _service(
    repo: _RecordingRepo, *, observed_at: datetime
) -> ExecutionAccountSyncService:
    return ExecutionAccountSyncService(
        client=_NoopClient(),  # type: ignore[arg-type]
        repository=repo,  # type: ignore[arg-type]
        config=_config_at(observed_at),
    )


async def test_same_state_is_not_repersisted_within_refresh_window() -> None:
    t0 = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)
    repo = _RecordingRepo()
    service = _service(repo, observed_at=t0)

    await service._save_state(
        ExecutionAccountStatus.READY_READONLY,
        config=service._config,
    )
    await service._save_state(
        ExecutionAccountStatus.READY_READONLY,
        config=_config_at(t0 + timedelta(minutes=1)),
    )
    assert len(repo.states) == 1

    await service._save_state(
        ExecutionAccountStatus.READY_READONLY,
        config=_config_at(t0 + timedelta(minutes=6)),
    )
    assert len(repo.states) == 2


async def test_state_transition_always_persists() -> None:
    t0 = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)
    repo = _RecordingRepo()
    service = _service(repo, observed_at=t0)

    await service._save_state(ExecutionAccountStatus.SYNCING, config=service._config)
    await service._save_state(
        ExecutionAccountStatus.READY_READONLY,
        config=_config_at(t0 + timedelta(seconds=10)),
    )
    assert [row.state for row in repo.states] == [
        ExecutionAccountStatus.SYNCING,
        ExecutionAccountStatus.READY_READONLY,
    ]
