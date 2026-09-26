"""Unit tests for recoverable deployment protocol and DeploymentCoordinator (R4).

Obeys Astra Architecture Blueprint 2026-09-25:
1. prepare -> apply -> status operator interface;
2. Schema incompatibility fails closed during preflight without side-effects;
3. Old writer failure prevents new epoch grant ("旧 writer 未停不能新发单");
4. Partial deployment failures record PARTIAL status and allow idempotent resume;
5. Successful operation returns cached receipt on repeat operation_id;
6. Monotonic fencing epoch handover across accounts.
"""

from __future__ import annotations

import pytest

from crypto_momentum_lab.domain.runtime.deployment_coordinator import (
    AccountDeploymentTarget,
    AccountTransitionStatus,
    DeploymentCoordinator,
    DeploymentManifest,
    DeploymentStatus,
    InMemoryDeploymentJournal,
    InMemoryWriterSupervisor,
    StaticDatabaseInspector,
)
from crypto_momentum_lab.domain.runtime.runtime_plan import (
    RuntimePlan,
    RuntimePlanCompiler,
)


def _make_plan(account: str, epoch: int, schema: str = "20260925_0043") -> RuntimePlan:
    return RuntimePlanCompiler.compile(
        environment="live",
        account_label=account,
        git_commit="abcdef123456",
        schema_version=schema,
        fencing_epoch=epoch,
    )


def _make_manifest(
    manifest_id: str = "rel_01",
    schema: str = "20260925_0043",
    account_2_epoch: int = 2,
) -> DeploymentManifest:
    p1 = _make_plan("primary", epoch=2, schema=schema)
    p2 = _make_plan("account-2", epoch=account_2_epoch, schema=schema)
    t1 = AccountDeploymentTarget(
        account_label="primary",
        target_plan=p1,
        target_epoch=2,
        target_generation="gen_primary_v2",
    )
    t2 = AccountDeploymentTarget(
        account_label="account-2",
        target_plan=p2,
        target_epoch=account_2_epoch,
        target_generation="gen_acc2_v2",
    )
    return DeploymentManifest(
        manifest_id=manifest_id,
        environment="live",
        build_ref="img_v2",
        git_commit="abcdef123456",
        declared_schema_version=schema,
        accounts=(t1, t2),
    )


@pytest.mark.asyncio
async def test_deployment_prepare_and_apply_happy_path() -> None:
    """Happy path deployment stops old writers, grants new epochs,
    and records SUCCESS.
    """
    supervisor = InMemoryWriterSupervisor(
        active_writers={"primary": True, "account-2": True},
        epochs={"primary": 1, "account-2": 1},
        generations={"primary": "gen_primary_v1", "account-2": "gen_acc2_v1"},
    )
    inspector = StaticDatabaseInspector("20260925_0043")
    journal = InMemoryDeploymentJournal()
    coordinator = DeploymentCoordinator(supervisor, inspector, journal)

    manifest = _make_manifest()

    # 1. Prepare
    candidate = await coordinator.prepare(manifest)
    assert candidate.is_valid is True
    assert len(candidate.blocking_reasons) == 0
    assert len(candidate.preflight_results) == 2
    assert all(r.passed for r in candidate.preflight_results)

    # 2. Apply
    receipt = await coordinator.apply(candidate, operation_id="op_deploy_01")
    assert receipt.status == DeploymentStatus.SUCCESS
    assert receipt.can_resume is False
    assert len(receipt.account_records) == 2

    # All accounts must have succeeded and upgraded epoch
    for r in receipt.account_records:
        assert r.status == AccountTransitionStatus.SUCCESS
        assert r.writer_stopped is True
        assert r.to_epoch == 2

    # Supervisor must reflect new epochs and active writers
    assert await supervisor.get_active_epoch("primary") == 2
    assert await supervisor.get_active_epoch("account-2") == 2
    assert await supervisor.check_writer_status("primary") is True
    assert await supervisor.check_writer_status("account-2") is True

    # 3. Status
    status_view = await coordinator.status()
    assert status_view.latest_operation_id == "op_deploy_01"
    assert status_view.latest_status == DeploymentStatus.SUCCESS
    assert status_view.current_fencing_epochs["primary"] == 2
    assert status_view.current_fencing_epochs["account-2"] == 2


@pytest.mark.asyncio
async def test_deployment_preflight_schema_mismatch_fails_closed() -> None:
    """Candidate preflight rejects when declared schema differs from database."""
    supervisor = InMemoryWriterSupervisor(epochs={"primary": 1, "account-2": 1})
    # Database is at 20260925_0043, but manifest requires 20260925_9999
    inspector = StaticDatabaseInspector("20260925_0043")
    journal = InMemoryDeploymentJournal()
    coordinator = DeploymentCoordinator(supervisor, inspector, journal)

    manifest = _make_manifest(schema="20260925_9999")

    candidate = await coordinator.prepare(manifest)
    assert candidate.is_valid is False
    assert any("schema_compatibility_mismatch" in r for r in candidate.blocking_reasons)

    # Apply must fail immediately without altering supervisor state
    receipt = await coordinator.apply(candidate, operation_id="op_fail_schema")
    assert receipt.status == DeploymentStatus.FAILED
    assert receipt.can_resume is False
    assert "schema_compatibility_mismatch" in (receipt.error_summary or "")


@pytest.mark.asyncio
async def test_deployment_old_writer_stop_failure_blocks_epoch_grant() -> None:
    """When old writer fails to stop, new epoch is NOT granted and status is PARTIAL."""
    supervisor = InMemoryWriterSupervisor(
        active_writers={"primary": True, "account-2": True},
        epochs={"primary": 1, "account-2": 1},
        stop_failure_accounts={"account-2"},  # account-2 writer hangs/refuses to stop
    )
    inspector = StaticDatabaseInspector("20260925_0043")
    journal = InMemoryDeploymentJournal()
    coordinator = DeploymentCoordinator(supervisor, inspector, journal)

    manifest = _make_manifest()
    candidate = await coordinator.prepare(manifest)
    assert candidate.is_valid is True

    receipt = await coordinator.apply(candidate, operation_id="op_partial_01")

    # Partial success: primary succeeded, account-2 failed
    assert receipt.status == DeploymentStatus.PARTIAL
    assert receipt.can_resume is True

    primary_rec = next(
        r for r in receipt.account_records if r.account_label == "primary"
    )
    acc2_rec = next(
        r for r in receipt.account_records if r.account_label == "account-2"
    )

    assert primary_rec.status == AccountTransitionStatus.SUCCESS
    assert acc2_rec.status == AccountTransitionStatus.FAILED
    assert acc2_rec.writer_stopped is False
    assert acc2_rec.error_message == "failed_to_stop_old_writer"

    # CRITICAL: account-2 epoch must NOT have been granted (still 1)
    assert await supervisor.get_active_epoch("primary") == 2
    assert await supervisor.get_active_epoch("account-2") == 1


@pytest.mark.asyncio
async def test_deployment_idempotent_resume_after_partial_failure() -> None:
    """Resuming deployment under same operation_id recovers without repeating
    successful accounts.
    """
    supervisor = InMemoryWriterSupervisor(
        active_writers={"primary": True, "account-2": True},
        epochs={"primary": 1, "account-2": 1},
        stop_failure_accounts={"account-2"},  # Fails initially
    )
    inspector = StaticDatabaseInspector("20260925_0043")
    journal = InMemoryDeploymentJournal()
    coordinator = DeploymentCoordinator(supervisor, inspector, journal)

    manifest = _make_manifest()
    candidate = await coordinator.prepare(manifest)

    # Attempt 1: Produces PARTIAL
    receipt1 = await coordinator.apply(candidate, operation_id="op_resume_01")
    assert receipt1.status == DeploymentStatus.PARTIAL

    # Fix the issue: writer on account-2 can now stop
    supervisor._stop_failures.clear()

    # Attempt 2: Same operation_id resumes
    receipt2 = await coordinator.apply(candidate, operation_id="op_resume_01")
    assert receipt2.status == DeploymentStatus.SUCCESS
    assert receipt2.can_resume is False

    # Both accounts now completed
    assert all(
        r.status == AccountTransitionStatus.SUCCESS
        for r in receipt2.account_records
    )
    assert await supervisor.get_active_epoch("primary") == 2
    assert await supervisor.get_active_epoch("account-2") == 2


@pytest.mark.asyncio
async def test_deployment_duplicate_operation_id_returns_cached_receipt() -> None:
    """Calling apply on an already SUCCESS operation_id immediately returns
    cached receipt.
    """
    supervisor = InMemoryWriterSupervisor(
        epochs={"primary": 1, "account-2": 1},
    )
    inspector = StaticDatabaseInspector("20260925_0043")
    journal = InMemoryDeploymentJournal()
    coordinator = DeploymentCoordinator(supervisor, inspector, journal)

    manifest = _make_manifest()
    candidate = await coordinator.prepare(manifest)

    receipt1 = await coordinator.apply(candidate, operation_id="op_cached_01")
    assert receipt1.status == DeploymentStatus.SUCCESS

    # Calling apply again with same operation_id returns exact same receipt
    receipt2 = await coordinator.apply(candidate, operation_id="op_cached_01")
    assert receipt2.operation_id == receipt1.operation_id
    assert receipt2.status == DeploymentStatus.SUCCESS
    assert receipt2.started_at == receipt1.started_at
