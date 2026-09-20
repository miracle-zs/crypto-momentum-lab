"""Architectural decoupling invariant tests for execution_account package."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import crypto_momentum_lab.execution_account as execution_account_pkg
from crypto_momentum_lab.domain.live_rollout.authorization import (
    EMERGENCY_FLATTEN_CONFIRMATION,
    require_authorized_command,
)
from crypto_momentum_lab.domain.live_rollout.models import RollbackCommand


def test_execution_account_has_no_dependency_on_live_rollout() -> None:
    """execution_account is an infrastructure/adapter layer and must NEVER depend on live_rollout."""
    pkg_dir = Path(execution_account_pkg.__file__).parent
    py_files = list(pkg_dir.rglob("*.py"))
    assert len(py_files) > 0, "No python files found in execution_account"

    forbidden_prefix = "crypto_momentum_lab.live_rollout"

    violations: list[str] = []
    for py_file in py_files:
        tree = ast.parse(py_file.read_text("utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == forbidden_prefix or alias.name.startswith(
                        forbidden_prefix + "."
                    ):
                        violations.append(
                            f"{py_file.relative_to(pkg_dir)}: import {alias.name}"
                        )
            elif isinstance(node, ast.ImportFrom):
                if node.module and (
                    node.module == forbidden_prefix
                    or node.module.startswith(forbidden_prefix + ".")
                ):
                    violations.append(
                        f"{py_file.relative_to(pkg_dir)}: from {node.module} import ..."
                    )

    assert not violations, (
        f"Found forbidden imports from live_rollout in execution_account: {violations}"
    )


def test_domain_authorization_require_authorized_command() -> None:
    """Verify authorization domain contract validates operator commands correctly."""
    from datetime import datetime, timezone

    cmd = RollbackCommand(
        command_id="cmd-1",
        command_type="emergency_flatten",
        requested_by="operator-1",
        confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
        requested_at=datetime.now(timezone.utc),
        idempotency_key="idemp-1",
        account_label="binance-prod",
        strategy_name="top_momentum",
        session_id="session-1",
        status="requested",
        completed_at=None,
        failure_reason=None,
    )

    validated = require_authorized_command(
        cmd,
        command_type="emergency_flatten",
        confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
    )
    assert validated.command_id == "cmd-1"

    # None command
    with pytest.raises(PermissionError, match="persisted operator command is required"):
        require_authorized_command(
            None,
            command_type="emergency_flatten",
            confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
        )

    # Wrong command_type
    with pytest.raises(PermissionError, match="operator command type mismatch"):
        require_authorized_command(
            cmd,
            command_type="cancel_all_open_entries",
            confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
        )

    # Wrong confirmation
    with pytest.raises(PermissionError, match="operator command confirmation mismatch"):
        require_authorized_command(
            cmd,
            command_type="emergency_flatten",
            confirmation_text="WRONG CONFIRMATION",
        )
