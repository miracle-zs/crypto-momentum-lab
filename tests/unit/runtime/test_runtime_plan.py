"""Unit tests for RuntimePlan and RuntimePlanCompiler (R4).

Tests:
1. Static compilation of RuntimePlan produces immutable, pinned hashes;
2. Secrets are recorded by reference only, never raw credentials;
3. Options source chain correctly attributes defaults vs user overrides;
4. Invariants and post-init validations.
"""

from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.runtime.runtime_plan import (
    RuntimePlan,
    RuntimePlanCompiler,
)
from crypto_momentum_lab.domain.strategy.models import EntryType


def test_runtime_plan_compilation_deterministic_hashes() -> None:
    plan1 = RuntimePlanCompiler.compile(
        environment="live",
        account_label="binance_primary",
        git_commit="abcdef123456",
        schema_version="20260925_0042",
        overrides={
            "entry_threshold": Decimal("64000.00"),
            "target_notional": Decimal("1000.00"),
            "order_type": "limit",
        },
    )

    plan2 = RuntimePlanCompiler.compile(
        environment="live",
        account_label="binance_primary",
        git_commit="abcdef123456",
        schema_version="20260925_0042",
        overrides={
            "entry_threshold": Decimal("64000.00"),
            "target_notional": Decimal("1000.00"),
            "order_type": "limit",
        },
    )

    # Identical compilation inputs must yield identical plan_id and hashes
    assert plan1.plan_id == plan2.plan_id
    assert plan1.strategy_hash == plan2.strategy_hash
    assert plan1.execution_policy_hash == plan2.execution_policy_hash
    assert plan1.deployment_hash == plan2.deployment_hash
    assert plan1.effective_policy.order_type == EntryType.LIMIT
    assert plan1.effective_policy.entry_threshold == Decimal("64000.00")
    assert plan1.effective_policy.target_notional == Decimal("1000.00")


def test_runtime_plan_secret_references_safety() -> None:
    plan = RuntimePlanCompiler.compile(
        environment="live",
        account_label="binance_primary",
        git_commit="commit_hash",
        secret_keys=("BINANCE_API_KEY", "BINANCE_API_SECRET", "SLACK_WEBHOOK"),
    )

    # Raw secrets must never appear in runtime plan
    assert "BINANCE_API_KEY" in plan.secret_references
    assert plan.secret_references["BINANCE_API_KEY"] == "env_ref:BINANCE_API_KEY"
    assert "BINANCE_API_SECRET" in plan.secret_references
    assert plan.secret_references["BINANCE_API_SECRET"] == "env_ref:BINANCE_API_SECRET"
    assert "SLACK_WEBHOOK" in plan.secret_references


def test_runtime_plan_source_chain_tracking() -> None:
    plan = RuntimePlanCompiler.compile(
        environment="paper",
        account_label="paper_account",
        overrides={
            "entry_threshold": Decimal("62000.00"),
        },
    )

    assert plan.options_source_chain["entry_threshold"] == "override"
    assert plan.options_source_chain["target_notional"] == "default"
    assert plan.options_source_chain["order_type"] == "default"


def test_runtime_plan_invalid_post_init() -> None:
    with pytest.raises(ValueError, match="plan_id must not be empty"):
        RuntimePlan(
            plan_id="",
            environment="live",
            account_label="acc",
            strategy_hash="hash",
            execution_policy_hash="hash",
            risk_policy_hash="hash",
            deployment_hash="hash",
            schema_compatibility_version="v1",
            effective_policy=RuntimePlanCompiler.compile(
                environment="live", account_label="acc"
            ).effective_policy,
        )
