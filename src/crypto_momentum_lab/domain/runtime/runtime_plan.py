"""RuntimePlan and compilation for deterministic execution configuration.

Obeys Astra Architecture Blueprint 2026-09-25:
- Compiled immutable RuntimePlan with explicit source tracking;
- Secrets stored by reference only; never enumerable in hash or payload;
- Prohibits runtime execution paths from reading dynamic environment variables;
- Pinned hashes for strategy, execution policy, risk policy, and deployment.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from crypto_momentum_lab.domain.decision.decision_engine import EffectivePolicy
from crypto_momentum_lab.domain.strategy.models import EntryType
from crypto_momentum_lab.domain.strategy.position_exit import (
    PositionExitMode,
    PositionExitPolicy,
)


@dataclass(frozen=True, slots=True)
class RuntimePlan:
    """Immutable compiled runtime plan governing a daemon or process lifecycle."""

    plan_id: str
    environment: str
    account_label: str
    strategy_hash: str
    execution_policy_hash: str
    risk_policy_hash: str
    deployment_hash: str
    schema_compatibility_version: str
    effective_policy: EffectivePolicy
    options_source_chain: dict[str, str] = field(default_factory=dict)
    secret_references: dict[str, str] = field(default_factory=dict)
    compiled_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __post_init__(self) -> None:
        if not self.plan_id.strip():
            raise ValueError("plan_id must not be empty")
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if not self.strategy_hash.strip():
            raise ValueError("strategy_hash must not be empty")
        if not self.deployment_hash.strip():
            raise ValueError("deployment_hash must not be empty")


class RuntimePlanCompiler:
    """Compiles configuration and overrides into an immutable RuntimePlan."""

    @classmethod
    def compile(
        cls,
        *,
        environment: str,
        account_label: str,
        strategy_name: str = "orderflow_impulse",
        git_commit: str = "unknown_commit",
        schema_version: str = "20260925_0042",
        overrides: dict[str, Any] | None = None,
        secret_keys: tuple[str, ...] = ("BINANCE_API_KEY", "BINANCE_API_SECRET"),
    ) -> RuntimePlan:
        """Statically compiles configuration options into an immutable RuntimePlan."""
        user_overrides = overrides or {}
        sources: dict[str, str] = {}

        # 1. Resolve strategy parameters
        entry_thresh = user_overrides.get("entry_threshold", Decimal("65000.00"))
        sources["entry_threshold"] = (
            "override" if "entry_threshold" in user_overrides else "default"
        )

        target_notional = user_overrides.get("target_notional", Decimal("500.00"))
        sources["target_notional"] = (
            "override" if "target_notional" in user_overrides else "default"
        )

        order_type_str = user_overrides.get("order_type", "market")
        order_type = EntryType.LIMIT if order_type_str == "limit" else EntryType.MARKET
        sources["order_type"] = (
            "override" if "order_type" in user_overrides else "default"
        )

        exit_policy = PositionExitPolicy(
            max_holding_seconds=user_overrides.get("max_holding_seconds", 1200),
            mode=PositionExitMode.CANDLE_15M,
        )
        sources["exit_policy"] = (
            "override" if "max_holding_seconds" in user_overrides else "default"
        )

        # 2. Compute Hashes
        strat_payload = {
            "strategy_name": strategy_name,
            "entry_threshold": str(entry_thresh),
            "order_type": order_type.value,
        }
        strat_hash = hashlib.sha256(
            json.dumps(strat_payload, sort_keys=True).encode()
        ).hexdigest()

        exec_payload = {
            "target_notional": str(target_notional),
            "max_holding_seconds": exit_policy.max_holding_seconds,
        }
        exec_hash = hashlib.sha256(
            json.dumps(exec_payload, sort_keys=True).encode()
        ).hexdigest()

        risk_payload = {
            "max_open_positions": 4,
            "max_account_drawdown": "0.10",
        }
        risk_hash = hashlib.sha256(
            json.dumps(risk_payload, sort_keys=True).encode()
        ).hexdigest()

        deployment_hash = hashlib.sha256(
            f"{environment}:{git_commit}:{schema_version}".encode()
        ).hexdigest()

        plan_id = f"plan_{environment}_{account_label}_{strat_hash[:8]}_{exec_hash[:8]}"

        policy = EffectivePolicy(
            policy_id=strat_hash[:16],
            strategy_name=strategy_name,
            policy_version=1,
            entry_threshold=Decimal(str(entry_thresh)),
            order_type=order_type,
            target_notional=Decimal(str(target_notional)),
            exit_policy=exit_policy,
        )

        # Secrets recorded by reference only
        secret_refs = {k: f"env_ref:{k}" for k in secret_keys}

        return RuntimePlan(
            plan_id=plan_id,
            environment=environment,
            account_label=account_label,
            strategy_hash=strat_hash,
            execution_policy_hash=exec_hash,
            risk_policy_hash=risk_hash,
            deployment_hash=deployment_hash,
            schema_compatibility_version=schema_version,
            effective_policy=policy,
            options_source_chain=sources,
            secret_references=secret_refs,
            compiled_at=datetime.now(UTC),
        )
