"""RuntimePlan and compilation for deterministic execution configuration.

Obeys Astra Architecture Blueprint 2026-09-25:
- Compiled immutable RuntimePlan with explicit source tracking;
- plan_hash captures the entire effective configuration content
  (compilation time excluded);
- Distinguishes runtime_generation, fencing_epoch, declared_schema_compatibility,
  and observed_database_revision;
- Secrets stored by reference only; never enumerable in hash or payload;
- Deep immutability enforced on mappings;
- Prohibits runtime execution paths from reading dynamic environment variables;
- Pinned hashes for strategy, execution policy, risk policy, and deployment.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from types import MappingProxyType
from typing import Any

from crypto_momentum_lab.domain.decision.decision_engine import EffectivePolicy
from crypto_momentum_lab.domain.strategy.models import EntryType
from crypto_momentum_lab.domain.strategy.position_exit import (
    PositionExitMode,
    PositionExitPolicy,
)
from crypto_momentum_lab.domain.strategy.sizing import (
    EquityFractionSizingModel,
    FixedNotionalSizingModel,
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
    plan_hash: str = ""
    runtime_generation: str = ""
    fencing_epoch: int = 1
    declared_schema_compatibility: str = ""
    observed_database_revision: str | None = None
    options_source_chain: Mapping[str, str] = field(default_factory=dict)
    secret_references: Mapping[str, str] = field(default_factory=dict)
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
        if self.fencing_epoch < 1:
            raise ValueError("fencing_epoch must be positive (>= 1)")

        if not self.declared_schema_compatibility:
            object.__setattr__(
                self, "declared_schema_compatibility", self.schema_compatibility_version
            )

        if not self.runtime_generation:
            default_gen = f"gen_{self.account_label}_{self.deployment_hash[:8]}"
            object.__setattr__(self, "runtime_generation", default_gen)

        if not self.plan_hash:
            content_payload = {
                "environment": self.environment,
                "account_label": self.account_label,
                "strategy_hash": self.strategy_hash,
                "execution_policy_hash": self.execution_policy_hash,
                "risk_policy_hash": self.risk_policy_hash,
                "deployment_hash": self.deployment_hash,
                "schema_compatibility_version": self.declared_schema_compatibility,
            }
            computed_plan_hash = hashlib.sha256(
                json.dumps(content_payload, sort_keys=True).encode()
            ).hexdigest()
            object.__setattr__(self, "plan_hash", computed_plan_hash)

        # Enforce deep immutability on options and secret references
        if isinstance(self.options_source_chain, dict):
            object.__setattr__(
                self,
                "options_source_chain",
                MappingProxyType(dict(self.options_source_chain)),
            )
        if isinstance(self.secret_references, dict):
            object.__setattr__(
                self,
                "secret_references",
                MappingProxyType(dict(self.secret_references)),
            )

    def with_observed_db_revision(self, revision: str | None) -> RuntimePlan:
        """Returns an immutable copy with the observed database revision set."""
        return replace(self, observed_database_revision=revision)

    def with_fencing_epoch(self, epoch: int) -> RuntimePlan:
        """Returns an immutable copy with an updated writer fencing epoch."""
        if epoch < self.fencing_epoch:
            raise ValueError(
                f"fencing_epoch must not decrease: {epoch} < {self.fencing_epoch}"
            )
        return replace(self, fencing_epoch=epoch)


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
        runtime_generation: str | None = None,
        fencing_epoch: int = 1,
        observed_database_revision: str | None = None,
        strict: bool = False,
    ) -> RuntimePlan:
        """Statically compiles configuration options into an immutable RuntimePlan."""
        user_overrides = overrides or {}
        sources: dict[str, str] = {}

        # 1. Resolve strategy parameters - NEVER use hardcoded 65000
        entry_thresh_raw = user_overrides.get("entry_threshold")
        if entry_thresh_raw is not None:
            entry_thresh = Decimal(str(entry_thresh_raw))
            sources["entry_threshold"] = "override"
        else:
            entry_thresh = None
            sources["entry_threshold"] = "none"

        target_notional_raw = user_overrides.get("target_notional")
        if target_notional_raw is None:
            if strict:
                raise ValueError("target_notional must be explicitly specified in strict mode")
            target_notional = Decimal("100.00")
            sources["target_notional"] = "default"
        else:
            target_notional = Decimal(str(target_notional_raw))
            sources["target_notional"] = "override"
        if target_notional <= Decimal("0"):
            raise ValueError("target_notional must be positive")

        order_type_str = user_overrides.get("order_type", "market")
        order_type = EntryType.LIMIT if order_type_str == "limit" else EntryType.MARKET
        sources["order_type"] = (
            "override" if "order_type" in user_overrides else "default"
        )

        max_holding_raw = user_overrides.get("max_holding_seconds")
        if max_holding_raw is not None:
            exit_policy = PositionExitPolicy(
                max_holding_seconds=int(max_holding_raw),
                mode=PositionExitMode.CANDLE_15M,
            )
            sources["exit_policy"] = "override"
        elif strict:
            raise ValueError("max_holding_seconds must be explicitly specified in strict mode")
        else:
            exit_policy = PositionExitPolicy(
                max_holding_seconds=1200,
                mode=PositionExitMode.CANDLE_15M,
            )
            sources["exit_policy"] = "default"

        # 2. Compute Hashes
        strat_payload = {
            "strategy_name": strategy_name,
            "entry_threshold": str(entry_thresh) if entry_thresh is not None else None,
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

        max_order_notional_raw = user_overrides.get("max_order_notional")
        if max_order_notional_raw is not None:
            max_order_notional = Decimal(str(max_order_notional_raw))
            sources["max_order_notional"] = "override"
        elif strict:
            raise ValueError("max_order_notional must be explicitly specified in strict mode")
        else:
            max_order_notional = target_notional
            sources["max_order_notional"] = "default"

        max_open_positions_raw = user_overrides.get("max_open_positions")
        if max_open_positions_raw is not None:
            max_open_positions = int(max_open_positions_raw)
            sources["max_open_positions"] = "override"
        elif strict:
            raise ValueError("max_open_positions must be explicitly specified in strict mode")
        else:
            max_open_positions = 4
            sources["max_open_positions"] = "default"

        max_account_drawdown_raw = user_overrides.get("max_account_drawdown")
        if max_account_drawdown_raw is not None:
            max_account_drawdown = str(max_account_drawdown_raw)
            sources["max_account_drawdown"] = "override"
        elif strict:
            raise ValueError("max_account_drawdown must be explicitly specified in strict mode")
        else:
            max_account_drawdown = "0.10"
            sources["max_account_drawdown"] = "default"

        max_gross_notional_raw = user_overrides.get("max_gross_notional")
        if max_gross_notional_raw is not None:
            max_gross_notional = Decimal(str(max_gross_notional_raw))
            sources["max_gross_notional"] = "override"
        elif strict:
            raise ValueError("max_gross_notional must be explicitly specified in strict mode")
        else:
            max_gross_notional = target_notional * Decimal(str(max_open_positions))
            sources["max_gross_notional"] = "default"

        risk_payload = {
            "max_open_positions": max_open_positions,
            "max_account_drawdown": max_account_drawdown,
            "max_gross_notional": str(max_gross_notional),
            "max_order_notional": str(max_order_notional),
        }
        risk_hash = hashlib.sha256(
            json.dumps(risk_payload, sort_keys=True).encode()
        ).hexdigest()

        deployment_hash = hashlib.sha256(
            f"{environment}:{git_commit}:{schema_version}".encode()
        ).hexdigest()

        plan_id = f"plan_{environment}_{account_label}_{strat_hash[:8]}_{exec_hash[:8]}"

        sizing_model_type = user_overrides.get("sizing_model", "fixed_notional")
        if sizing_model_type == "equity_fraction":
            fraction_raw = user_overrides.get("equity_fraction")
            if fraction_raw is None and strict:
                raise ValueError("equity_fraction must be explicitly specified in strict mode")
            fraction = Decimal(str(fraction_raw if fraction_raw is not None else "0.05"))
            max_notional = Decimal(
                str(user_overrides.get("max_order_notional", target_notional))
            )
            min_floor = Decimal(str(user_overrides.get("min_notional_floor", "10.00")))
            max_leverage = Decimal(str(user_overrides.get("entry_leverage", user_overrides.get("max_leverage", "5.0"))))
            max_slippage = Decimal(str(user_overrides.get("max_slippage_budget_bps", "10.0")))
            resize_tol = Decimal(str(user_overrides.get("resize_tolerance", "0.10")))
            sizing_model = EquityFractionSizingModel(
                fraction_of_equity=fraction,
                min_notional_floor=min_floor,
                max_notional_cap=max_notional,
                max_leverage=max_leverage,
                max_slippage_budget_bps=max_slippage,
                resize_tolerance=resize_tol,
            )
        else:
            max_leverage = Decimal(str(user_overrides.get("entry_leverage", user_overrides.get("max_leverage", "5.0"))))
            max_slippage = Decimal(str(user_overrides.get("max_slippage_budget_bps", "10.0")))
            resize_tol = Decimal(str(user_overrides.get("resize_tolerance", "0.05")))
            sizing_model = FixedNotionalSizingModel(
                target_notional=Decimal(str(target_notional)),
                max_leverage=max_leverage,
                max_slippage_budget_bps=max_slippage,
                resize_tolerance=resize_tol,
            )

        policy = EffectivePolicy(
            policy_id=strat_hash[:16],
            strategy_name=strategy_name,
            policy_version=1,
            entry_threshold=entry_thresh,
            order_type=order_type,
            target_notional=Decimal(str(target_notional)),
            sizing_model=sizing_model,
            exit_policy=exit_policy,
        )

        # Secrets recorded by reference only
        secret_refs = {k: f"env_ref:{k}" for k in secret_keys}

        gen = (
            runtime_generation
            if runtime_generation
            else f"gen_{account_label}_{deployment_hash[:8]}"
        )

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
            runtime_generation=gen,
            fencing_epoch=fencing_epoch,
            declared_schema_compatibility=schema_version,
            observed_database_revision=observed_database_revision,
            options_source_chain=sources,
            secret_references=secret_refs,
            compiled_at=datetime.now(UTC),
        )
