"""Live execution shadow auditor.

Encapsulates shadow comparison and auditing for live execution:
- Position ledger projection auditing
- Exit allocation auditing
- Trade command execution planning auditing

Tracks audit revision, execution success/failure state, and divergence categories.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import structlog

from crypto_momentum_lab.domain.execution import OrderExecutionPlan
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger import (
    PositionLedgerProjection,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    PositionEpisode,
    PositionKey,
    PositionLedgerBatch,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    ExitAllocator,
    ExitPolicyMode,
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import OrderIntentCandidate, StrategySide
from crypto_momentum_lab.execution_account.orders.quantization import (
    QuantizationRejection,
)
from crypto_momentum_lab.execution_account.orders.trade_command_executor import (
    TradeCommandExecutor,
)

if TYPE_CHECKING:
    from crypto_momentum_lab.execution_account.orders.quantization import (
        SymbolTradingRules,
    )
    from crypto_momentum_lab.live_rollout.exits import ManagedLivePosition

log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class ShadowAuditResult:
    """Outcome of a shadow execution audit."""

    audit_type: str
    revision: int
    success: bool
    is_concordant: bool
    divergence_category: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    shadow_plan: OrderExecutionPlan | None = None
    shadow_command: TradeCommand | None = None


class LiveExecutionShadowAuditor:
    """Auditor port that runs shadow domain models against legacy live paths."""

    _revision: int = 0
    _failure_count: int = 0
    _divergence_count: int = 0

    @classmethod
    def next_revision(cls) -> int:
        cls._revision += 1
        return cls._revision

    @classmethod
    def get_stats(cls) -> dict[str, int]:
        return {
            "revision": cls._revision,
            "failure_count": cls._failure_count,
            "divergence_count": cls._divergence_count,
        }

    @classmethod
    def audit_submission(
        cls,
        *,
        candidate: OrderIntentCandidate,
        rules: SymbolTradingRules,
        reference_price: Decimal,
        legacy_plan: OrderExecutionPlan | QuantizationRejection,
        run_id: str,
        hedge_mode: bool,
        requested_quantity: Decimal | None = None,
    ) -> ShadowAuditResult:
        """Audit trade submission against TradeCommandExecutor in shadow mode."""
        rev = cls.next_revision()
        try:
            raw_position_side = candidate.features.get("position_side")
            if isinstance(raw_position_side, str) and raw_position_side.strip():
                position_side = FuturesPositionSide(raw_position_side.strip().upper())
            elif hedge_mode:
                position_side = (
                    FuturesPositionSide.LONG
                    if candidate.side is StrategySide.LONG
                    else FuturesPositionSide.SHORT
                )
            else:
                position_side = FuturesPositionSide.BOTH

            account_label = getattr(candidate, "account_label", None) or "primary"
            position_key = PositionKey(
                environment="live",
                account_label=account_label,
                symbol=candidate.symbol,
                position_side=position_side,
            )

            sizing_price = (
                candidate.limit_price
                if candidate.limit_price is not None and candidate.limit_price > 0
                else reference_price
            )
            if requested_quantity is None:
                if candidate.desired_notional is None or sizing_price <= 0:
                    return ShadowAuditResult(
                        audit_type="submission",
                        revision=rev,
                        success=True,
                        is_concordant=True,
                    )
                req_qty = candidate.desired_notional / sizing_price
            else:
                req_qty = requested_quantity

            raw_idempotency = candidate.features.get("idempotency_key")
            idempotency_key = (
                str(raw_idempotency)
                if isinstance(raw_idempotency, str) and raw_idempotency.strip()
                else None
            )

            cmd = TradeCommand(
                command_id=candidate.candidate_id,
                position_key=position_key,
                command_type=(
                    TradeCommandType.EXIT
                    if candidate.reduce_only
                    else TradeCommandType.ENTRY
                ),
                side=candidate.side,
                order_type=candidate.entry_type,
                requested_quantity=req_qty,
                limit_price=candidate.limit_price,
                reduce_only=candidate.reduce_only,
                idempotency_key=idempotency_key,
                created_at=candidate.created_at,
            )
            shadow_result = TradeCommandExecutor.plan_execution(
                cmd,
                rules,
                run_id=run_id,
                reference_price=reference_price,
                hedge_mode=hedge_mode,
            )

            is_concordant = True
            category: str | None = None
            details: dict[str, Any] = {}

            if isinstance(legacy_plan, QuantizationRejection):
                if shadow_result.plan is not None:
                    is_concordant = False
                    category = "rejection_mismatch"
                    details = {
                        "legacy_status": "rejected",
                        "legacy_reason": legacy_plan.reason,
                        "shadow_status": "planned",
                    }
            elif shadow_result.plan is None:
                is_concordant = False
                category = "rejection_mismatch"
                details = {
                    "legacy_status": "planned",
                    "shadow_status": "rejected",
                    "shadow_reason": (
                        shadow_result.rejection.reason
                        if shadow_result.rejection
                        else "unknown"
                    ),
                }
            else:
                if (
                    shadow_result.plan.quantity != legacy_plan.quantity
                    or shadow_result.plan.side != legacy_plan.side
                ):
                    is_concordant = False
                    category = "attribute_mismatch"
                    details = {
                        "legacy_quantity": str(legacy_plan.quantity),
                        "shadow_quantity": str(shadow_result.plan.quantity),
                        "legacy_side": legacy_plan.side.value,
                        "shadow_side": shadow_result.plan.side.value,
                    }

            if not is_concordant:
                cls._divergence_count += 1
                log.info(
                    "shadow_execution_divergence",
                    audit_type="submission",
                    revision=rev,
                    category=category,
                    candidate_id=candidate.candidate_id,
                    symbol=candidate.symbol,
                    **details,
                )

            return ShadowAuditResult(
                audit_type="submission",
                revision=rev,
                success=True,
                is_concordant=is_concordant,
                divergence_category=category,
                details=details,
                shadow_plan=shadow_result.plan,
            )
        except Exception as exc:
            cls._failure_count += 1
            log.warning(
                "shadow_execution_audit_failed",
                audit_type="submission",
                revision=rev,
                candidate_id=candidate.candidate_id,
                symbol=candidate.symbol,
                error=str(exc),
            )
            return ShadowAuditResult(
                audit_type="submission",
                revision=rev,
                success=False,
                is_concordant=False,
                error=str(exc),
            )

    @classmethod
    def audit_exit_allocation(
        cls,
        *,
        position: ManagedLivePosition,
        order_quantity: Decimal,
        reference_price: Decimal,
        reason: str,
        hedge_mode: bool = False,
    ) -> ShadowAuditResult:
        """Audit exit order creation against ExitAllocator in shadow mode."""
        rev = cls.next_revision()
        try:
            pos_side = getattr(position, "position_side", None)
            if isinstance(pos_side, FuturesPositionSide):
                position_side = pos_side
            elif isinstance(pos_side, str) and pos_side.strip():
                position_side = FuturesPositionSide(pos_side.strip().upper())
            elif hedge_mode:
                position_side = (
                    FuturesPositionSide.LONG
                    if position.side is StrategySide.LONG
                    else FuturesPositionSide.SHORT
                )
            else:
                position_side = FuturesPositionSide.BOTH

            position_key = PositionKey(
                environment="live",
                account_label=getattr(position, "account_label", "primary"),
                symbol=position.symbol,
                position_side=position_side,
            )

            batches: tuple[PositionLedgerBatch, ...] = (
                tuple(
                    PositionLedgerBatch(
                        batch_id=b.batch_id or f"b_{idx}",
                        episode_id="shadow_ep",
                        quantity=b.quantity,
                        original_quantity=b.quantity,
                        entry_price=b.entry_price,
                        opened_at=b.opened_at,
                    )
                    for idx, b in enumerate(position.batches)
                )
                if position.batches
                else (
                    PositionLedgerBatch(
                        batch_id=position.batch_id or "batch_default",
                        episode_id="shadow_ep",
                        quantity=order_quantity,
                        original_quantity=order_quantity,
                        entry_price=position.entry_price,
                        opened_at=position.opened_at,
                    ),
                )
            )

            episode = PositionEpisode(
                episode_id="shadow_ep",
                position_key=position_key,
                side=position.side,
                opened_at=position.opened_at,
                batches=batches,
            )
            projection = PositionLedgerProjection(
                position_key=position_key,
                active_episode=episode,
                active_batches=batches,
                total_active_quantity=sum(
                    (b.quantity for b in batches), start=Decimal("0")
                ),
                unallocated_quantity=Decimal("0"),
                reconciliation_gap=Decimal("0"),
                high_watermark_trade_at=position.opened_at,
            )
            shadow_cmd = ExitAllocator.create_exit_command(
                projection,
                requested_quantity=order_quantity,
                target_batch_ids=(position.batch_id,) if position.batch_id else None,
                policy=(
                    ExitPolicyMode.TARGET_BATCHES_ONLY
                    if position.batch_id
                    else ExitPolicyMode.FULL_POSITION_CLOSE
                ),
                reference_price=reference_price,
                reason=reason,
            )

            is_concordant = True
            category: str | None = None
            details: dict[str, Any] = {}

            if (
                shadow_cmd is not None
                and shadow_cmd.requested_quantity != order_quantity
            ):
                is_concordant = False
                category = "quantity_mismatch"
                details = {
                    "order_quantity": str(order_quantity),
                    "shadow_quantity": str(shadow_cmd.requested_quantity),
                }
                cls._divergence_count += 1
                log.info(
                    "shadow_exit_allocation_divergence",
                    audit_type="exit_allocation",
                    revision=rev,
                    symbol=position.symbol,
                    **details,
                )

            return ShadowAuditResult(
                audit_type="exit_allocation",
                revision=rev,
                success=True,
                is_concordant=is_concordant,
                divergence_category=category,
                details=details,
                shadow_command=shadow_cmd,
            )
        except Exception as exc:
            cls._failure_count += 1
            log.warning(
                "shadow_exit_allocation_failed",
                audit_type="exit_allocation",
                revision=rev,
                symbol=position.symbol,
                error=str(exc),
            )
            return ShadowAuditResult(
                audit_type="exit_allocation",
                revision=rev,
                success=False,
                is_concordant=False,
                error=str(exc),
            )
