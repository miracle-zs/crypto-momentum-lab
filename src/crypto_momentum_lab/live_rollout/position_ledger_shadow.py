"""Shadow projection comparator and legacy adapters for PositionLedger.

Operates strictly in read-only shadow mode:
1. Runs PositionLedger projection in parallel with legacy rebuild_position_batches;
2. Produces structured divergence diagnostics without mutating state;
3. Never affects live order execution or submission paths.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum

import structlog

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution import (
    ManagedLivePositionBatch,
    PositionObservation,
    PositionOrderFact,
)
from crypto_momentum_lab.domain.execution.position_batches import (
    _EXIT_SUBMITTED_STATES,
)
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    ExitOrderSubmissionFact,
    PositionKey,
    PositionLedgerProjection,
)

log = structlog.get_logger()


class ShadowDiffCategory(StrEnum):
    EXACT_MATCH = "exact_match"
    EXTERNAL_FILL_DETECTED = "external_fill_detected"
    BATCH_COUNT_MISMATCH = "batch_count_mismatch"
    QUANTITY_MISMATCH = "quantity_mismatch"
    ZERO_CROSSING_DIVERGENCE = "zero_crossing_divergence"
    LOT_ATTRIBUTION_MISMATCH = "lot_attribution_mismatch"


@dataclass(frozen=True, slots=True)
class ShadowDiffReport:
    """Audit report comparing legacy batch rebuild with PositionLedger v2."""

    position_key: PositionKey
    category: ShadowDiffCategory
    legacy_batch_count: int
    ledger_batch_count: int
    legacy_total_quantity: Decimal
    ledger_total_quantity: Decimal
    oldest_batch_age_diff_seconds: float
    details: str

    @property
    def is_concordant(self) -> bool:
        return self.category is ShadowDiffCategory.EXACT_MATCH


class LegacyOrderIdentityAdapter:
    """Adapts legacy execution order facts and observations into AccountFacts."""

    @staticmethod
    def to_account_facts(
        *,
        position_key: PositionKey,
        orders: Sequence[PositionOrderFact],
        fills: Sequence[AccountFillEvent] = (),
        observation: PositionObservation | None = None,
    ) -> AccountFacts:
        """Construct normalized AccountFacts from legacy inputs.

        If explicit fills are not available, synthetic fills are synthesized
        from filled PositionOrderFact entries to allow backward compatibility.
        """

        def _fill_matches_side(fill: AccountFillEvent) -> bool:
            if fill.symbol != position_key.symbol:
                return False
            payload = fill.raw_payload or {}
            ps = payload.get("positionSide") or payload.get("ps")
            if ps:
                ps_str = str(ps).upper()
                if (
                    ps_str != "BOTH"
                    and ps_str != position_key.position_side.value.upper()
                ):
                    return False
            return True

        fill_list: list[AccountFillEvent] = [f for f in fills if _fill_matches_side(f)]

        if not fill_list and orders:
            for ord_idx, order in enumerate(orders):
                if order.executed_quantity > 0:
                    oid = (
                        order.exchange_order_id
                        or order.client_order_id
                        or "unknown"
                    )
                    fill_list.append(
                        AccountFillEvent(
                            environment=position_key.environment,
                            account_label=position_key.account_label,
                            symbol=position_key.symbol,
                            trade_id=f"syn_t_{ord_idx}_{oid}",
                            order_id=oid if oid != "unknown" else f"ord_{ord_idx}",
                            side=order.side,
                            price=order.price or Decimal("1.0"),
                            quantity=order.executed_quantity,
                            realized_pnl=Decimal("0.0"),
                            fee=Decimal("0.0"),
                            fee_asset="USDT",
                            trade_at=order.created_at,
                            raw_payload={
                                "synthetic_from_order": True,
                                "is_system": True,
                            },
                        )
                    )

        exit_boundaries: list[ExitOrderSubmissionFact] = []
        for order in orders:
            if order.reduce_only and order.state in _EXIT_SUBMITTED_STATES:
                exit_boundaries.append(
                    ExitOrderSubmissionFact(
                        order_id=order.exchange_order_id
                        or order.client_order_id
                        or f"exit_{order.created_at.timestamp()}",
                        submitted_at=order.created_at,
                        symbol=position_key.symbol,
                        position_side=position_key.position_side,
                        client_order_id=order.client_order_id,
                        target_batch_id=order.exit_batch_id,
                    )
                )

        snapshots: list[AccountPositionSnapshot] = []
        if observation is not None:
            now_dt = datetime.now(UTC)
            obs_time = orders[-1].updated_at if orders else now_dt
            snapshots.append(
                AccountPositionSnapshot(
                    environment=position_key.environment,
                    account_label=position_key.account_label,
                    symbol=position_key.symbol,
                    position_side=position_key.position_side.value,
                    position_amt=observation.position_amt,
                    entry_price=observation.entry_price,
                    mark_price=observation.entry_price,
                    unrealized_pnl=Decimal("0.0"),
                    notional=observation.position_amt * observation.entry_price,
                    leverage=None,
                    margin_type=None,
                    observed_at=obs_time,
                    raw_payload={},
                )
            )

        return AccountFacts(
            position_key=position_key,
            fills=tuple(fill_list),
            snapshots=tuple(snapshots),
            exit_boundaries=tuple(exit_boundaries),
        )


class PositionLedgerShadowComparator:
    """Compares legacy batch reconstruction against PositionLedger projection."""

    @staticmethod
    def compare(
        *,
        position_key: PositionKey,
        legacy_batches: Sequence[ManagedLivePositionBatch],
        ledger_projection: PositionLedgerProjection,
    ) -> ShadowDiffReport:
        """Compare legacy batches with ledger batches and return audit report."""
        legacy_count = len(legacy_batches)
        ledger_count = len(ledger_projection.active_batches)

        legacy_total_qty = sum(
            (b.quantity for b in legacy_batches),
            start=Decimal("0"),
        )
        ledger_total_qty = ledger_projection.total_active_quantity

        # Oldest batch timestamp diff
        age_diff_sec = 0.0
        if legacy_batches and ledger_projection.active_batches:
            legacy_oldest = min(b.opened_at for b in legacy_batches)
            ledger_oldest = min(b.opened_at for b in ledger_projection.active_batches)
            age_diff_sec = abs((legacy_oldest - ledger_oldest).total_seconds())

        has_external_reduction = any(
            bool(ep.reductions and any(not r.is_system for r in ep.reductions))
            for ep in (ledger_projection.active_episode,)
            if ep is not None
        )

        category = ShadowDiffCategory.EXACT_MATCH
        details = "Exact match between legacy rebuild and PositionLedger v2"

        if legacy_total_qty != ledger_total_qty:
            category = ShadowDiffCategory.QUANTITY_MISMATCH
            details = (
                f"Total quantity mismatch: legacy={legacy_total_qty}, "
                f"ledger={ledger_total_qty}"
            )
        elif legacy_count != ledger_count:
            if has_external_reduction:
                category = ShadowDiffCategory.EXTERNAL_FILL_DETECTED
                details = (
                    f"External fill presence caused lot attribution divergence: "
                    f"legacy_count={legacy_count}, ledger_count={ledger_count}"
                )
            else:
                category = ShadowDiffCategory.BATCH_COUNT_MISMATCH
                details = (
                    f"Batch count mismatch: legacy={legacy_count}, "
                    f"ledger={ledger_count}"
                )
        elif age_diff_sec > 1.0:
            category = ShadowDiffCategory.ZERO_CROSSING_DIVERGENCE
            details = (
                f"Oldest batch timestamp diverged by {age_diff_sec:.3f}s "
                f"(indicates different zero-crossing or lookback anchor)"
            )
        else:
            sorted_legacy = sorted(
                legacy_batches, key=lambda b: (b.opened_at, str(b.batch_id))
            )
            sorted_ledger = sorted(
                ledger_projection.active_batches,
                key=lambda b: (b.opened_at, str(b.batch_id)),
            )
            for idx, (leg_b, led_b) in enumerate(
                zip(sorted_legacy, sorted_ledger, strict=True)
            ):
                if leg_b.quantity != led_b.quantity:
                    category = ShadowDiffCategory.LOT_ATTRIBUTION_MISMATCH
                    details = (
                        f"Batch quantity mismatch at index {idx}: "
                        f"legacy={leg_b.quantity}, ledger={led_b.quantity}"
                    )
                    break
                if leg_b.entry_price != led_b.entry_price:
                    category = ShadowDiffCategory.LOT_ATTRIBUTION_MISMATCH
                    details = (
                        f"Batch entry_price mismatch at index {idx}: "
                        f"legacy={leg_b.entry_price}, ledger={led_b.entry_price}"
                    )
                    break
                batch_age_diff = abs(
                    (leg_b.opened_at - led_b.opened_at).total_seconds()
                )
                if batch_age_diff > 1.0:
                    category = ShadowDiffCategory.LOT_ATTRIBUTION_MISMATCH
                    details = (
                        f"Batch opened_at mismatch at index {idx}: "
                        f"diff={batch_age_diff:.3f}s (> 1.0s)"
                    )
                    break
                leg_exit_sub = leg_b.exit_order_submitted_at
                led_exit_sub = led_b.exit_order_submitted_at
                if (leg_exit_sub is None) != (led_exit_sub is None):
                    category = ShadowDiffCategory.LOT_ATTRIBUTION_MISMATCH
                    details = (
                        f"Batch exit_order_submitted_at existence mismatch "
                        f"at index {idx}: legacy={leg_exit_sub}, ledger={led_exit_sub}"
                    )
                    break
                if leg_exit_sub is not None and led_exit_sub is not None:
                    if abs((leg_exit_sub - led_exit_sub).total_seconds()) > 1.0:
                        category = ShadowDiffCategory.LOT_ATTRIBUTION_MISMATCH
                        details = (
                            f"Batch exit_order_submitted_at timestamp mismatch "
                            f"at index {idx}: legacy={leg_exit_sub}, "
                            f"ledger={led_exit_sub}"
                        )
                        break

        report = ShadowDiffReport(
            position_key=position_key,
            category=category,
            legacy_batch_count=legacy_count,
            ledger_batch_count=ledger_count,
            legacy_total_quantity=legacy_total_qty,
            ledger_total_quantity=ledger_total_qty,
            oldest_batch_age_diff_seconds=age_diff_sec,
            details=details,
        )

        log.info(
            "position_ledger_shadow_comparison",
            symbol=position_key.symbol,
            category=category.value,
            concordant=report.is_concordant,
            legacy_count=legacy_count,
            ledger_count=ledger_count,
            details=details,
        )

        return report
