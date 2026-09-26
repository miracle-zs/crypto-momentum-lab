"""Build FrozenDecisionInputs from the live daemon runtime context.

Never invents READY positions or cash. Missing or incomplete account facts
yield ``None`` so the decision filter can fail closed.
"""

from __future__ import annotations

from decimal import Decimal

from crypto_momentum_lab.domain.decision.decision_engine import (
    FrozenDecisionInputs,
    PolicyState,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    FactCoverageStatus,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionView,
    compose_fact_coverage,
)
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.risk import StrategyLiveState
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext


def _cash_balance(context: LiveDaemonRuntimeContext) -> Decimal | None:
    snapshot = context.account_snapshot
    if snapshot is None:
        return None
    usdt = [
        b.wallet_balance
        for b in snapshot.balances
        if getattr(b, "asset", "").upper() in {"USDT", "USDC", "BUSD"}
    ]
    if not usdt:
        return None
    total = sum(usdt, start=Decimal("0"))
    return total if total >= Decimal("0") else None


def _position_batches(
    context: LiveDaemonRuntimeContext,
    symbol: str,
) -> tuple[PositionLedgerBatch, ...]:
    batches: list[PositionLedgerBatch] = []
    for pos in context.managed_positions:
        if pos.symbol != symbol:
            continue
        if pos.batches:
            for b in pos.batches:
                batches.append(
                    PositionLedgerBatch(
                        batch_id=b.batch_id,
                        episode_id=f"ep_{symbol}_{pos.opened_at.date()}",
                        quantity=b.quantity,
                        original_quantity=b.quantity,
                        entry_price=b.entry_price,
                        opened_at=b.opened_at,
                    )
                )
        else:
            batches.append(
                PositionLedgerBatch(
                    batch_id=pos.batch_id or f"live_{symbol}_{pos.opened_at.date()}",
                    episode_id=f"ep_{symbol}_{pos.opened_at.date()}",
                    quantity=pos.quantity,
                    original_quantity=pos.quantity,
                    entry_price=pos.entry_price,
                    opened_at=pos.opened_at,
                )
            )
    return tuple(batches)


def frozen_decision_inputs_from_context(
    context: LiveDaemonRuntimeContext,
    state: MarketState15s,
    *,
    account_label: str,
    policy_state: PolicyState | None = None,
) -> FrozenDecisionInputs | None:
    """Derive frozen decision facts from real account context.

    Returns ``None`` when cash or account identity cannot be proven so the
    filter fails closed instead of trading on synthetic facts.
    """
    cash = _cash_balance(context)
    if cash is None:
        return None

    pos_key = PositionKey(
        environment="live",
        account_label=account_label,
        symbol=state.symbol,
        position_side=FuturesPositionSide.BOTH,
    )
    batches = _position_batches(context, state.symbol)

    pending_or_unmanaged = state.symbol in (
        getattr(context, "pending_position_symbols", frozenset())
        | getattr(context, "unmanaged_position_symbols", frozenset())
    )
    evidence = getattr(context, "coverage_by_symbol", {}).get(state.symbol)
    coverage = (
        compose_fact_coverage(
            evidence,
            start=state.bucket_start,
            end=state.bucket_end,
        )
        if evidence is not None
        else None
    )
    if context.strategy_state != StrategyLiveState.ACTIVE:
        health = PositionHealthStatus.CATCHING_UP
    elif context.active_halts or pending_or_unmanaged:
        health = PositionHealthStatus.INCOMPLETE
    elif coverage is None or coverage.status != FactCoverageStatus.CONFIRMED:
        # Without proven coverage the position facts are not authoritative.
        health = PositionHealthStatus.CATCHING_UP
    else:
        health = PositionHealthStatus.READY

    projection_version = (
        f"pv_{account_label}_{state.symbol}_{context.account_snapshot_version}"
        if context.account_snapshot_version is not None
        else f"pv_{account_label}_{state.symbol}_unversioned"
    )
    risk_version = getattr(context.risk_config, "config_hash", None) or (
        f"risk_{context.risk_config.created_at.isoformat()}"
    )
    universe_version = (
        f"univ_{context.context_epoch}"
        if context.context_epoch is not None
        else "univ_live"
    )

    pos_view = PositionView(
        key=pos_key,
        projection_version=projection_version,
        input_revision=int(context.context_epoch or 0),
        event_cut=state.bucket_end,
        policy_version="live",
        schema_version="v1",
        coverage=coverage,
        active_episode=None,
        batches=batches,
        unallocated_quantity=Decimal("0"),
        reconciliation_gap=Decimal("0"),
        health_status=health,
    )
    return FrozenDecisionInputs(
        position_view=pos_view,
        cash_balance=cash,
        policy_state=policy_state or PolicyState(),
        universe_version=universe_version,
        risk_config_version=str(risk_version),
    )


class LiveDecisionFactSource:
    """Mutable holder the market loop updates before each filter call."""

    def __init__(self, account_label: str) -> None:
        self._account_label = account_label
        self._context: LiveDaemonRuntimeContext | None = None
        self._policy_state = PolicyState()

    def bind_context(self, context: LiveDaemonRuntimeContext | None) -> None:
        self._context = context

    def set_policy_state(self, state: PolicyState) -> None:
        self._policy_state = state

    def build(self, state: MarketState15s) -> FrozenDecisionInputs | None:
        if self._context is None:
            return None
        return frozen_decision_inputs_from_context(
            self._context,
            state,
            account_label=self._account_label,
            policy_state=self._policy_state,
        )
