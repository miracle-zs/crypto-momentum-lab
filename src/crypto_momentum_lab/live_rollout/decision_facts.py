"""Live decision inputs and durable commit coordination.

Position facts are read directly from the restored ExecutionBook. Context
supplies only cash, risk, and operational posture; it never reconstructs lots.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import structlog

from crypto_momentum_lab.domain.account import ExecutionAccountStatus
from crypto_momentum_lab.domain.decision.decision_engine import (
    DecisionInput,
    DecisionResult,
    FrozenDecisionInputs,
    PolicyState,
)
from crypto_momentum_lab.domain.decision.policy_transition import (
    compute_policy_state_digest,
)
from crypto_momentum_lab.domain.execution.execution_book import (
    ExecutionBook,
    ExecutionScope,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionView,
)
from crypto_momentum_lab.domain.execution.trade_command import TradeCommand
from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import DecisionTrace
from crypto_momentum_lab.domain.operational.retention_models import (
    ConsumerDependency,
    RecoverySpec,
)
from crypto_momentum_lab.domain.risk import StrategyLiveState
from crypto_momentum_lab.domain.strategy import StrategySide
from crypto_momentum_lab.live_rollout.context import LiveDaemonRuntimeContext
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work import (
    AsyncPostgresDecisionUnitOfWork,
    DecisionCommit,
    DecisionCommitReceipt,
)

log = structlog.get_logger(__name__)


def _cash_balance(context: LiveDaemonRuntimeContext) -> Decimal | None:
    snapshot = context.account_snapshot
    if snapshot is None:
        return None
    balances = [
        balance.wallet_balance
        for balance in snapshot.balances
        if getattr(balance, "asset", "").upper() in {"USDT", "USDC", "BUSD"}
    ]
    if not balances:
        return None
    total = sum(balances, start=Decimal("0"))
    return total if total >= 0 else None


def frozen_decision_inputs_from_context(
    context: LiveDaemonRuntimeContext,
    state: MarketState15s,
    *,
    account_label: str,
    position_view: PositionView | None = None,
    policy_state: PolicyState | None = None,
) -> FrozenDecisionInputs | None:
    """Combine operational context with one already-read Book view.

    No position, coverage, or zero-balance inference is performed here. A
    caller that does not supply an authoritative Book view cannot evaluate a
    live decision.
    """
    if position_view is None:
        return None
    if (
        position_view.key.environment != "live"
        or position_view.key.account_label != account_label
        or position_view.key.symbol != state.symbol
    ):
        return None
    if not position_view.is_ready_for_trade:
        return None
    cash = _cash_balance(context)
    if cash is None:
        return None
    if context.account_state != ExecutionAccountStatus.READY_READONLY:
        return None
    if context.strategy_state != StrategyLiveState.ACTIVE:
        return None
    if context.active_halts:
        return None
    if state.symbol in (
        context.pending_position_symbols | context.unmanaged_position_symbols
    ):
        return None
    risk_config = context.risk_config
    risk_version = getattr(risk_config, "config_hash", None) or (
        f"risk_{risk_config.created_at.isoformat()}"
    )
    universe_version = (
        f"univ_{context.context_epoch}"
        if context.context_epoch is not None
        else "univ_live"
    )
    return FrozenDecisionInputs(
        position_view=position_view,
        cash_balance=cash,
        policy_state=policy_state or PolicyState(),
        universe_version=universe_version,
        risk_config_version=str(risk_version),
    )


class LiveDecisionFactSource:
    """Live authority for Book reads and synchronous durable decisions."""

    _ACCOUNT_EVENT_STREAM_ID = "account_event_hub"

    def __init__(
        self,
        account_label: str,
        trace_repository: Any | None = None,
        strategy_name: str = "orderflow_impulse",
        retention_authority: Any | None = None,
        *,
        execution_book: ExecutionBook | None = None,
        decision_unit_of_work: AsyncPostgresDecisionUnitOfWork | None = None,
        hedge_mode: bool = True,
    ) -> None:
        # Legacy persistence arguments remain accepted for callers migrating
        # to the UoW, but live commits never use their background APIs.
        del trace_repository, retention_authority
        if not account_label.strip() or not strategy_name.strip():
            raise ValueError("account and strategy identity must not be empty")
        self._account_label = account_label
        self._strategy_name = strategy_name
        self._policy_key = f"live/{account_label}/{strategy_name}"
        self._execution_book = execution_book
        self._decision_uow = decision_unit_of_work
        self._hedge_mode = hedge_mode
        self._context: LiveDaemonRuntimeContext | None = None
        self._policy_state = PolicyState()
        self._policy_revision = 0
        self._policy_digest = compute_policy_state_digest(self._policy_state)
        self._stream_id: str | None = None
        self._stream_epoch: str | None = None
        self._stream_sequence: int | None = None
        self._reported_stream_mismatches: set[tuple[str, str, str]] = set()
        self._exit_handler: Any | None = None
        self._commit_lock = asyncio.Lock()

    @property
    def policy_key(self) -> str:
        return self._policy_key

    @property
    def policy_revision(self) -> int:
        return self._policy_revision

    @property
    def current_context(self) -> LiveDaemonRuntimeContext | None:
        return self._context

    @property
    def current_policy_state(self) -> PolicyState:
        return self._policy_state

    def set_execution_book(self, execution_book: ExecutionBook) -> None:
        if execution_book is None:
            raise ValueError("execution_book is required")
        self._execution_book = execution_book

    def set_exit_handler(self, handler: Any | None) -> None:
        self._exit_handler = handler

    def bind_context(self, context: LiveDaemonRuntimeContext | None) -> None:
        self._context = context

    def bind_account_stream(
        self,
        *,
        stream_id: str,
        stream_epoch: str,
        sequence: int,
    ) -> None:
        if not stream_id.strip() or not stream_epoch.strip() or sequence <= 0:
            raise ValueError("account stream identity must be complete and positive")
        if self._stream_epoch == stream_epoch and (
            self._stream_sequence is not None and sequence < self._stream_sequence
        ):
            raise ValueError("account event sequence regressed")
        self._stream_id = stream_id
        self._stream_epoch = stream_epoch
        self._stream_sequence = sequence

    async def restore(self) -> None:
        """Restore the newest durable policy head before decision admission."""
        if self._decision_uow is None:
            raise RuntimeError("live decision persistence UoW is required")
        snapshot = await self._decision_uow.load_or_import_policy_state(
            self._policy_key,
            strategy_name=self._strategy_name,
            account_label=self._account_label,
        )
        if snapshot is None:
            default_state = PolicyState()
            self._policy_state = default_state
            self._policy_revision = 0
            self._policy_digest = compute_policy_state_digest(default_state)
        else:
            self._policy_state = snapshot.state
            self._policy_revision = snapshot.revision
            self._policy_digest = snapshot.state_digest
        log.info(
            "durable_policy_state_restored",
            policy_key=self._policy_key,
            policy_revision=self._policy_revision,
        )

    async def build(
        self,
        state: MarketState15s,
        candidate_side: StrategySide | None = None,
    ) -> FrozenDecisionInputs | None:
        """Read the exact Book view used by decision and exit allocation."""
        context = self._context
        book = self._execution_book
        if (
            context is None
            or book is None
            or self._stream_id is None
            or self._stream_epoch is None
            or self._stream_sequence is None
        ):
            return None

        if self._hedge_mode:
            if candidate_side is not None:
                position_side = (
                    FuturesPositionSide.LONG
                    if candidate_side == StrategySide.LONG
                    else FuturesPositionSide.SHORT
                )
            else:
                views = await book.list_position_views(
                    environment="live",
                    account_label=self._account_label,
                    event_cut=state.bucket_end,
                    stream_id=self._stream_id,
                    stream_epoch=self._stream_epoch,
                )
                active_views = tuple(
                    view
                    for view in views
                    if view.key.symbol == state.symbol
                    and (
                        view.total_quantity > 0
                        or view.unallocated_quantity > 0
                    )
                )
                if len(active_views) != 1:
                    return None
                position_side = active_views[0].key.position_side
        else:
            position_side = FuturesPositionSide.BOTH

        scope = ExecutionScope(
            environment="live",
            account_label=self._account_label,
            symbol=state.symbol,
            position_side=position_side,
        )
        try:
            view = await book.read(
                scope,
                event_cut=state.bucket_end,
                stream_id=self._stream_id,
                stream_epoch=self._stream_epoch,
            )
        except ValueError as error:
            if str(error) != (
                "requested account stream does not match the restored position"
            ):
                raise
            mismatch = (state.symbol, self._stream_id, self._stream_epoch)
            if mismatch not in self._reported_stream_mismatches:
                self._reported_stream_mismatches.add(mismatch)
                log.warning(
                    "live_decision_position_stream_mismatch",
                    account_label=self._account_label,
                    symbol=state.symbol,
                    stream_epoch=self._stream_epoch,
                )
            return None
        expected_stream = AccountFactStreamScope.for_position_key(
            scope.to_position_key(),
            stream_id=self._stream_id,
            stream_epoch=self._stream_epoch,
        )
        if view.stream_scope != expected_stream:
            return None
        return frozen_decision_inputs_from_context(
            context,
            state,
            account_label=self._account_label,
            position_view=view,
            policy_state=self._policy_state,
        )

    async def commit_decision(
        self,
        trace: DecisionTrace,
        result: DecisionResult,
        decision_input: DecisionInput,
    ) -> DecisionCommitReceipt:
        """Await durable trace/policy/exit commit before any effect is released."""
        if self._decision_uow is None:
            raise RuntimeError("live decision persistence UoW is required")
        if trace.account_label != self._account_label:
            raise ValueError("decision trace account does not match live source")
        async with self._commit_lock:
            prior_state = self._policy_state
            prior_revision = self._policy_revision
            prior_digest = compute_policy_state_digest(prior_state)
            if prior_digest != self._policy_digest:
                raise RuntimeError("in-memory durable policy head digest diverged")
            dependencies = _decision_dependencies(trace)
            commit = DecisionCommit(
                trace=trace,
                policy_key=self._policy_key,
                expected_policy_revision=prior_revision,
                expected_prior_digest=prior_digest,
                prior_policy_state=prior_state,
                next_policy_state=result.next_policy_state,
                dependencies=dependencies,
                accepted_exit=result.exit_command,
            )
            receipt = await self._decision_uow.commit_decision(commit)

            if getattr(receipt, "is_replay", False):
                # UoW returns the original durable receipt for an exact retry.
                # Its trace may still contain an entry candidate or an exit
                # command; neither may be released a second time here. Pending
                # exits are handled by the durable outbox recovery path.
                if receipt.policy_revision > self._policy_revision:
                    raise RuntimeError(
                        "replayed decision receipt is ahead of the durable policy head"
                    )
                if receipt.policy_revision == self._policy_revision and (
                    receipt.next_state_digest != self._policy_digest
                ):
                    raise RuntimeError(
                        "replayed decision receipt disagrees with the durable "
                        "policy head"
                    )
                return receipt
            if receipt.policy_revision <= self._policy_revision:
                raise RuntimeError(
                    "new decision receipt did not advance the durable policy head"
                )
            if receipt.policy_revision != prior_revision + 1:
                raise RuntimeError(
                    "decision commit receipt skipped the expected policy revision"
                )

            # Publish only after PostgreSQL's synchronous transaction returns.
            self._policy_state = result.next_policy_state
            self._policy_revision = receipt.policy_revision
            self._policy_digest = receipt.next_state_digest

            if result.exit_command is not None:
                await self._dispatch_exit(
                    receipt.decision_id,
                    result.exit_command,
                )
            del decision_input
            return receipt

    async def recover_pending_exits(self) -> None:
        if self._decision_uow is None:
            raise RuntimeError("live decision persistence UoW is required")
        if self._exit_handler is None:
            raise RuntimeError("live decision exit handler is not configured")
        for decision_id, command in await self._decision_uow.load_pending_exits(
            self._policy_key
        ):
            if not await self._exit_matches_current_book(command):
                log.warning(
                    "durable_decision_exit_deferred_until_book_ready",
                    decision_id=decision_id,
                    command_id=command.command_id,
                    projection_version=command.expected_projection_version,
                    stream_id=self._stream_id,
                    stream_epoch=self._stream_epoch,
                )
                continue
            try:
                await self._dispatch_exit(decision_id, command)
            except asyncio.CancelledError:
                raise
            except Exception:
                # The durable row remains PENDING until the coordinator gives
                # an accepted result and its acknowledgement is persisted.
                # This allows account/order reconciliation to resolve unknown
                # POST outcomes without reposting the command.
                log.exception(
                    "durable_decision_exit_dispatch_deferred",
                    decision_id=decision_id,
                    command_id=command.command_id,
                )

    async def _exit_matches_current_book(self, command: TradeCommand) -> bool:
        book = self._execution_book
        if (
            book is None
            or self._stream_id is None
            or self._stream_epoch is None
            or command.expected_projection_version is None
        ):
            return False
        key = command.position_key
        if key.environment != "live" or key.account_label != self._account_label:
            return False
        scope = ExecutionScope(
            environment=key.environment,
            account_label=key.account_label,
            symbol=key.symbol,
            position_side=key.position_side,
        )
        view = await book.read(
            scope,
            stream_id=self._stream_id,
            stream_epoch=self._stream_epoch,
        )
        expected_scope = AccountFactStreamScope.for_position_key(
            key,
            stream_id=self._stream_id,
            stream_epoch=self._stream_epoch,
        )
        return (
            view.stream_scope == expected_scope
            and view.is_ready_for_trade
            and view.projection_version == command.expected_projection_version
            and command.allocation_plan is not None
            and command.allocation_plan.projection_version
            == command.expected_projection_version
        )

    async def _dispatch_exit(
        self,
        decision_id: str,
        command: TradeCommand,
    ) -> None:
        handler = self._exit_handler
        if handler is None:
            raise RuntimeError("durable accepted exit has no dispatch handler")
        if not await self._exit_matches_current_book(command):
            raise RuntimeError(
                f"accepted exit {command.command_id} does not match the current "
                "ready Book projection; its durable outbox remains pending"
            )
        result = handler(command)
        if asyncio.iscoroutine(result):
            result = await result
        state = getattr(result, "state", None)
        state_value = getattr(state, "value", state)
        if state_value not in {
            "submitted",
            "acknowledged",
            "partially_filled",
            "filled",
        }:
            raise RuntimeError(
                f"accepted exit {command.command_id} was not durably accepted: "
                f"{state_value}"
            )
        assert self._decision_uow is not None
        marked = await self._decision_uow.mark_exit_dispatched(
            decision_id,
            command.command_id,
        )
        if not marked:
            raise RuntimeError(
                f"durable accepted exit {decision_id} disappeared before ack"
            )

    async def drain(self, timeout_seconds: float = 5.0) -> None:
        """Compatibility lifecycle hook; all decision commits are inline."""
        del timeout_seconds


def _decision_dependencies(trace: DecisionTrace) -> tuple[ConsumerDependency, ...]:
    if not trace.evaluated_market_refs:
        raise ValueError("durable live decision has no market references")
    earliest_bucket = min(ref.bucket_start for ref in trace.evaluated_market_refs)
    dependency = ConsumerDependency(
        consumer_id=f"live-policy:{trace.account_label}:{trace.strategy_name}",
        dataset_name="market_revisions",
        generation=1,
        recovery_spec=RecoverySpec(
            source_dataset="market_revisions",
            earliest_needed_watermark=earliest_bucket,
            earliest_checkpoint_id=trace.decision_id,
            cold_recovery_supported=True,
            reason="durable live decision input",
        ),
        dependency_version=trace.frame_digest,
        updated_at=trace.decision_time,
    )
    return (dependency,)


__all__ = [
    "LiveDecisionFactSource",
    "frozen_decision_inputs_from_context",
]
