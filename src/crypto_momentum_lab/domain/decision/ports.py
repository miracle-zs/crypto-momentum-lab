"""Persistence seam for live decisions; implementations own atomic commits.

commit_decision atomically saves policy state, trace/dependencies and exit
outbox. Revision/digest conflicts fail without publishing a partial result;
replays return their original receipt. Exit acknowledgements are idempotent
and may only advance a matching command's pending row. Superseding requires
caller-verified exchange absence and proof that the exit is obsolete.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, Protocol

from crypto_momentum_lab.domain.decision.commit_models import (
    DecisionCommit,
    DecisionCommitReceipt,
    DurablePolicySnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import ExchangeOrderState
from crypto_momentum_lab.domain.execution.trade_command import TradeCommand


class DecisionUnitOfWorkPort(Protocol):
    async def load_or_import_policy_state(
        self,
        policy_key: str,
        strategy_name: str,
        account_label: str,
    ) -> DurablePolicySnapshot | None: ...

    async def commit_decision(
        self, commit: DecisionCommit
    ) -> DecisionCommitReceipt: ...

    async def load_pending_exits(
        self,
        policy_key: str | None = None,
    ) -> tuple[tuple[str, TradeCommand], ...]: ...

    async def mark_exit_superseded(
        self,
        decision_id: str,
        command_id: str,
        reason: str,
    ) -> bool: ...

    async def mark_exit_dispatched(self, decision_id: str, command_id: str) -> bool: ...


class ExitDispatchReceipt(Protocol):
    @property
    def state(self) -> ExchangeOrderState | str: ...


ExitDispatchHandler = Callable[[TradeCommand], Awaitable[ExitDispatchReceipt]]


@dataclass(frozen=True, slots=True)
class ExitRecoveryDisposition:
    status: Literal["PENDING", "DISPATCHED", "SUPERSEDED"]
    reason: str

    def __post_init__(self) -> None:
        if (
            self.status not in {"PENDING", "DISPATCHED", "SUPERSEDED"}
            or not self.reason.strip()
        ):
            raise ValueError(
                "exit recovery needs a known disposition and evidence reason"
            )


ExitRecoveryHandler = Callable[[TradeCommand], Awaitable[ExitRecoveryDisposition]]
