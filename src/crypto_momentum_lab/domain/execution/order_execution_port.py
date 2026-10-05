"""Application-facing execution seam, independent of exchange adapters."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from crypto_momentum_lab.domain.execution.order_read_models import PersistedOrderReceipt
from crypto_momentum_lab.domain.execution.order_result import OrderExecutionResult
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderSnapshot,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.execution.order_submission import (
    OrderSubmissionPreparation,
    OrderSubmissionRepository,
    PreparedOrderSubmission,
)
from crypto_momentum_lab.domain.market.models import JsonValue


class OrderExecutionPort(Protocol):
    async def submit(
        self,
        plan: OrderExecutionPlan,
        *,
        prepared_submission: PreparedOrderSubmission,
    ) -> OrderExecutionResult: ...

    async def reconcile_order(
        self, plan: OrderExecutionPlan
    ) -> OrderExecutionResult: ...

    async def cancel_order(
        self, plan: OrderExecutionPlan
    ) -> OrderExecutionResult: ...

    async def apply_observed_snapshot(
        self,
        plan: OrderExecutionPlan,
        snapshot: ExchangeOrderSnapshot,
    ) -> OrderExecutionResult: ...

    async def mark_reconciliation_pending(
        self, plan: OrderExecutionPlan
    ) -> OrderExecutionResult: ...

    async def mark_absent_reconciled(
        self,
        plan: OrderExecutionPlan,
        *,
        details: dict[str, JsonValue],
    ) -> OrderExecutionResult: ...


class EntrySubmissionGate(Protocol):
    """The only execution control surface required by entry admission."""

    def block_entry_submissions(self) -> None: ...

    def unblock_entry_submissions(self) -> None: ...


class CoordinatedOrderExecutionPort(OrderExecutionPort, Protocol):
    """Execution seam with live submission serialization and preparation."""

    async def wait_for_entry_submissions_idle(self) -> None: ...

    async def observe_recovered_receipt(
        self, plan: OrderExecutionPlan, receipt: PersistedOrderReceipt
    ) -> None: ...

    def block_entry_submissions(self) -> None: ...

    def unblock_entry_submissions(self) -> None: ...

    def configure_submission(
        self,
        repository: OrderSubmissionRepository,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None: ...

    async def prepare_and_execute(
        self,
        plan: OrderExecutionPlan,
        *,
        preparation: OrderSubmissionPreparation,
    ) -> OrderExecutionResult | None: ...
