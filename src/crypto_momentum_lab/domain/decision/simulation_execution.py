"""SimulationExecution adapter driving AccountJournal with explicit FillModel.

Obeys Astra Architecture Blueprint 2026-09-25:
- Explicit versioned FillModel (slippage, fee, queue delay, funding rate);
- No magical 'hit price = fill at mid' shortcuts;
- Generates reproducible AccountFillEvent driving authoritative AccountJournal;
- Shared batch attribution and reconciliation contract across live and paper.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.account_journal import AccountJournal
from crypto_momentum_lab.domain.execution.execution_coordinator import (
    ExecutionCoordinator,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.market.revision_models import MarketEnvelope
from crypto_momentum_lab.domain.strategy.models import (
    OrderIntentCandidate,
    StrategySide,
)


@dataclass(frozen=True, slots=True)
class FillModel:
    """Explicit parameters governing order execution simulation."""

    model_version: str = "conservative_v1"
    slippage_bps: Decimal = Decimal("5.0")  # 5 bps slippage
    fee_rate: Decimal = Decimal("0.0005")  # 0.05% taker fee
    queue_delay_seconds: Decimal = Decimal("0.1")
    funding_rate_hourly: Decimal = Decimal("0.00001")

    def __post_init__(self) -> None:
        if not self.model_version.strip():
            raise ValueError("model_version must not be empty")
        if self.slippage_bps < Decimal("0"):
            raise ValueError("slippage_bps must not be negative")
        if self.fee_rate < Decimal("0"):
            raise ValueError("fee_rate must not be negative")

    def compute_executed_price(
        self, base_price: Decimal, side: str
    ) -> tuple[Decimal, Decimal]:
        """Calculates executed price with slippage.

        Returns (executed_price, slippage_amount_per_unit).
        """
        slip_mult = self.slippage_bps / Decimal("10000")
        if side.upper() in ("BUY", "LONG"):
            executed = base_price * (Decimal("1") + slip_mult)
            slippage_cost = executed - base_price
        else:
            executed = base_price * (Decimal("1") - slip_mult)
            slippage_cost = base_price - executed
        return executed, slippage_cost


@dataclass(frozen=True, slots=True)
class SimulatedFillResult:
    """Detailed result of simulated order execution."""

    fill_id: str
    order_id: str
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal
    fee: Decimal
    slippage_cost: Decimal
    filled_at: datetime
    fill_model_version: str
    realized_pnl: Decimal = Decimal("0.00")


class SimulationExecutionAdapter:
    """Simulates realistic exchange execution driving canonical AccountJournal."""

    def __init__(self, default_fill_model: FillModel | None = None) -> None:
        self._fill_model = default_fill_model or FillModel()

    def execute_entry(
        self,
        intent: OrderIntentCandidate,
        envelope: MarketEnvelope,
        journal: AccountJournal,
        *,
        fill_model: FillModel | None = None,
    ) -> SimulatedFillResult:
        """Executes an entry order intent into canonical AccountJournal."""
        model = fill_model or self._fill_model
        state = envelope.state
        base_price = state.last_ask_price or state.close_price
        if base_price is None or base_price <= Decimal("0"):
            raise ValueError(
                f"Cannot execute entry for {intent.symbol}: missing valid ask or close price"
            )

        exec_price, unit_slip = model.compute_executed_price(base_price, "BUY")
        notional = intent.desired_notional
        if notional is None or notional <= Decimal("0"):
            raise ValueError(
                f"Cannot execute entry for {intent.symbol}: desired_notional must be positive"
            )
        quantity = (notional / exec_price).quantize(Decimal("0.0001"))
        fee = quantity * exec_price * model.fee_rate
        slippage_cost = quantity * unit_slip

        fill_time = state.bucket_end
        seed = f"entry:{intent.candidate_id}:{envelope.ref.content_hash}:{fill_time.isoformat()}"
        trade_id = f"sim_tr_{hashlib.sha256(seed.encode()).hexdigest()[:12]}"
        order_id = f"sim_ord_{intent.candidate_id[-12:]}"

        fill_event = AccountFillEvent(
            environment=journal.position_key.environment,
            account_label=journal.position_key.account_label,
            symbol=intent.symbol,
            trade_id=trade_id,
            order_id=order_id,
            side="BUY" if intent.side == StrategySide.LONG else "SELL",
            price=exec_price,
            quantity=quantity,
            realized_pnl=Decimal("0.00"),
            fee=fee,
            fee_asset="USDT",
            trade_at=fill_time,
            raw_payload={"fill_model": model.model_version},
        )
        journal.append_fill(fill_event)

        # Update position snapshot
        journal.record_snapshot(
            AccountPositionSnapshot(
                environment=journal.position_key.environment,
                account_label=journal.position_key.account_label,
                symbol=intent.symbol,
                position_side="LONG" if intent.side == StrategySide.LONG else "SHORT",
                position_amt=quantity,
                entry_price=exec_price,
                mark_price=exec_price,
                unrealized_pnl=Decimal("0.00"),
                notional=quantity * exec_price,
                leverage=1,
                margin_type="cross",
                observed_at=fill_time,
                raw_payload={"fill_model": model.model_version},
            )
        )

        return SimulatedFillResult(
            fill_id=trade_id,
            order_id=order_id,
            symbol=intent.symbol,
            side="BUY",
            quantity=quantity,
            price=exec_price,
            fee=fee,
            slippage_cost=slippage_cost,
            filled_at=fill_time,
            fill_model_version=model.model_version,
            realized_pnl=Decimal("0.00"),
        )

    def execute_exit(
        self,
        command: TradeCommand,
        envelope: MarketEnvelope,
        journal: AccountJournal,
        coordinator: ExecutionCoordinator,
        *,
        reservation_id: str | None = None,
        fill_model: FillModel | None = None,
    ) -> SimulatedFillResult:
        """Executes an exit TradeCommand against reserved lots."""
        if command.command_type != TradeCommandType.EXIT:
            raise ValueError("execute_exit requires EXIT command_type")

        model = fill_model or self._fill_model
        state = envelope.state

        # Determine exit direction: exiting SHORT requires BUY; exiting LONG requires SELL
        pos_side_str = getattr(
            command.position_key.position_side, "value", str(command.position_key.position_side)
        ).upper()
        is_short = command.side in (StrategySide.SHORT, "SHORT") or pos_side_str.endswith("SHORT")
        exchange_side = "BUY" if is_short else "SELL"

        if is_short:
            base_price = state.last_ask_price or state.close_price
        else:
            base_price = state.last_bid_price or state.close_price

        if base_price is None or base_price <= Decimal("0"):
            raise ValueError(
                f"Cannot execute exit for {command.position_key.symbol}: missing valid price"
            )

        exec_price, unit_slip = model.compute_executed_price(base_price, exchange_side)
        quantity = command.requested_quantity
        fee = quantity * exec_price * model.fee_rate
        slippage_cost = quantity * unit_slip

        # Compute actual realized PnL from allocated batch entry prices
        if command.allocation_plan is not None and command.allocation_plan.allocations:
            if is_short:
                # Exiting SHORT (buy to cover): profit when entry_price > exec_price
                realized_pnl = sum(
                    (
                        (alloc.entry_price - exec_price) * alloc.allocated_quantity
                        for alloc in command.allocation_plan.allocations
                    ),
                    start=Decimal("0.00"),
                )
            else:
                # Exiting LONG (sell to close): profit when exec_price > entry_price
                realized_pnl = sum(
                    (
                        (exec_price - alloc.entry_price) * alloc.allocated_quantity
                        for alloc in command.allocation_plan.allocations
                    ),
                    start=Decimal("0.00"),
                )
        else:
            realized_pnl = Decimal("0.00")

        fill_time = state.bucket_end
        seed = (
            f"exit:{command.command_id}:{envelope.ref.content_hash}:{fill_time.isoformat()}"
        )
        trade_id = f"sim_tr_{hashlib.sha256(seed.encode()).hexdigest()[:12]}"
        order_id = f"sim_exit_{command.command_id[-12:]}"

        fill_event = AccountFillEvent(
            environment=journal.position_key.environment,
            account_label=journal.position_key.account_label,
            symbol=command.position_key.symbol,
            trade_id=trade_id,
            order_id=order_id,
            side=exchange_side,
            price=exec_price,
            quantity=quantity,
            realized_pnl=realized_pnl,
            fee=fee,
            fee_asset="USDT",
            trade_at=fill_time,
            raw_payload={"fill_model": model.model_version},
        )
        journal.append_fill(fill_event)

        # Update position snapshot to 0
        journal.record_snapshot(
            AccountPositionSnapshot(
                environment=journal.position_key.environment,
                account_label=journal.position_key.account_label,
                symbol=command.position_key.symbol,
                position_side=str(
                    command.position_key.position_side.value
                    if hasattr(command.position_key.position_side, "value")
                    else command.position_key.position_side
                ),
                position_amt=Decimal("0.00"),
                entry_price=Decimal("0.00"),
                mark_price=exec_price,
                unrealized_pnl=Decimal("0.00"),
                notional=Decimal("0.00"),
                leverage=1,
                margin_type="cross",
                observed_at=fill_time,
                raw_payload={"fill_model": model.model_version},
            )
        )

        # Reconcile in coordinator if reservation_id provided
        if reservation_id is not None:
            coordinator.reconcile_fill(
                reservation_id=reservation_id,
                filled_quantity=quantity,
            )

        return SimulatedFillResult(
            fill_id=trade_id,
            order_id=order_id,
            symbol=command.position_key.symbol,
            side=exchange_side,
            quantity=quantity,
            price=exec_price,
            fee=fee,
            slippage_cost=slippage_cost,
            filled_at=fill_time,
            fill_model_version=model.model_version,
            realized_pnl=realized_pnl,
        )
