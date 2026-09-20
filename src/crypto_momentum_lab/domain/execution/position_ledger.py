"""Authoritative position ledger domain service.

Derives position batches and lifecycle episodes strictly from immutable facts
(AccountFillEvent and AccountPositionSnapshot), obeying:
1. Exact quantity conservation;
2. Zero-crossing episode bounding (pre-zero batches never taint post-zero state);
3. Explicit FIFO attribution for lot reductions (both system and external);
4. Zero heuristics: no silent quantity clipping or artificial lookback boundaries.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    BatchReductionAttribution,
    ExternalReductionFact,
    PositionEpisode,
    PositionKey,
    PositionLedgerBatch,
    PositionLedgerProjection,
)
from crypto_momentum_lab.domain.strategy import StrategySide


class PositionLedger:
    """Pure domain service executing deterministic replay of account fills."""

    def __init__(
        self,
        position_key: PositionKey,
        *,
        system_order_ids: frozenset[str] = frozenset(),
    ) -> None:
        self._position_key = position_key
        self._system_order_ids = system_order_ids

    def project(self, facts: AccountFacts) -> PositionLedgerProjection:
        """Project the current ledger state by replaying all fills in AccountFacts."""
        if facts.position_key.canonical_id != self._position_key.canonical_id:
            raise ValueError(
                f"Facts position key {facts.position_key.canonical_id} does not match "
                f"ledger key {self._position_key.canonical_id}"
            )

        # 1. Deduplicate fills by trade_id and sort chronologically
        deduped_fills: dict[str, AccountFillEvent] = {}
        for fill in facts.fills:
            if fill.trade_id not in deduped_fills:
                deduped_fills[fill.trade_id] = fill

        sorted_fills = sorted(
            deduped_fills.values(),
            key=lambda f: (f.trade_at, f.trade_id),
        )

        active_episode: PositionEpisode | None = None
        archived_episodes: list[PositionEpisode] = []
        diagnostics: list[str] = []
        high_watermark: datetime | None = None
        episode_counter = 0

        # Ephemeral mutable state for active episode
        current_batches: list[PositionLedgerBatch] = []
        current_reductions: list[ExternalReductionFact] = []
        cum_bought = Decimal("0")
        cum_sold = Decimal("0")
        peak_qty = Decimal("0")
        batch_counter = 0

        for fill in sorted_fills:
            high_watermark = fill.trade_at
            is_system = (
                fill.order_id in self._system_order_ids
                or bool(fill.raw_payload.get("is_system", False))
            )
            fill_side = fill.side.upper()

            # If no active episode, this fill initiates a new episode
            if active_episode is None:
                episode_counter += 1
                side = (
                    StrategySide.LONG
                    if fill_side == "BUY"
                    else StrategySide.SHORT
                )
                ep_id = (
                    f"ep_{self._position_key.symbol}_"
                    f"{fill.trade_at.strftime('%Y%m%d%H%M%S')}_{episode_counter}"
                )
                active_episode = PositionEpisode(
                    episode_id=ep_id,
                    position_key=self._position_key,
                    side=side,
                    opened_at=fill.trade_at,
                    is_active=True,
                )
                current_batches = []
                current_reductions = []
                cum_bought = Decimal("0")
                cum_sold = Decimal("0")
                peak_qty = Decimal("0")
                batch_counter = 0

            # LONG Episode Processing
            if active_episode.side == StrategySide.LONG:
                if fill_side == "BUY":
                    # Entry / Scaling add
                    batch_counter += 1
                    batch_id = f"{active_episode.episode_id}_b{batch_counter}"
                    batch = PositionLedgerBatch(
                        batch_id=batch_id,
                        episode_id=active_episode.episode_id,
                        quantity=fill.quantity,
                        original_quantity=fill.quantity,
                        entry_price=fill.price,
                        opened_at=fill.trade_at,
                        order_id=fill.order_id,
                        client_order_id=None,
                        is_external=not is_system,
                    )
                    current_batches.append(batch)
                    cum_bought += fill.quantity
                    current_net = cum_bought - cum_sold
                    if current_net > peak_qty:
                        peak_qty = current_net

                elif fill_side == "SELL":
                    # Exit / Reduction
                    to_reduce = fill.quantity
                    attributions: list[BatchReductionAttribution] = []

                    # FIFO reduction across active batches
                    new_batches: list[PositionLedgerBatch] = []
                    for b in current_batches:
                        if b.quantity > 0 and to_reduce > 0:
                            deduct = min(b.quantity, to_reduce)
                            remaining_b_qty = b.quantity - deduct
                            to_reduce -= deduct
                            attributions.append(
                                BatchReductionAttribution(
                                    batch_id=b.batch_id,
                                    quantity=deduct,
                                )
                            )
                            new_batches.append(
                                replace(b, quantity=remaining_b_qty)
                            )
                        else:
                            new_batches.append(b)

                    current_batches = new_batches
                    cum_sold += fill.quantity

                    reduction_fact = ExternalReductionFact(
                        trade_id=fill.trade_id,
                        order_id=fill.order_id,
                        quantity=fill.quantity,
                        price=fill.price,
                        reduced_at=fill.trade_at,
                        attributions=tuple(attributions),
                    )
                    current_reductions.append(reduction_fact)

                    current_net = cum_bought - cum_sold

                    # Zero-crossing or flip check
                    if current_net <= Decimal("0"):
                        # Close and archive active episode
                        closed_ep = replace(
                            active_episode,
                            closed_at=fill.trade_at,
                            is_active=False,
                            cumulative_bought=cum_bought,
                            cumulative_sold=cum_sold,
                            peak_quantity=peak_qty,
                            batches=tuple(current_batches),
                            reductions=tuple(current_reductions),
                        )
                        archived_episodes.append(closed_ep)
                        active_episode = None
                        current_batches = []
                        current_reductions = []

                        # If position flipped short
                        if current_net < Decimal("0"):
                            flipped_qty = abs(current_net)
                            episode_counter += 1
                            ep_id = (
                                f"ep_{self._position_key.symbol}_"
                                f"{fill.trade_at.strftime('%Y%m%d%H%M%S')}_{episode_counter}"
                            )
                            active_episode = PositionEpisode(
                                episode_id=ep_id,
                                position_key=self._position_key,
                                side=StrategySide.SHORT,
                                opened_at=fill.trade_at,
                                is_active=True,
                            )
                            cum_bought = Decimal("0")
                            cum_sold = flipped_qty
                            peak_qty = flipped_qty
                            batch_counter = 1
                            batch_id = f"{ep_id}_b{batch_counter}"
                            batch = PositionLedgerBatch(
                                batch_id=batch_id,
                                episode_id=ep_id,
                                quantity=flipped_qty,
                                original_quantity=flipped_qty,
                                entry_price=fill.price,
                                opened_at=fill.trade_at,
                                order_id=fill.order_id,
                                is_external=not is_system,
                            )
                            current_batches = [batch]

            # SHORT Episode Processing (Symmetric)
            elif active_episode.side == StrategySide.SHORT:
                if fill_side == "SELL":
                    batch_counter += 1
                    batch_id = f"{active_episode.episode_id}_b{batch_counter}"
                    batch = PositionLedgerBatch(
                        batch_id=batch_id,
                        episode_id=active_episode.episode_id,
                        quantity=fill.quantity,
                        original_quantity=fill.quantity,
                        entry_price=fill.price,
                        opened_at=fill.trade_at,
                        order_id=fill.order_id,
                        client_order_id=None,
                        is_external=not is_system,
                    )
                    current_batches.append(batch)
                    cum_sold += fill.quantity
                    current_net = cum_sold - cum_bought
                    if current_net > peak_qty:
                        peak_qty = current_net

                elif fill_side == "BUY":
                    to_reduce = fill.quantity
                    attributions = []
                    new_batches = []
                    for b in current_batches:
                        if b.quantity > 0 and to_reduce > 0:
                            deduct = min(b.quantity, to_reduce)
                            remaining_b_qty = b.quantity - deduct
                            to_reduce -= deduct
                            attributions.append(
                                BatchReductionAttribution(
                                    batch_id=b.batch_id,
                                    quantity=deduct,
                                )
                            )
                            new_batches.append(
                                replace(b, quantity=remaining_b_qty)
                            )
                        else:
                            new_batches.append(b)

                    current_batches = new_batches
                    cum_bought += fill.quantity

                    reduction_fact = ExternalReductionFact(
                        trade_id=fill.trade_id,
                        order_id=fill.order_id,
                        quantity=fill.quantity,
                        price=fill.price,
                        reduced_at=fill.trade_at,
                        attributions=tuple(attributions),
                    )
                    current_reductions.append(reduction_fact)

                    current_net = cum_sold - cum_bought
                    if current_net <= Decimal("0"):
                        closed_ep = replace(
                            active_episode,
                            closed_at=fill.trade_at,
                            is_active=False,
                            cumulative_bought=cum_bought,
                            cumulative_sold=cum_sold,
                            peak_quantity=peak_qty,
                            batches=tuple(current_batches),
                            reductions=tuple(current_reductions),
                        )
                        archived_episodes.append(closed_ep)
                        active_episode = None
                        current_batches = []
                        current_reductions = []

                        if current_net < Decimal("0"):
                            flipped_qty = abs(current_net)
                            episode_counter += 1
                            ep_id = (
                                f"ep_{self._position_key.symbol}_"
                                f"{fill.trade_at.strftime('%Y%m%d%H%M%S')}_{episode_counter}"
                            )
                            active_episode = PositionEpisode(
                                episode_id=ep_id,
                                position_key=self._position_key,
                                side=StrategySide.LONG,
                                opened_at=fill.trade_at,
                                is_active=True,
                            )
                            cum_bought = flipped_qty
                            cum_sold = Decimal("0")
                            peak_qty = flipped_qty
                            batch_counter = 1
                            batch_id = f"{ep_id}_b{batch_counter}"
                            batch = PositionLedgerBatch(
                                batch_id=batch_id,
                                episode_id=ep_id,
                                quantity=flipped_qty,
                                original_quantity=flipped_qty,
                                entry_price=fill.price,
                                opened_at=fill.trade_at,
                                order_id=fill.order_id,
                                is_external=not is_system,
                            )
                            current_batches = [batch]

        # Finalize active episode state if open
        final_active_episode: PositionEpisode | None = None
        if active_episode is not None:
            final_active_episode = replace(
                active_episode,
                cumulative_bought=cum_bought,
                cumulative_sold=cum_sold,
                peak_quantity=peak_qty,
                batches=tuple(current_batches),
                reductions=tuple(current_reductions),
            )

        active_batches = (
            final_active_episode.active_batches
            if final_active_episode is not None
            else ()
        )
        total_active_qty = sum(
            (b.quantity for b in active_batches),
            start=Decimal("0"),
        )

        # 4. Compare with latest observation snapshot if present
        unallocated_quantity = Decimal("0")
        reconciliation_gap = Decimal("0")

        if facts.snapshots:
            latest_snapshot = max(
                facts.snapshots,
                key=lambda s: s.observed_at,
            )
            # Compare absolute quantities
            obs_amt = abs(latest_snapshot.position_amt)
            reconciliation_gap = obs_amt - total_active_qty

            if reconciliation_gap != Decimal("0"):
                diagnostics.append(
                    f"Reconciliation gap detected: snapshot={obs_amt}, "
                    f"ledger_active={total_active_qty}, gap={reconciliation_gap}"
                )

        return PositionLedgerProjection(
            position_key=self._position_key,
            active_episode=final_active_episode,
            active_batches=active_batches,
            total_active_quantity=total_active_qty,
            unallocated_quantity=unallocated_quantity,
            reconciliation_gap=reconciliation_gap,
            high_watermark_trade_at=high_watermark,
            archived_episodes=tuple(archived_episodes),
            diagnostics=tuple(diagnostics),
        )
