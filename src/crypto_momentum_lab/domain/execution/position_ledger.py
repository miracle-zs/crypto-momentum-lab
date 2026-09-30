"""Authoritative position ledger domain service.

Derives position batches and lifecycle episodes strictly from immutable facts
(AccountFillEvent and AccountPositionSnapshot), obeying:
1. Exact quantity conservation;
2. Zero-crossing episode bounding (pre-zero batches never taint post-zero state);
3. Explicit FIFO attribution for lot reductions (both system and external);
4. Zero heuristics: no silent quantity clipping or artificial lookback boundaries.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from crypto_momentum_lab.domain.account.models import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFacts,
    AccountFactStreamScope,
    BatchReductionAttribution,
    DiscrepancyKind,
    ExitOrderSubmissionFact,
    ExternalReductionFact,
    FactCoverageInterval,
    FactCoverageStatus,
    PositionDiscrepancy,
    PositionEpisode,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionLedgerProjection,
)
from crypto_momentum_lab.domain.execution.recovery_models import (
    PositionRecoveryCheckpoint,
    StreamCheckpointAdoption,
    compute_checkpoint_chain_hash,
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

    def create_recovery_checkpoint(
        self,
        facts: AccountFacts,
        *,
        source_revision: int,
        event_cut: datetime | None = None,
        checkpoint_id: str | None = None,
        stream_adoption: StreamCheckpointAdoption | None = None,
    ) -> PositionRecoveryCheckpoint:
        """Create a versioned, complete projection checkpoint from real facts."""
        if facts.position_key.canonical_id != self._position_key.canonical_id:
            raise ValueError("Facts position key does not match ledger key")
        if facts.stream_scope is None or not facts.stream_scope.matches(
            self._position_key
        ):
            raise ValueError("checkpoint creation requires an exact stream scope")
        if type(source_revision) is not int or source_revision < 0:
            raise ValueError("source_revision must be a non-negative integer")
        if (
            facts.has_synthetic_fills
            or facts.has_late_events
            or facts.conflicting_fills
            or facts.fact_conflicts
            or facts.integrity_issues
        ):
            raise ValueError("checkpoint source contains unresolved integrity issues")

        candidate_times = [
            *(fill.trade_at for fill in facts.fills),
            *(snapshot.observed_at for snapshot in facts.snapshots),
            *(boundary.submitted_at for boundary in facts.exit_boundaries),
        ]
        if event_cut is None:
            if not candidate_times:
                raise ValueError("cannot checkpoint without an observed fact cut")
            event_cut = max(candidate_times)
        if event_cut.tzinfo is None or event_cut.utcoffset() is None:
            raise ValueError("event_cut must be timezone-aware")
        if (
            stream_adoption is not None
            and stream_adoption.target_event_cut != event_cut
        ):
            raise ValueError("checkpoint cut does not match stream adoption target")

        if stream_adoption is not None and facts.prefix_facts_complete:
            raise ValueError("stream adoption requires suffix-only facts")
        if stream_adoption is not None and facts.recovery_checkpoint is not None:
            raise ValueError(
                "stream adoption cannot replace an existing target checkpoint"
            )
        parent = (
            stream_adoption.parent_checkpoint
            if stream_adoption is not None
            else facts.recovery_checkpoint
            if not facts.prefix_facts_complete
            else None
        )
        parent_checkpoint_id: str | None = None
        parent_stream_scope: AccountFactStreamScope | None = None
        parent_facts_hash: str | None = None
        parent_projection_digest: str | None = None
        parent_event_cut: datetime | None = None
        checkpoint_suffix_hash: str | None = None
        if not facts.prefix_facts_complete:
            if parent is None:
                raise ValueError(
                    "rolling checkpoint requires a validated parent recovery checkpoint"
                )
            parent_error = (
                _stream_adoption_error(facts, stream_adoption)
                if stream_adoption is not None
                else _checkpoint_error(facts, parent)
            )
            if parent_error is not None:
                raise ValueError(f"parent recovery checkpoint rejected: {parent_error}")
            if parent.event_cut >= event_cut:
                raise ValueError("rolling checkpoint cut must advance beyond parent")
            if any(fill.trade_at <= parent.event_cut for fill in facts.fills):
                raise ValueError("rolling checkpoint suffix contains a late fill")
            if (
                facts.has_late_events
                or facts.has_synthetic_fills
                or facts.conflicting_fills
                or facts.fact_conflicts
                or facts.integrity_issues
            ):
                raise ValueError(
                    "rolling checkpoint suffix contains unresolved integrity issues"
                )

        if (
            parent is None
            and not facts.fills
            and not facts.conflicting_fills
            and not facts.fact_conflicts
            and not _has_verified_flat_snapshot_anchor(facts, event_cut)
        ):
            raise ValueError(
                "an empty new stream requires a verified flat snapshot and complete "
                "source-anchored fill coverage"
            )

        prefix = _facts_at_cut(
            facts,
            event_cut,
            coverage=facts.coverage,
            stream_scope=facts.stream_scope,
            has_synthetic_fills=facts.has_synthetic_fills,
            has_late_events=facts.has_late_events,
            integrity_issues=facts.integrity_issues,
            recovery_checkpoint=None,
        )
        if parent is not None:
            suffix = _facts_after_checkpoint(prefix, parent.event_cut)
            suffix_facts_hash = suffix.compute_facts_hash()
            facts_hash = compute_checkpoint_chain_hash(
                scope=facts.stream_scope,
                parent_stream_scope=parent.stream_scope,
                event_cut=event_cut,
                parent_checkpoint_id=parent.checkpoint_id,
                parent_facts_hash=parent.facts_hash,
                parent_projection_digest=parent.projection_digest,
                parent_event_cut=parent.event_cut,
                suffix_facts_hash=suffix_facts_hash,
            )
            parent_checkpoint_id = parent.checkpoint_id
            parent_stream_scope = parent.stream_scope
            parent_facts_hash = parent.facts_hash
            parent_projection_digest = parent.projection_digest
            parent_event_cut = parent.event_cut
            checkpoint_suffix_hash = suffix_facts_hash
        else:
            facts_hash = prefix.compute_facts_hash()
        projection_facts = (
            replace(
                prefix,
                recovery_checkpoint=(None if stream_adoption is not None else parent),
                prefix_facts_complete=False,
            )
            if parent is not None
            else prefix
        )
        projection = self.project(
            projection_facts,
            stream_adoption=stream_adoption,
        )
        if projection.event_cut is not None and projection.event_cut > event_cut:
            raise ValueError("projection contains facts after checkpoint cut")
        if stream_adoption is not None and (
            not projection.is_comparable
            or projection.health_status != PositionHealthStatus.READY
            or projection.reconciliation_gap != Decimal("0")
            or projection.unallocated_quantity != Decimal("0")
        ):
            raise ValueError(
                "adopted suffix does not reconcile to a complete projection"
            )
        has_synthetic = prefix.has_synthetic_fills or any(
            _is_synthetic_fill(fill) for fill in prefix.fills
        )
        has_conflicts = bool(prefix.conflicting_fills or prefix.fact_conflicts)
        stable_checkpoint_id = checkpoint_id or _checkpoint_identity(
            facts.stream_scope,
            event_cut,
            source_revision,
            facts_hash,
            projection,
        )
        return PositionRecoveryCheckpoint(
            checkpoint_id=stable_checkpoint_id,
            key=self._position_key,
            stream_scope=facts.stream_scope,
            event_cut=event_cut,
            projection=projection,
            facts_hash=facts_hash,
            source_revision=source_revision,
            coverage=prefix.coverage,
            has_conflicts=has_conflicts,
            has_synthetic_fills=has_synthetic,
            has_late_events=prefix.has_late_events,
            integrity_issues=prefix.integrity_issues,
            parent_checkpoint_id=parent_checkpoint_id,
            parent_stream_scope=parent_stream_scope,
            parent_facts_hash=parent_facts_hash,
            parent_projection_digest=parent_projection_digest,
            parent_event_cut=parent_event_cut,
            suffix_facts_hash=checkpoint_suffix_hash,
        )

    def project(
        self,
        facts: AccountFacts,
        *,
        stream_adoption: StreamCheckpointAdoption | None = None,
    ) -> PositionLedgerProjection:
        """Project the current ledger state by replaying all fills in AccountFacts."""
        if facts.position_key.canonical_id != self._position_key.canonical_id:
            raise ValueError(
                f"Facts position key {facts.position_key.canonical_id} does not match "
                f"ledger key {self._position_key.canonical_id}"
            )

        # 1. Reject facts that cannot be assigned to this exact position leg,
        # then deduplicate immutable exchange fills and retain every conflict.
        deduped_fills: dict[str, AccountFillEvent] = {}
        conflicting_fills: list[AccountFillEvent] = list(facts.conflicting_fills)
        identity_issues = list(facts.integrity_issues)
        for fill in facts.fills:
            if (
                fill.environment != self._position_key.environment
                or fill.account_label != self._position_key.account_label
                or fill.symbol != self._position_key.symbol
            ):
                identity_issues.append(
                    f"Fill {fill.trade_id} account/symbol scope does not match "
                    "position key"
                )
                continue
            raw_position_side = fill.raw_position_side
            if raw_position_side is not None:
                if (
                    str(raw_position_side).upper()
                    != self._position_key.position_side.value
                ):
                    identity_issues.append(
                        f"Fill {fill.trade_id} positionSide does not match position key"
                    )
                    continue
            elif self._position_key.position_side != FuturesPositionSide.BOTH:
                identity_issues.append(
                    f"Fill {fill.trade_id} has no positionSide for "
                    "side-specific position"
                )
                continue
            if fill.trade_id in deduped_fills:
                existing = deduped_fills[fill.trade_id]
                if not _same_fill(existing, fill):
                    conflicting_fills.append(fill)
            else:
                deduped_fills[fill.trade_id] = fill

        has_synthetic_fills = facts.has_synthetic_fills or any(
            _is_synthetic_fill(fill) for fill in facts.fills
        )

        all_sorted_fills = sorted(
            deduped_fills.values(),
            key=lambda f: (f.trade_at, f.trade_id),
        )

        if stream_adoption is not None and facts.recovery_checkpoint is not None:
            raise ValueError("stream adoption cannot replace an existing checkpoint")
        checkpoint = facts.recovery_checkpoint
        if stream_adoption is not None:
            checkpoint_error = _stream_adoption_error(facts, stream_adoption)
            checkpoint = stream_adoption.parent_checkpoint
        else:
            checkpoint_error = _checkpoint_error(facts, checkpoint)
        checkpoint_usable = checkpoint is not None and checkpoint_error is None
        if checkpoint_usable and checkpoint is not None:
            if any(fill.trade_at <= checkpoint.event_cut for fill in facts.late_fills):
                checkpoint_usable = False
                checkpoint_error = (
                    "late fill arrived at or before recovery checkpoint cut"
                )

        if checkpoint_usable and checkpoint is not None:
            sorted_fills = [
                fill
                for fill in all_sorted_fills
                if fill.trade_at > checkpoint.event_cut
            ]
        elif not facts.prefix_facts_complete:
            sorted_fills = []
        else:
            sorted_fills = all_sorted_fills

        seed_projection = (
            checkpoint.projection if checkpoint_usable and checkpoint else None
        )
        active_episode: PositionEpisode | None = (
            seed_projection.active_episode if seed_projection is not None else None
        )
        archived_episodes: list[PositionEpisode] = (
            list(seed_projection.archived_episodes)
            if seed_projection is not None
            else []
        )
        diagnostics: list[str] = (
            list(seed_projection.diagnostics) if seed_projection is not None else []
        )
        high_watermark: datetime | None = (
            seed_projection.high_watermark_trade_at
            if seed_projection is not None
            else None
        )
        episode_counter = 0

        # Ephemeral mutable state for active episode
        current_batches: list[PositionLedgerBatch] = (
            list(active_episode.batches) if active_episode is not None else []
        )
        current_reductions: list[ExternalReductionFact] = (
            list(active_episode.reductions) if active_episode is not None else []
        )
        cum_bought = (
            active_episode.cumulative_bought
            if active_episode is not None
            else Decimal("0")
        )
        cum_sold = (
            active_episode.cumulative_sold
            if active_episode is not None
            else Decimal("0")
        )
        peak_qty = (
            active_episode.peak_quantity if active_episode is not None else Decimal("0")
        )
        batch_counter = 0

        if active_episode is not None:
            episode_counter = _episode_counter(active_episode.episode_id)
            batch_counter = max(
                (_batch_counter(batch.batch_id) for batch in current_batches),
                default=0,
            )
        elif archived_episodes:
            episode_counter = max(
                (_episode_counter(ep.episode_id) for ep in archived_episodes),
                default=0,
            )

        # Sort exit boundaries chronologically
        scoped_boundaries = []
        for boundary in facts.exit_boundaries:
            if (
                boundary.symbol != self._position_key.symbol
                or boundary.position_side != self._position_key.position_side
            ):
                identity_issues.append(
                    f"Exit boundary {boundary.order_id} has mismatched position scope"
                )
                continue
            scoped_boundaries.append(boundary)
        sorted_boundaries = sorted(
            scoped_boundaries,
            key=lambda fact: (fact.submitted_at, fact.order_id),
        )
        if checkpoint_usable and checkpoint is not None:
            sorted_boundaries = [
                boundary
                for boundary in sorted_boundaries
                if boundary.submitted_at > checkpoint.event_cut
            ]
        boundary_idx = 0

        def apply_exit_boundary(boundary: ExitOrderSubmissionFact) -> None:
            if not current_batches:
                return
            if boundary.target_batch_id is not None:
                for i, b in enumerate(current_batches):
                    if (
                        b.batch_id == boundary.target_batch_id
                        and b.exit_order_submitted_at is None
                    ):
                        current_batches[i] = replace(
                            b, exit_order_submitted_at=boundary.submitted_at
                        )
                        return
            for i in range(len(current_batches) - 1, -1, -1):
                b = current_batches[i]
                if b.quantity > 0 and b.exit_order_submitted_at is None:
                    current_batches[i] = replace(
                        b, exit_order_submitted_at=boundary.submitted_at
                    )
                    return

        for fill in sorted_fills:
            high_watermark = fill.trade_at
            is_system = fill.order_id in self._system_order_ids or bool(
                fill.raw_payload.get("is_system", False)
            )
            fill_side = fill.side.upper()
            stored_client_order_id = fill.raw_payload.get("client_order_id")
            fill_client_order_id = (
                stored_client_order_id
                if isinstance(stored_client_order_id, str)
                else None
            )

            # Advance and apply boundaries that occurred before this fill
            # (or at the same time if exit fill)
            while boundary_idx < len(sorted_boundaries):
                b = sorted_boundaries[boundary_idx]
                if b.submitted_at < fill.trade_at:
                    apply_exit_boundary(b)
                    boundary_idx += 1
                elif b.submitted_at == fill.trade_at:
                    is_exit_fill = active_episode is not None and (
                        (
                            active_episode.side == StrategySide.LONG
                            and fill_side == "SELL"
                        )
                        or (
                            active_episode.side == StrategySide.SHORT
                            and fill_side == "BUY"
                        )
                    )
                    if is_exit_fill:
                        apply_exit_boundary(b)
                        boundary_idx += 1
                    else:
                        break
                else:
                    break

            # If no active episode, this fill initiates a new episode
            if active_episode is None:
                if bool((fill.raw_payload or {}).get("reduce_only", False)):
                    continue
                if (
                    self._position_key.position_side == FuturesPositionSide.LONG
                    and fill_side != "BUY"
                ):
                    continue
                if (
                    self._position_key.position_side == FuturesPositionSide.SHORT
                    and fill_side != "SELL"
                ):
                    continue
                episode_counter += 1
                side = StrategySide.LONG if fill_side == "BUY" else StrategySide.SHORT
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
                batch_counter = 1
                batch_id = f"{ep_id}_b{batch_counter}"
                batch = PositionLedgerBatch(
                    batch_id=batch_id,
                    episode_id=ep_id,
                    quantity=fill.quantity,
                    original_quantity=fill.quantity,
                    entry_price=fill.price,
                    opened_at=fill.trade_at,
                    order_id=fill.order_id,
                    client_order_id=fill_client_order_id,
                    is_external=not is_system,
                    exit_order_submitted_at=None,
                )
                current_batches.append(batch)
                if side == StrategySide.LONG:
                    cum_bought = fill.quantity
                    peak_qty = fill.quantity
                else:
                    cum_sold = fill.quantity
                    peak_qty = fill.quantity
                continue

            # LONG Episode Processing
            if active_episode.side == StrategySide.LONG:
                if fill_side == "BUY":
                    # Entry / Scaling add
                    latest_batch = current_batches[-1] if current_batches else None
                    if (
                        latest_batch is not None
                        and latest_batch.exit_order_submitted_at is None
                    ):
                        # Add-on entry before exit boundary: aggregate & update anchor
                        new_qty = latest_batch.quantity + fill.quantity
                        new_orig_qty = latest_batch.original_quantity + fill.quantity
                        new_entry_price = (
                            latest_batch.quantity * latest_batch.entry_price
                            + fill.quantity * fill.price
                        ) / new_qty
                        new_opened_at = max(latest_batch.opened_at, fill.trade_at)
                        new_is_external = latest_batch.is_external or (not is_system)
                        current_batches[-1] = replace(
                            latest_batch,
                            quantity=new_qty,
                            original_quantity=new_orig_qty,
                            entry_price=new_entry_price,
                            opened_at=new_opened_at,
                            is_external=new_is_external,
                        )
                    else:
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
                            client_order_id=fill_client_order_id,
                            is_external=not is_system,
                            exit_order_submitted_at=None,
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
                    for lot in current_batches:
                        if lot.quantity > 0 and to_reduce > 0:
                            deduct = min(lot.quantity, to_reduce)
                            remaining_b_qty = lot.quantity - deduct
                            to_reduce -= deduct
                            attributions.append(
                                BatchReductionAttribution(
                                    batch_id=lot.batch_id,
                                    quantity=deduct,
                                )
                            )
                            exit_sub_at = lot.exit_order_submitted_at
                            new_batches.append(
                                replace(
                                    lot,
                                    quantity=remaining_b_qty,
                                    exit_order_submitted_at=exit_sub_at,
                                )
                            )
                        else:
                            new_batches.append(lot)

                    current_batches = new_batches
                    cum_sold += fill.quantity

                    reduction_fact = ExternalReductionFact(
                        trade_id=fill.trade_id,
                        order_id=fill.order_id,
                        quantity=fill.quantity,
                        price=fill.price,
                        reduced_at=fill.trade_at,
                        is_system=is_system,
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
                                client_order_id=fill_client_order_id,
                                is_external=not is_system,
                                exit_order_submitted_at=None,
                            )
                            current_batches = [batch]

            # SHORT Episode Processing (Symmetric)
            elif active_episode.side == StrategySide.SHORT:
                if fill_side == "SELL":
                    latest_batch = current_batches[-1] if current_batches else None
                    if (
                        latest_batch is not None
                        and latest_batch.exit_order_submitted_at is None
                    ):
                        new_qty = latest_batch.quantity + fill.quantity
                        new_orig_qty = latest_batch.original_quantity + fill.quantity
                        new_entry_price = (
                            latest_batch.quantity * latest_batch.entry_price
                            + fill.quantity * fill.price
                        ) / new_qty
                        new_opened_at = max(latest_batch.opened_at, fill.trade_at)
                        new_is_external = latest_batch.is_external or (not is_system)
                        current_batches[-1] = replace(
                            latest_batch,
                            quantity=new_qty,
                            original_quantity=new_orig_qty,
                            entry_price=new_entry_price,
                            opened_at=new_opened_at,
                            is_external=new_is_external,
                        )
                    else:
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
                            client_order_id=fill_client_order_id,
                            is_external=not is_system,
                            exit_order_submitted_at=None,
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
                    for lot in current_batches:
                        if lot.quantity > 0 and to_reduce > 0:
                            deduct = min(lot.quantity, to_reduce)
                            remaining_b_qty = lot.quantity - deduct
                            to_reduce -= deduct
                            attributions.append(
                                BatchReductionAttribution(
                                    batch_id=lot.batch_id,
                                    quantity=deduct,
                                )
                            )
                            exit_sub_at = lot.exit_order_submitted_at
                            new_batches.append(
                                replace(
                                    lot,
                                    quantity=remaining_b_qty,
                                    exit_order_submitted_at=exit_sub_at,
                                )
                            )
                        else:
                            new_batches.append(lot)

                    current_batches = new_batches
                    cum_bought += fill.quantity

                    reduction_fact = ExternalReductionFact(
                        trade_id=fill.trade_id,
                        order_id=fill.order_id,
                        quantity=fill.quantity,
                        price=fill.price,
                        reduced_at=fill.trade_at,
                        is_system=is_system,
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
                                client_order_id=fill_client_order_id,
                                is_external=not is_system,
                                exit_order_submitted_at=None,
                            )
                            current_batches = [batch]

        # Apply any trailing exit boundaries
        while boundary_idx < len(sorted_boundaries):
            apply_exit_boundary(sorted_boundaries[boundary_idx])
            boundary_idx += 1

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

        # 4. Consistent Cut Reconciliation with observation snapshots
        unallocated_quantity = (
            seed_projection.unallocated_quantity
            if seed_projection is not None
            else Decimal("0")
        )
        reconciliation_gap = (
            seed_projection.reconciliation_gap
            if seed_projection is not None
            else Decimal("0")
        )
        health_status = (
            seed_projection.health_status
            if seed_projection is not None
            else PositionHealthStatus.READY
        )
        is_comparable = (
            seed_projection.is_comparable if seed_projection is not None else True
        )
        discrepancy: PositionDiscrepancy | None = (
            seed_projection.discrepancy if seed_projection is not None else None
        )
        if not facts.prefix_facts_complete and not checkpoint_usable:
            health_status = PositionHealthStatus.INCOMPLETE
            is_comparable = False
            diagnostics.append(
                "Historical prefix is unavailable and no valid checkpoint can "
                "seed the position"
            )
        reconciliation_snapshots_list: list[AccountPositionSnapshot] = []
        for snapshot in facts.snapshots:
            if (
                snapshot.environment != self._position_key.environment
                or snapshot.account_label != self._position_key.account_label
                or snapshot.symbol != self._position_key.symbol
                or snapshot.position_side.upper()
                != self._position_key.position_side.value
            ):
                identity_issues.append(
                    f"Snapshot at {snapshot.observed_at.isoformat()} has "
                    "mismatched position scope"
                )
                continue
            if (
                checkpoint is not None
                and checkpoint_usable
                and snapshot.observed_at <= checkpoint.event_cut
            ):
                continue
            reconciliation_snapshots_list.append(snapshot)
        reconciliation_snapshots = tuple(reconciliation_snapshots_list)

        if reconciliation_snapshots:
            latest_snapshot = max(
                reconciliation_snapshots,
                key=lambda s: s.observed_at,
            )
            snap_time = latest_snapshot.observed_at
            obs_amt = abs(latest_snapshot.position_amt)

            if high_watermark is not None and snap_time < high_watermark:
                # Fills stream has advanced past snapshot observed_at (mixed cut /
                # in-flight gap).
                # Replay and verify cut consistency at snap_time.
                fills_at_cut = tuple(f for f in facts.fills if f.trade_at <= snap_time)
                cut_facts = replace(
                    facts,
                    fills=fills_at_cut,
                    snapshots=(),
                )
                cut_projection = PositionLedger(
                    self._position_key,
                    system_order_ids=self._system_order_ids,
                ).project(cut_facts)
                cut_qty = cut_projection.total_active_quantity

                if cut_qty == obs_amt:
                    # Verified consistent at snapshot cut; delta is in-flight recent
                    # fills
                    reconciliation_gap = Decimal("0")
                    health_status = PositionHealthStatus.CATCHING_UP
                    is_comparable = False
                    diagnostics.append(
                        f"Consistent historical cut verified at "
                        f"{snap_time.isoformat()}:"
                        f"cut_qty={cut_qty}, obs_amt={obs_amt}. "
                        f"Recent fills active up to {high_watermark.isoformat()}"
                        "(status=CATCHING_UP)."
                    )
                else:
                    # True divergence even at historical snapshot cut
                    reconciliation_gap = obs_amt - cut_qty
                    health_status = PositionHealthStatus.CONFLICT
                    is_comparable = True
                    diagnostics.append(
                        f"Reconciliation gap at cut {snap_time.isoformat()}: "
                        f"snapshot={obs_amt}, cut_qty={cut_qty}, "
                        f"gap={reconciliation_gap}"
                    )
            else:
                # Snapshot is at or ahead of all fills
                reconciliation_gap = obs_amt - total_active_qty
                if reconciliation_gap == Decimal("0"):
                    health_status = PositionHealthStatus.READY
                    is_comparable = True
                else:
                    # Check if snapshot is slightly ahead within in-flight stream window
                    is_transient = high_watermark is not None and (
                        snap_time - high_watermark
                    ) <= timedelta(seconds=3.0)
                    if is_transient:
                        health_status = PositionHealthStatus.CATCHING_UP
                        is_comparable = False
                        diagnostics.append(
                            f"Transient snapshot lead: snapshot={obs_amt} ahead of "
                            f"fills_qty={total_active_qty} by {reconciliation_gap}"
                            "within flight window."
                        )
                    else:
                        health_status = PositionHealthStatus.CONFLICT
                        is_comparable = True
                        diagnostics.append(
                            f"Reconciliation gap detected: snapshot={obs_amt}, "
                            f"ledger_active={total_active_qty},"
                            "gap={reconciliation_gap}"
                        )

            if health_status in {
                PositionHealthStatus.CONFLICT,
                PositionHealthStatus.INCOMPLETE,
            }:
                raw_hash = (
                    f"{self._position_key.canonical_id}:"
                    f"{snap_time.isoformat()}:{reconciliation_gap}"
                )
                disc_hash = hashlib.sha256(raw_hash.encode()).hexdigest()[:16]
                discrepancy = PositionDiscrepancy(
                    discrepancy_id=f"disc_{self._position_key.symbol}_{disc_hash}",
                    key=self._position_key,
                    kind=(
                        DiscrepancyKind.QUANTITY_MISMATCH
                        if reconciliation_gap != Decimal("0")
                        else DiscrepancyKind.INPUT_MISSING
                    ),
                    first_seen_at=snap_time,
                    last_seen_at=snap_time,
                    count=1,
                    input_hash=disc_hash,
                    details="; ".join(diagnostics),
                    event_cut=high_watermark,
                    snapshot_at=snap_time,
                )

        if (
            unallocated_quantity > Decimal("0")
            and health_status == PositionHealthStatus.READY
        ):
            health_status = PositionHealthStatus.INCOMPLETE

        if conflicting_fills:
            health_status = PositionHealthStatus.CONFLICT
            is_comparable = False
            diag_msg = (
                f"Conflicting duplicate fills detected for trade IDs: "
                f"{[f.trade_id for f in conflicting_fills]}"
            )
            diagnostics.append(diag_msg)
            if discrepancy is None:
                first_conf = conflicting_fills[0]
                raw_hash = (
                    f"{self._position_key.canonical_id}:conflict:{first_conf.trade_id}"
                )
                disc_hash = hashlib.sha256(raw_hash.encode()).hexdigest()[:16]
                discrepancy = PositionDiscrepancy(
                    discrepancy_id=f"disc_{self._position_key.symbol}_{disc_hash}",
                    key=self._position_key,
                    kind=DiscrepancyKind.IDENTITY_MISMATCH,
                    first_seen_at=first_conf.trade_at,
                    last_seen_at=first_conf.trade_at,
                    count=len(conflicting_fills),
                    input_hash=disc_hash,
                    details=diag_msg,
                    event_cut=high_watermark,
                )

        if facts.fact_conflicts:
            health_status = PositionHealthStatus.CONFLICT
            is_comparable = False
            conflict_summary = "; ".join(
                f"{item.event_kind}:{item.event_id}: {item.details}"
                for item in facts.fact_conflicts
            )
            diagnostics.append(f"Conflicting account facts: {conflict_summary}")
            if discrepancy is None:
                event_at = next(
                    (
                        conflict.event_at
                        for conflict in facts.fact_conflicts
                        if conflict.event_at is not None
                    ),
                    high_watermark or datetime(1970, 1, 1, tzinfo=UTC),
                )
                raw_hash = (
                    f"{self._position_key.canonical_id}:fact_conflict:"
                    f"{conflict_summary}"
                )
                disc_hash = hashlib.sha256(raw_hash.encode()).hexdigest()[:16]
                discrepancy = PositionDiscrepancy(
                    discrepancy_id=f"disc_{self._position_key.symbol}_{disc_hash}",
                    key=self._position_key,
                    kind=DiscrepancyKind.IDENTITY_MISMATCH,
                    first_seen_at=event_at,
                    last_seen_at=event_at,
                    count=len(facts.fact_conflicts),
                    input_hash=disc_hash,
                    details=conflict_summary,
                    event_cut=high_watermark,
                )

        if identity_issues:
            health_status = PositionHealthStatus.INCOMPLETE
            is_comparable = False
            diagnostics.extend(identity_issues)

        if facts.has_late_events or facts.late_fills:
            health_status = PositionHealthStatus.INCOMPLETE
            is_comparable = False
            diagnostics.append(
                "Late account facts require checkpoint rebuild and reconciliation"
            )

        if checkpoint is not None and checkpoint_error is not None:
            health_status = PositionHealthStatus.INCOMPLETE
            is_comparable = False
            diagnostics.append(f"Recovery checkpoint rejected: {checkpoint_error}")

        if facts.checkpoint is not None and checkpoint is None:
            health_status = PositionHealthStatus.INCOMPLETE
            is_comparable = False
            diagnostics.append(
                "Legacy quantity checkpoint is not a replayable recovery checkpoint"
            )

        if has_synthetic_fills or (
            checkpoint is not None and checkpoint.has_synthetic_fills
        ):
            health_status = PositionHealthStatus.INCOMPLETE
            is_comparable = False
            diag_msg = "Synthetic fills present; non-authoritative input"
            diagnostics.append(diag_msg)
            synthetic_times = [
                fill.trade_at for fill in facts.fills if _is_synthetic_fill(fill)
            ]
            synthetic_at = min(synthetic_times, default=None)
            observed_cut = (
                synthetic_at
                or high_watermark
                or (checkpoint.event_cut if checkpoint is not None else None)
                or max(
                    (snapshot.observed_at for snapshot in facts.snapshots),
                    default=None,
                )
            )
            if observed_cut is not None:
                raw_hash = f"{self._position_key.canonical_id}:synthetic_fill"
                disc_hash = hashlib.sha256(raw_hash.encode()).hexdigest()[:16]
                discrepancy = PositionDiscrepancy(
                    discrepancy_id=f"disc_{self._position_key.symbol}_{disc_hash}",
                    key=self._position_key,
                    kind=DiscrepancyKind.INPUT_MISSING,
                    first_seen_at=observed_cut,
                    last_seen_at=observed_cut,
                    count=1,
                    input_hash=disc_hash,
                    details=(
                        f"{diag_msg}; previous discrepancy: {discrepancy.details}"
                        if discrepancy is not None
                        else diag_msg
                    ),
                    event_cut=high_watermark,
                )

        if facts.coverage is not None:
            if (
                facts.coverage.has_known_gaps
                or facts.coverage.status == FactCoverageStatus.GAP_DETECTED
            ):
                health_status = _escalate_status(
                    health_status,
                    PositionHealthStatus.INCOMPLETE,
                )
                diag_msg = "Fact coverage interval has known gaps or gap detected"
                diagnostics.append(diag_msg)
                if discrepancy is None:
                    raw_hash = f"{self._position_key.canonical_id}:coverage_gap"
                    disc_hash = hashlib.sha256(raw_hash.encode()).hexdigest()[:16]
                    discrepancy = PositionDiscrepancy(
                        discrepancy_id=f"disc_{self._position_key.symbol}_{disc_hash}",
                        key=self._position_key,
                        kind=DiscrepancyKind.INPUT_MISSING,
                        first_seen_at=facts.coverage.start_at,
                        last_seen_at=facts.coverage.end_at,
                        count=1,
                        input_hash=disc_hash,
                        details=diag_msg,
                        event_cut=high_watermark,
                    )
            elif facts.coverage.status == FactCoverageStatus.PENDING:
                health_status = _escalate_status(
                    health_status,
                    PositionHealthStatus.CATCHING_UP,
                )
                is_comparable = False
                diag_msg = "Fact coverage interval is unconfirmed/pending verification"
                diagnostics.append(diag_msg)
            elif facts.stream_scope is not None and (
                facts.coverage.stream_scope != facts.stream_scope
                or facts.coverage.evidence_observed_at is None
            ):
                health_status = _escalate_status(
                    health_status,
                    PositionHealthStatus.CATCHING_UP,
                )
                is_comparable = False
                diagnostics.append(
                    "Fact coverage does not prove this exact stream scope and epoch"
                )
            elif (
                final_active_episode is not None
                and facts.coverage.start_at > final_active_episode.opened_at
            ):
                health_status = _escalate_status(
                    health_status,
                    PositionHealthStatus.INCOMPLETE,
                )
                diag_msg = (
                    f"Fact coverage start ({facts.coverage.start_at.isoformat()}) "
                    "does not cover active episode opened_at("
                    f"{final_active_episode.opened_at.isoformat()})"
                )
                diagnostics.append(diag_msg)
                if discrepancy is None:
                    raw_hash = f"{self._position_key.canonical_id}:coverage_truncated"
                    disc_hash = hashlib.sha256(raw_hash.encode()).hexdigest()[:16]
                    discrepancy = PositionDiscrepancy(
                        discrepancy_id=f"disc_{self._position_key.symbol}_{disc_hash}",
                        key=self._position_key,
                        kind=DiscrepancyKind.INPUT_MISSING,
                        first_seen_at=facts.coverage.start_at,
                        last_seen_at=facts.coverage.end_at,
                        count=1,
                        input_hash=disc_hash,
                        details=diag_msg,
                        event_cut=high_watermark,
                    )
            elif high_watermark is not None and facts.coverage.end_at < high_watermark:
                health_status = _escalate_status(
                    health_status,
                    PositionHealthStatus.CATCHING_UP,
                )
                is_comparable = False
                diagnostics.append(
                    "Fact coverage ends before the latest applied fill watermark"
                )
        elif facts.stream_scope is not None:
            health_status = _escalate_status(
                health_status,
                PositionHealthStatus.CATCHING_UP,
            )
            is_comparable = False
            diagnostics.append("No durable coverage evidence exists for stream scope")

        facts_hash = facts.compute_facts_hash()
        version_id = f"pv_{facts_hash[:60]}"

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
            health_status=health_status,
            event_cut=high_watermark,
            discrepancy=discrepancy,
            is_comparable=is_comparable,
            projection_version=version_id,
            stream_scope=facts.stream_scope,
        )


def _checkpoint_identity(
    scope: AccountFactStreamScope,
    event_cut: datetime,
    source_revision: int,
    facts_hash: str,
    projection: PositionLedgerProjection,
) -> str:
    from crypto_momentum_lab.domain.execution.recovery_codec import (
        PositionRecoveryCodec,
    )

    projection_payload = PositionRecoveryCodec.encode_projection(projection)
    projection_bytes = json.dumps(
        projection_payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    projection_hash = hashlib.sha256(projection_bytes).hexdigest()
    identity = json.dumps(
        [
            scope.canonical_id,
            event_cut.isoformat(),
            source_revision,
            facts_hash,
            projection_hash,
        ],
        separators=(",", ":"),
    ).encode()
    return f"prc_{hashlib.sha256(identity).hexdigest()}"


def _same_fill(first: AccountFillEvent, second: AccountFillEvent) -> bool:
    return (
        first.environment == second.environment
        and first.account_label == second.account_label
        and first.symbol == second.symbol
        and first.trade_id == second.trade_id
        and first.order_id == second.order_id
        and first.side.upper() == second.side.upper()
        and first.price == second.price
        and first.quantity == second.quantity
        and first.realized_pnl == second.realized_pnl
        and first.fee == second.fee
        and first.fee_asset == second.fee_asset
        and first.trade_at == second.trade_at
        and first.raw_payload == second.raw_payload
    )


def _is_synthetic_fill(fill: AccountFillEvent) -> bool:
    return bool((fill.raw_payload or {}).get("synthetic_from_order", False))


def _checkpoint_error(
    facts: AccountFacts,
    checkpoint: PositionRecoveryCheckpoint | None,
) -> str | None:
    if checkpoint is None:
        return None
    if checkpoint.key.canonical_id != facts.position_key.canonical_id:
        return "position key mismatch"
    if checkpoint.stream_scope != facts.stream_scope:
        return "stream scope or epoch mismatch"
    if checkpoint.has_conflicts:
        return "checkpoint contains unresolved conflicts"
    if checkpoint.has_synthetic_fills:
        return "checkpoint contains non-authoritative synthetic fills"
    if checkpoint.has_late_events:
        return "checkpoint contains late fills"
    if any(fill.trade_at <= checkpoint.event_cut for fill in facts.late_fills):
        return "late fill arrived at or before checkpoint cut"

    if facts.prefix_facts_complete:
        prefix = _facts_at_cut(
            facts,
            checkpoint.event_cut,
            coverage=checkpoint.coverage,
            stream_scope=checkpoint.stream_scope,
            has_synthetic_fills=checkpoint.has_synthetic_fills,
            has_late_events=checkpoint.has_late_events,
            integrity_issues=checkpoint.integrity_issues,
            recovery_checkpoint=None,
        )
        if checkpoint.parent_checkpoint_id is None:
            if prefix.compute_facts_hash() != checkpoint.facts_hash:
                return "prefix facts hash mismatch"
        else:
            if checkpoint.parent_event_cut is None or checkpoint.suffix_facts_hash is None:
                return "checkpoint parent chain incomplete"
            suffix = _facts_after_checkpoint(prefix, checkpoint.parent_event_cut)
            if suffix.compute_facts_hash() != checkpoint.suffix_facts_hash:
                return "checkpoint suffix facts hash mismatch"
            expected_hash = compute_checkpoint_chain_hash(
                scope=checkpoint.stream_scope,
                parent_stream_scope=(
                    checkpoint.parent_stream_scope or checkpoint.stream_scope
                ),
                event_cut=checkpoint.event_cut,
                parent_checkpoint_id=checkpoint.parent_checkpoint_id,
                parent_facts_hash=checkpoint.parent_facts_hash or "",
                parent_projection_digest=checkpoint.parent_projection_digest or "",
                parent_event_cut=checkpoint.parent_event_cut,
                suffix_facts_hash=checkpoint.suffix_facts_hash,
                schema_version=checkpoint.schema_version,
            )
            if expected_hash != checkpoint.facts_hash:
                return "checkpoint parent chain hash mismatch"
    return None


def _stream_adoption_error(
    facts: AccountFacts,
    adoption: StreamCheckpointAdoption,
) -> str | None:
    parent = adoption.parent_checkpoint
    provenance = adoption.fill_load_provenance
    if facts.stream_scope != adoption.target_scope:
        return "target stream scope mismatch"
    if facts.position_key.canonical_id != parent.key.canonical_id:
        return "parent position key mismatch"
    if not adoption.target_scope.matches(facts.position_key):
        return "target position key mismatch"
    if adoption.target_scope == parent.stream_scope:
        return "target stream epoch is not new"
    if (
        parent.has_conflicts
        or parent.has_synthetic_fills
        or parent.has_late_events
        or parent.integrity_issues
    ):
        return "parent checkpoint contains unresolved facts"
    if (
        parent.coverage is None
        or parent.coverage.stream_scope != parent.stream_scope
        or not parent.coverage.is_authoritative
        or not parent.coverage.covers(parent.event_cut)
    ):
        return "parent checkpoint source coverage is not verified"
    if (
        provenance.stream_scope != adoption.target_scope
        or provenance.source_anchor_kind != "recovery_checkpoint"
        or provenance.source_anchor_id != parent.checkpoint_id
        or provenance.source_anchor_event_cut != parent.event_cut
        or provenance.scan_origin_start_time_ms is None
        or provenance.origin_start_at is None
        or provenance.origin_start_at > parent.event_cut
        or provenance.request_from_id is not None
        or not provenance.is_complete
        or provenance.checked_through != adoption.target_event_cut
        or provenance.observed_at != adoption.target_event_cut
    ):
        return "new stream suffix provenance is incomplete or misbound"
    coverage = facts.coverage
    if (
        coverage is None
        or coverage.status != FactCoverageStatus.CONFIRMED
        or not coverage.is_authoritative
        or coverage.stream_scope != adoption.target_scope
        or coverage.load_provenance != provenance
        or facts.fill_load_provenance != provenance
        or coverage.checkpoint_event_cut != adoption.target_event_cut
        or coverage.checkpoint_id is None
        or not coverage.covers_range(parent.event_cut, adoption.target_event_cut)
    ):
        return "new stream suffix is not covered through the adoption cut"
    if (
        facts.has_synthetic_fills
        or facts.has_late_events
        or facts.conflicting_fills
        or facts.fact_conflicts
        or facts.integrity_issues
    ):
        return "new stream suffix contains unresolved facts"
    if any(fill.trade_at <= parent.event_cut for fill in facts.fills):
        return "new stream suffix contains fills at or before parent cut"
    if any(snapshot.observed_at <= parent.event_cut for snapshot in facts.snapshots):
        return "new stream suffix contains snapshots at or before parent cut"
    if any(
        boundary.submitted_at <= parent.event_cut for boundary in facts.exit_boundaries
    ):
        return "new stream suffix contains boundaries at or before parent cut"
    if not any(
        snapshot.environment == facts.position_key.environment
        and snapshot.account_label == facts.position_key.account_label
        and snapshot.symbol == facts.position_key.symbol
        and snapshot.position_side == facts.position_key.position_side.value
        and snapshot.observed_at == adoption.target_event_cut
        for snapshot in facts.snapshots
    ):
        return "new stream adoption requires a snapshot at its target cut"
    if (
        not parent.projection.is_comparable
        or parent.projection.health_status != PositionHealthStatus.READY
        or parent.projection.reconciliation_gap != Decimal("0")
        or parent.projection.unallocated_quantity != Decimal("0")
    ):
        return "parent checkpoint projection is not fully reconciled"
    return None


def _facts_at_cut(
    facts: AccountFacts,
    cut: datetime,
    *,
    coverage: FactCoverageInterval | None,
    stream_scope: AccountFactStreamScope | None,
    has_synthetic_fills: bool | None = None,
    has_late_events: bool | None = None,
    integrity_issues: tuple[str, ...] | None = None,
    recovery_checkpoint: PositionRecoveryCheckpoint | None = None,
) -> AccountFacts:
    selected_coverage = coverage
    selected_scope = stream_scope
    if selected_coverage is not None and (
        (
            selected_coverage.evidence_observed_at is not None
            and selected_coverage.evidence_observed_at > cut
        )
        or (
            selected_coverage.checkpoint_event_cut is not None
            and selected_coverage.checkpoint_event_cut > cut
        )
        or (
            selected_coverage.load_provenance is not None
            and selected_coverage.load_provenance.observed_at > cut
        )
    ):
        selected_coverage = None
    if selected_coverage is not None:
        if selected_coverage.start_at > cut:
            selected_coverage = None
        elif selected_coverage.end_at > cut:
            selected_coverage = replace(selected_coverage, end_at=cut)

    fills = tuple(fill for fill in facts.fills if fill.trade_at <= cut)
    conflicts = tuple(fill for fill in facts.conflicting_fills if fill.trade_at <= cut)
    fact_conflicts = tuple(
        conflict
        for conflict in facts.fact_conflicts
        if conflict.event_at is None or conflict.event_at <= cut
    )
    late_fills = tuple(fill for fill in facts.late_fills if fill.trade_at <= cut)
    return replace(
        facts,
        fills=fills,
        snapshots=tuple(s for s in facts.snapshots if s.observed_at <= cut),
        exit_boundaries=tuple(
            b for b in facts.exit_boundaries if b.submitted_at <= cut
        ),
        coverage=selected_coverage,
        checkpoint=(
            facts.checkpoint
            if facts.checkpoint is not None and facts.checkpoint.event_cut <= cut
            else None
        ),
        has_synthetic_fills=(
            has_synthetic_fills
            if has_synthetic_fills is not None
            else any(_is_synthetic_fill(fill) for fill in fills)
        ),
        conflicting_fills=conflicts,
        has_late_events=(
            has_late_events if has_late_events is not None else bool(late_fills)
        ),
        stream_scope=selected_scope,
        recovery_checkpoint=recovery_checkpoint,
        fact_conflicts=fact_conflicts,
        integrity_issues=(
            integrity_issues if integrity_issues is not None else facts.integrity_issues
        ),
        late_fills=late_fills,
        fill_cursor_provenance=(
            facts.fill_cursor_provenance
            if facts.fill_cursor_provenance is not None
            and facts.fill_cursor_provenance.last_checked_at <= cut
            else None
        ),
        fill_load_provenance=(
            facts.fill_load_provenance
            if facts.fill_load_provenance is not None
            and facts.fill_load_provenance.observed_at <= cut
            else None
        ),
    )


def _facts_after_checkpoint(
    facts: AccountFacts,
    parent_event_cut: datetime,
) -> AccountFacts:
    """Return the exact event-time suffix represented after a checkpoint cut.

    Coverage and integrity state remain attached because they describe the
    suffix's authority. Immutable trade/snapshot/boundary facts at the parent's
    inclusive cut belong to the parent projection and are excluded here.
    """
    if parent_event_cut.tzinfo is None or parent_event_cut.utcoffset() is None:
        raise ValueError("parent checkpoint cut must be timezone-aware")
    return replace(
        facts,
        fills=tuple(fill for fill in facts.fills if fill.trade_at > parent_event_cut),
        snapshots=tuple(
            snapshot
            for snapshot in facts.snapshots
            if snapshot.observed_at > parent_event_cut
        ),
        exit_boundaries=tuple(
            boundary
            for boundary in facts.exit_boundaries
            if boundary.submitted_at > parent_event_cut
        ),
        checkpoint=(
            facts.checkpoint
            if facts.checkpoint is not None
            and facts.checkpoint.event_cut > parent_event_cut
            else None
        ),
        conflicting_fills=tuple(
            fill for fill in facts.conflicting_fills if fill.trade_at > parent_event_cut
        ),
        fact_conflicts=tuple(
            conflict
            for conflict in facts.fact_conflicts
            if conflict.event_at is None or conflict.event_at > parent_event_cut
        ),
        late_fills=tuple(
            fill for fill in facts.late_fills if fill.trade_at > parent_event_cut
        ),
        fill_cursor_provenance=(
            facts.fill_cursor_provenance
            if facts.fill_cursor_provenance is not None
            and facts.fill_cursor_provenance.last_checked_at > parent_event_cut
            else None
        ),
        fill_load_provenance=(
            facts.fill_load_provenance
            if facts.fill_load_provenance is not None
            and facts.fill_load_provenance.observed_at > parent_event_cut
            else None
        ),
        recovery_checkpoint=None,
        prefix_facts_complete=False,
    )


def _has_verified_flat_snapshot_anchor(
    facts: AccountFacts,
    event_cut: datetime,
) -> bool:
    coverage = facts.coverage
    provenance = facts.fill_load_provenance
    if (
        coverage is None
        or provenance is None
        or coverage.status != FactCoverageStatus.CONFIRMED
        or not coverage.is_authoritative
        or coverage.stream_scope != facts.stream_scope
        or coverage.checkpoint_event_cut != event_cut
        or not coverage.covers(event_cut)
        or provenance != coverage.load_provenance
        or provenance.source_anchor_kind != "zero_snapshot"
        or provenance.source_anchor_event_cut != event_cut
        or provenance.checked_through != event_cut
        or provenance.observed_at != event_cut
        or coverage.checkpoint_id != provenance.source_anchor_id
    ):
        return False
    from crypto_momentum_lab.domain.execution.snapshot_encoding import (
        stable_snapshot_anchor_id,
    )

    return any(
        snapshot.environment == facts.position_key.environment
        and snapshot.account_label == facts.position_key.account_label
        and snapshot.symbol == facts.position_key.symbol
        and snapshot.position_side == facts.position_key.position_side.value
        and snapshot.position_amt == Decimal("0")
        and snapshot.observed_at == event_cut
        and stable_snapshot_anchor_id(snapshot)
        == provenance.source_anchor_id
        for snapshot in facts.snapshots
    )


def _episode_counter(episode_id: str) -> int:
    try:
        return int(episode_id.rsplit("_", maxsplit=1)[1])
    except (IndexError, ValueError):
        return 0


def _batch_counter(batch_id: str) -> int:
    try:
        return int(batch_id.rsplit("_b", maxsplit=1)[1])
    except (IndexError, ValueError):
        return 0


def _escalate_status(
    current: PositionHealthStatus,
    requested: PositionHealthStatus,
) -> PositionHealthStatus:
    severity = {
        PositionHealthStatus.READY: 0,
        PositionHealthStatus.CATCHING_UP: 1,
        PositionHealthStatus.INCOMPLETE: 2,
        PositionHealthStatus.CONFLICT: 3,
    }
    return current if severity[current] >= severity[requested] else requested
