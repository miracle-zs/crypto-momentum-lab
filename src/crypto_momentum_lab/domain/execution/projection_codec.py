"""Canonical materialized projection encoding and version-independent digest.

This module depends only on ledger values. Recovery checkpoint validation and
strict recovery decoding share this encoding without depending on each other.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal

from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    ExternalReductionFact,
    PositionDiscrepancy,
    PositionEpisode,
    PositionKey,
    PositionLedgerBatch,
    PositionLedgerProjection,
)


def encode_scope(scope: AccountFactStreamScope) -> dict[str, object]:
    return {
        "environment": scope.environment,
        "account_label": scope.account_label,
        "symbol": scope.symbol,
        "position_side": scope.position_side.value,
        "stream_id": scope.stream_id,
        "stream_epoch": scope.stream_epoch,
    }


def encode_position_key(key: PositionKey) -> dict[str, object]:
    return {
        "environment": key.environment,
        "account_label": key.account_label,
        "symbol": key.symbol,
        "position_side": key.position_side.value,
    }


def encode_batch(batch: PositionLedgerBatch) -> dict[str, object]:
    return {
        "batch_id": batch.batch_id,
        "episode_id": batch.episode_id,
        "quantity": _decimal(batch.quantity),
        "original_quantity": _decimal(batch.original_quantity),
        "entry_price": _decimal(batch.entry_price),
        "opened_at": _datetime(batch.opened_at),
        "order_id": batch.order_id,
        "client_order_id": batch.client_order_id,
        "entry_order_ids": list(batch.entry_order_ids),
        "entry_client_order_ids": list(batch.entry_client_order_ids),
        "is_external": batch.is_external,
        "exit_order_submitted_at": (
            _datetime(batch.exit_order_submitted_at)
            if batch.exit_order_submitted_at is not None
            else None
        ),
    }


def encode_episode(episode: PositionEpisode | None) -> object:
    if episode is None:
        return None
    return {
        "episode_id": episode.episode_id,
        "position_key": encode_position_key(episode.position_key),
        "side": episode.side.value,
        "opened_at": _datetime(episode.opened_at),
        "closed_at": (
            _datetime(episode.closed_at) if episode.closed_at is not None else None
        ),
        "is_active": episode.is_active,
        "cumulative_bought": _decimal(episode.cumulative_bought),
        "cumulative_sold": _decimal(episode.cumulative_sold),
        "peak_quantity": _decimal(episode.peak_quantity),
        "batches": [encode_batch(batch) for batch in episode.batches],
        "reductions": [encode_reduction(reduction) for reduction in episode.reductions],
    }


def encode_reduction(reduction: ExternalReductionFact) -> dict[str, object]:
    return {
        "trade_id": reduction.trade_id,
        "order_id": reduction.order_id,
        "quantity": _decimal(reduction.quantity),
        "price": _decimal(reduction.price),
        "reduced_at": _datetime(reduction.reduced_at),
        "is_system": reduction.is_system,
        "attributions": [
            {"batch_id": item.batch_id, "quantity": _decimal(item.quantity)}
            for item in reduction.attributions
        ],
    }


def encode_discrepancy(
    discrepancy: PositionDiscrepancy | None,
) -> object:
    if discrepancy is None:
        return None
    return {
        "discrepancy_id": discrepancy.discrepancy_id,
        "key": encode_position_key(discrepancy.key),
        "kind": discrepancy.kind.value,
        "first_seen_at": _datetime(discrepancy.first_seen_at),
        "last_seen_at": _datetime(discrepancy.last_seen_at),
        "count": discrepancy.count,
        "input_hash": discrepancy.input_hash,
        "details": discrepancy.details,
        "event_cut": (
            _datetime(discrepancy.event_cut)
            if discrepancy.event_cut is not None
            else None
        ),
        "snapshot_at": (
            _datetime(discrepancy.snapshot_at)
            if discrepancy.snapshot_at is not None
            else None
        ),
        "first_divergent_fact": discrepancy.first_divergent_fact,
        "is_reconciled": discrepancy.is_reconciled,
        "resolution_evidence": discrepancy.resolution_evidence,
    }


def encode_projection(
    projection: PositionLedgerProjection,
) -> dict[str, object]:
    return {
        "position_key": encode_position_key(projection.position_key),
        "active_episode": encode_episode(projection.active_episode),
        "active_batches": [encode_batch(item) for item in projection.active_batches],
        "total_active_quantity": _decimal(projection.total_active_quantity),
        "unallocated_quantity": _decimal(projection.unallocated_quantity),
        "reconciliation_gap": _decimal(projection.reconciliation_gap),
        "high_watermark_trade_at": (
            _datetime(projection.high_watermark_trade_at)
            if projection.high_watermark_trade_at is not None
            else None
        ),
        "archived_episodes": [
            encode_episode(item) for item in projection.archived_episodes
        ],
        "diagnostics": list(projection.diagnostics),
        "health_status": projection.health_status.value,
        "event_cut": (
            _datetime(projection.event_cut)
            if projection.event_cut is not None
            else None
        ),
        "discrepancy": encode_discrepancy(projection.discrepancy),
        "is_comparable": projection.is_comparable,
        "projection_version": projection.projection_version,
        "stream_scope": (
            encode_scope(projection.stream_scope)
            if projection.stream_scope is not None
            else None
        ),
    }


def _decimal(value: Decimal) -> str:
    return format(value, "f")


def _datetime(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("recovery datetimes must be timezone-aware")
    return value.isoformat()


def compute_projection_digest(projection: PositionLedgerProjection) -> str:
    """Hash complete materialized state independently of the fact token."""
    payload = encode_projection(projection)
    payload.pop("projection_version", None)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()
