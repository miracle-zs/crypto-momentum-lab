"""Strict JSON codec for versioned account fact recovery values."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Any

from crypto_momentum_lab.domain.account import (
    AccountFillEvent,
    AccountFillReconciliationCursor,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution import projection_codec
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactConflict,
    AccountFacts,
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    BatchReductionAttribution,
    DiscrepancyKind,
    ExitOrderSubmissionFact,
    ExternalReductionFact,
    FactCoverageInterval,
    FactCoverageStatus,
    PositionCheckpoint,
    PositionDiscrepancy,
    PositionEpisode,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
    PositionLedgerProjection,
)
from crypto_momentum_lab.domain.execution.projection_codec import _datetime, _decimal
from crypto_momentum_lab.domain.execution.recovery_models import (
    POSITION_RECOVERY_CHECKPOINT_SCHEMA_VERSION,
    PositionRecoveryCheckpoint,
    RecoverySchemaError,
)
from crypto_momentum_lab.domain.strategy import StrategySide

ACCOUNT_FACTS_SCHEMA_VERSION = 2


class PositionRecoveryCodec:
    """Encode and decode durable facts without permissive field fallbacks."""

    encode_scope = staticmethod(projection_codec.encode_scope)

    @classmethod
    def stable_snapshot_anchor_id(
        cls,
        snapshot: AccountPositionSnapshot,
    ) -> str:
        """Return a deterministic identity suitable for a zero-snapshot anchor."""
        encoded = json.dumps(
            cls.encode_snapshot(snapshot),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return f"psnap_{hashlib.sha256(encoded).hexdigest()}"

    @classmethod
    def decode_scope(cls, value: object) -> AccountFactStreamScope:
        data = _mapping(value, "scope")
        _require_keys(
            data,
            {
                "environment",
                "account_label",
                "symbol",
                "position_side",
                "stream_id",
                "stream_epoch",
            },
            "scope",
        )
        return AccountFactStreamScope(
            environment=_string(data, "environment"),
            account_label=_string(data, "account_label"),
            symbol=_string(data, "symbol"),
            position_side=FuturesPositionSide(_string(data, "position_side")),
            stream_id=_string(data, "stream_id"),
            stream_epoch=_string(data, "stream_epoch"),
        )

    @classmethod
    def encode_cursor(
        cls, cursor: AccountFillReconciliationCursor
    ) -> dict[str, object]:
        return {
            "environment": cursor.environment,
            "account_label": cursor.account_label,
            "symbol": cursor.symbol,
            "from_id": cursor.from_id,
            "start_time_ms": cursor.start_time_ms,
            "last_checked_at": _datetime(cursor.last_checked_at),
        }

    @classmethod
    def decode_cursor(cls, value: object) -> AccountFillReconciliationCursor:
        data = _mapping(value, "fill cursor")
        _require_keys(
            data,
            {
                "environment",
                "account_label",
                "symbol",
                "from_id",
                "start_time_ms",
                "last_checked_at",
            },
            "fill cursor",
        )
        from_id = data.get("from_id")
        start_time_ms = data.get("start_time_ms")
        if from_id is not None and type(from_id) is not int:
            raise RecoverySchemaError("from_id must be an integer or null")
        if start_time_ms is not None and type(start_time_ms) is not int:
            raise RecoverySchemaError("start_time_ms must be an integer or null")
        return AccountFillReconciliationCursor(
            environment=_string(data, "environment"),
            account_label=_string(data, "account_label"),
            symbol=_string(data, "symbol"),
            from_id=from_id,
            start_time_ms=start_time_ms,
            last_checked_at=_datetime_value(data, "last_checked_at"),
        )

    @classmethod
    def encode_fill_load_provenance(
        cls, provenance: AccountFillLoadProvenance
    ) -> dict[str, object]:
        return {
            "stream_scope": cls.encode_scope(provenance.stream_scope),
            "load_id": provenance.load_id,
            "scan_origin_from_id": provenance.scan_origin_from_id,
            "scan_origin_start_time_ms": provenance.scan_origin_start_time_ms,
            "request_from_id": provenance.request_from_id,
            "next_from_id": provenance.next_from_id,
            "page_count": provenance.page_count,
            "page_exhausted": provenance.page_exhausted,
            "truncated": provenance.truncated,
            "checked_through": (
                _datetime(provenance.checked_through)
                if provenance.checked_through is not None
                else None
            ),
            "observed_at": _datetime(provenance.observed_at),
            "source_anchor_id": provenance.source_anchor_id,
            "source_anchor_event_cut": _datetime(provenance.source_anchor_event_cut),
            "source_anchor_kind": provenance.source_anchor_kind,
        }

    @classmethod
    def decode_fill_load_provenance(cls, value: object) -> AccountFillLoadProvenance:
        data = _mapping(value, "fill load provenance")
        _require_keys(
            data,
            {
                "stream_scope",
                "load_id",
                "scan_origin_from_id",
                "scan_origin_start_time_ms",
                "request_from_id",
                "next_from_id",
                "page_count",
                "page_exhausted",
                "truncated",
                "checked_through",
                "observed_at",
                "source_anchor_id",
                "source_anchor_event_cut",
                "source_anchor_kind",
            },
            "fill load provenance",
        )
        nullable_ints = (
            "scan_origin_from_id",
            "scan_origin_start_time_ms",
            "request_from_id",
            "next_from_id",
        )
        for name in nullable_ints:
            if data.get(name) is not None and type(data[name]) is not int:
                raise RecoverySchemaError(f"{name} must be an integer or null")
        if type(data.get("page_count")) is not int:
            raise RecoverySchemaError("page_count must be an integer")
        for name in ("page_exhausted", "truncated"):
            if type(data.get(name)) is not bool:
                raise RecoverySchemaError(f"{name} must be a boolean")
        checked_through = data.get("checked_through")
        if checked_through is not None and not isinstance(checked_through, str):
            raise RecoverySchemaError("checked_through must be a timestamp or null")
        return AccountFillLoadProvenance(
            stream_scope=cls.decode_scope(data.get("stream_scope")),
            load_id=_string(data, "load_id"),
            scan_origin_from_id=data["scan_origin_from_id"],
            scan_origin_start_time_ms=data["scan_origin_start_time_ms"],
            request_from_id=data["request_from_id"],
            next_from_id=data["next_from_id"],
            page_count=data["page_count"],
            page_exhausted=data["page_exhausted"],
            truncated=data["truncated"],
            checked_through=(
                _datetime_value({"value": checked_through}, "value")
                if checked_through is not None
                else None
            ),
            observed_at=_datetime_value(data, "observed_at"),
            source_anchor_id=_string(data, "source_anchor_id"),
            source_anchor_event_cut=_datetime_value(data, "source_anchor_event_cut"),
            source_anchor_kind=_string(data, "source_anchor_kind"),
        )

    encode_position_key = staticmethod(projection_codec.encode_position_key)

    @classmethod
    def decode_position_key(cls, value: object) -> PositionKey:
        data = _mapping(value, "position_key")
        _require_keys(
            data,
            {"environment", "account_label", "symbol", "position_side"},
            "position key",
        )
        return PositionKey(
            environment=_string(data, "environment"),
            account_label=_string(data, "account_label"),
            symbol=_string(data, "symbol"),
            position_side=FuturesPositionSide(_string(data, "position_side")),
        )

    @classmethod
    def encode_fill(cls, fill: AccountFillEvent) -> dict[str, object]:
        return {
            "environment": fill.environment,
            "account_label": fill.account_label,
            "symbol": fill.symbol,
            "trade_id": fill.trade_id,
            "order_id": fill.order_id,
            "side": fill.side,
            "price": _decimal(fill.price),
            "quantity": _decimal(fill.quantity),
            "realized_pnl": _decimal(fill.realized_pnl),
            "fee": _decimal(fill.fee),
            "fee_asset": fill.fee_asset,
            "trade_at": _datetime(fill.trade_at),
            "raw_payload": fill.raw_payload,
        }

    @classmethod
    def decode_fill(cls, value: object) -> AccountFillEvent:
        data = _mapping(value, "fill")
        _require_keys(
            data,
            {
                "environment",
                "account_label",
                "symbol",
                "trade_id",
                "order_id",
                "side",
                "price",
                "quantity",
                "realized_pnl",
                "fee",
                "fee_asset",
                "trade_at",
                "raw_payload",
            },
            "fill",
        )
        return AccountFillEvent(
            environment=_string(data, "environment"),
            account_label=_string(data, "account_label"),
            symbol=_string(data, "symbol"),
            trade_id=_string(data, "trade_id"),
            order_id=_string(data, "order_id"),
            side=_string(data, "side"),
            price=_decimal_value(data, "price"),
            quantity=_decimal_value(data, "quantity"),
            realized_pnl=_decimal_value(data, "realized_pnl"),
            fee=_decimal_value(data, "fee"),
            fee_asset=_string(data, "fee_asset"),
            trade_at=_datetime_value(data, "trade_at"),
            raw_payload=_json_mapping(data, "raw_payload"),
        )

    @classmethod
    def encode_snapshot(cls, snapshot: AccountPositionSnapshot) -> dict[str, object]:
        return {
            "environment": snapshot.environment,
            "account_label": snapshot.account_label,
            "symbol": snapshot.symbol,
            "position_side": snapshot.position_side,
            "position_amt": _decimal(snapshot.position_amt),
            "entry_price": _decimal(snapshot.entry_price),
            "mark_price": _decimal(snapshot.mark_price),
            "unrealized_pnl": _decimal(snapshot.unrealized_pnl),
            "notional": _decimal(snapshot.notional),
            "leverage": snapshot.leverage,
            "margin_type": snapshot.margin_type,
            "observed_at": _datetime(snapshot.observed_at),
            "raw_payload": snapshot.raw_payload,
        }

    @classmethod
    def decode_snapshot(cls, value: object) -> AccountPositionSnapshot:
        data = _mapping(value, "snapshot")
        _require_keys(
            data,
            {
                "environment",
                "account_label",
                "symbol",
                "position_side",
                "position_amt",
                "entry_price",
                "mark_price",
                "unrealized_pnl",
                "notional",
                "leverage",
                "margin_type",
                "observed_at",
                "raw_payload",
            },
            "snapshot",
        )
        leverage = data.get("leverage")
        margin_type = data.get("margin_type")
        if leverage is not None and type(leverage) is not int:
            raise RecoverySchemaError("leverage must be an integer or null")
        if margin_type is not None and not isinstance(margin_type, str):
            raise RecoverySchemaError("margin_type must be a string or null")
        return AccountPositionSnapshot(
            environment=_string(data, "environment"),
            account_label=_string(data, "account_label"),
            symbol=_string(data, "symbol"),
            position_side=_string(data, "position_side"),
            position_amt=_decimal_value(data, "position_amt"),
            entry_price=_decimal_value(data, "entry_price"),
            mark_price=_decimal_value(data, "mark_price"),
            unrealized_pnl=_decimal_value(data, "unrealized_pnl"),
            notional=_decimal_value(data, "notional"),
            leverage=leverage,
            margin_type=margin_type,
            observed_at=_datetime_value(data, "observed_at"),
            raw_payload=_json_mapping(data, "raw_payload"),
        )

    @classmethod
    def encode_boundary(cls, boundary: ExitOrderSubmissionFact) -> dict[str, object]:
        return {
            "order_id": boundary.order_id,
            "submitted_at": _datetime(boundary.submitted_at),
            "symbol": boundary.symbol,
            "position_side": boundary.position_side.value,
            "client_order_id": boundary.client_order_id,
            "target_batch_id": boundary.target_batch_id,
        }

    @classmethod
    def decode_boundary(cls, value: object) -> ExitOrderSubmissionFact:
        data = _mapping(value, "boundary")
        _require_keys(
            data,
            {
                "order_id",
                "submitted_at",
                "symbol",
                "position_side",
                "client_order_id",
                "target_batch_id",
            },
            "boundary",
        )
        client_order_id = data.get("client_order_id")
        target_batch_id = data.get("target_batch_id")
        if client_order_id is not None and not isinstance(client_order_id, str):
            raise RecoverySchemaError("client_order_id must be a string or null")
        if target_batch_id is not None and not isinstance(target_batch_id, str):
            raise RecoverySchemaError("target_batch_id must be a string or null")
        return ExitOrderSubmissionFact(
            order_id=_string(data, "order_id"),
            submitted_at=_datetime_value(data, "submitted_at"),
            symbol=_string(data, "symbol"),
            position_side=FuturesPositionSide(_string(data, "position_side")),
            client_order_id=(
                str(client_order_id) if client_order_id is not None else None
            ),
            target_batch_id=(
                str(target_batch_id) if target_batch_id is not None else None
            ),
        )

    @classmethod
    def encode_coverage(cls, coverage: FactCoverageInterval) -> dict[str, object]:
        return {
            "start_at": _datetime(coverage.start_at),
            "end_at": _datetime(coverage.end_at),
            "has_known_gaps": coverage.has_known_gaps,
            "source_cursor": coverage.source_cursor,
            "status": coverage.status.value,
            "confirmed_revision": coverage.confirmed_revision,
            "checkpoint_id": coverage.checkpoint_id,
            "checkpoint_event_cut": (
                _datetime(coverage.checkpoint_event_cut)
                if coverage.checkpoint_event_cut is not None
                else None
            ),
            "stream_scope": (
                cls.encode_scope(coverage.stream_scope)
                if coverage.stream_scope is not None
                else None
            ),
            "evidence_observed_at": (
                _datetime(coverage.evidence_observed_at)
                if coverage.evidence_observed_at is not None
                else None
            ),
            "load_provenance": (
                cls.encode_fill_load_provenance(coverage.load_provenance)
                if coverage.load_provenance is not None
                else None
            ),
            "page_exhausted": coverage.page_exhausted,
            "not_truncated": coverage.not_truncated,
        }

    @classmethod
    def decode_coverage(cls, value: object) -> FactCoverageInterval:
        data = _mapping(value, "coverage")
        _require_keys(
            data,
            {
                "start_at",
                "end_at",
                "has_known_gaps",
                "source_cursor",
                "status",
                "confirmed_revision",
                "checkpoint_id",
                "checkpoint_event_cut",
                "stream_scope",
                "evidence_observed_at",
                "load_provenance",
                "page_exhausted",
                "not_truncated",
            },
            "coverage",
        )
        revision = data.get("confirmed_revision")
        checkpoint_id = data.get("checkpoint_id")
        checkpoint_event_cut = data.get("checkpoint_event_cut")
        scope = data.get("stream_scope")
        observed_at = data.get("evidence_observed_at")
        source_cursor = data.get("source_cursor")
        if type(data.get("has_known_gaps")) is not bool:
            raise RecoverySchemaError("has_known_gaps must be a boolean")
        if type(data.get("page_exhausted")) is not bool:
            raise RecoverySchemaError("page_exhausted must be a boolean")
        if type(data.get("not_truncated")) is not bool:
            raise RecoverySchemaError("not_truncated must be a boolean")
        if revision is not None and type(revision) is not int:
            raise RecoverySchemaError("confirmed_revision must be an integer or null")
        if checkpoint_id is not None and not isinstance(checkpoint_id, str):
            raise RecoverySchemaError("checkpoint_id must be a string or null")
        if checkpoint_event_cut is not None and not isinstance(
            checkpoint_event_cut, str
        ):
            raise RecoverySchemaError(
                "checkpoint_event_cut must be a timestamp or null"
            )
        if source_cursor is not None and not isinstance(source_cursor, str):
            raise RecoverySchemaError("source_cursor must be a string or null")
        return FactCoverageInterval(
            start_at=_datetime_value(data, "start_at"),
            end_at=_datetime_value(data, "end_at"),
            has_known_gaps=data["has_known_gaps"],
            source_cursor=source_cursor,
            status=FactCoverageStatus(_string(data, "status")),
            confirmed_revision=revision,
            checkpoint_id=checkpoint_id,
            checkpoint_event_cut=(
                _datetime_value({"value": checkpoint_event_cut}, "value")
                if checkpoint_event_cut is not None
                else None
            ),
            stream_scope=(cls.decode_scope(scope) if scope is not None else None),
            evidence_observed_at=(
                _datetime_value({"value": observed_at}, "value")
                if observed_at is not None
                else None
            ),
            load_provenance=(
                cls.decode_fill_load_provenance(data["load_provenance"])
                if data["load_provenance"] is not None
                else None
            ),
            page_exhausted=data["page_exhausted"],
            not_truncated=data["not_truncated"],
        )

    encode_batch = staticmethod(projection_codec.encode_batch)

    @classmethod
    def decode_batch(cls, value: object) -> PositionLedgerBatch:
        data = _mapping(value, "batch")
        _require_keys(
            data,
            {
                "batch_id",
                "episode_id",
                "quantity",
                "original_quantity",
                "entry_price",
                "opened_at",
                "order_id",
                "client_order_id",
                "is_external",
                "exit_order_submitted_at",
            },
            "batch",
        )
        order_id = data.get("order_id")
        client_order_id = data.get("client_order_id")
        exit_at = data.get("exit_order_submitted_at")
        for name, optional_value in (
            ("order_id", order_id),
            ("client_order_id", client_order_id),
        ):
            if optional_value is not None and not isinstance(optional_value, str):
                raise RecoverySchemaError(f"{name} must be a string or null")
        if type(data.get("is_external")) is not bool:
            raise RecoverySchemaError("is_external must be a boolean")
        return PositionLedgerBatch(
            batch_id=_string(data, "batch_id"),
            episode_id=_string(data, "episode_id"),
            quantity=_decimal_value(data, "quantity"),
            original_quantity=_decimal_value(data, "original_quantity"),
            entry_price=_decimal_value(data, "entry_price"),
            opened_at=_datetime_value(data, "opened_at"),
            order_id=order_id,
            client_order_id=client_order_id,
            is_external=data["is_external"],
            exit_order_submitted_at=(
                _datetime_value({"value": exit_at}, "value")
                if exit_at is not None
                else None
            ),
        )

    encode_episode = staticmethod(projection_codec.encode_episode)

    @classmethod
    def decode_episode(cls, value: object) -> PositionEpisode | None:
        if value is None:
            return None
        data = _mapping(value, "episode")
        _require_keys(
            data,
            {
                "episode_id",
                "position_key",
                "side",
                "opened_at",
                "closed_at",
                "is_active",
                "cumulative_bought",
                "cumulative_sold",
                "peak_quantity",
                "batches",
                "reductions",
            },
            "episode",
        )
        closed_at = data.get("closed_at")
        if type(data.get("is_active")) is not bool:
            raise RecoverySchemaError("is_active must be a boolean")
        return PositionEpisode(
            episode_id=_string(data, "episode_id"),
            position_key=cls.decode_position_key(data.get("position_key")),
            side=StrategySide(_string(data, "side")),
            opened_at=_datetime_value(data, "opened_at"),
            closed_at=(
                _datetime_value({"value": closed_at}, "value")
                if closed_at is not None
                else None
            ),
            is_active=data["is_active"],
            cumulative_bought=_decimal_value(data, "cumulative_bought"),
            cumulative_sold=_decimal_value(data, "cumulative_sold"),
            peak_quantity=_decimal_value(data, "peak_quantity"),
            batches=tuple(cls.decode_batch(item) for item in _array(data, "batches")),
            reductions=tuple(
                cls.decode_reduction(item) for item in _array(data, "reductions")
            ),
        )

    encode_reduction = staticmethod(projection_codec.encode_reduction)

    @classmethod
    def decode_reduction(cls, value: object) -> ExternalReductionFact:
        data = _mapping(value, "reduction")
        _require_keys(
            data,
            {
                "trade_id",
                "order_id",
                "quantity",
                "price",
                "reduced_at",
                "is_system",
                "attributions",
            },
            "reduction",
        )
        if type(data.get("is_system")) is not bool:
            raise RecoverySchemaError("is_system must be a boolean")
        attributions: list[BatchReductionAttribution] = []
        for item in _array(data, "attributions"):
            item_data = _mapping(item, "attribution")
            _require_keys(item_data, {"batch_id", "quantity"}, "attribution")
            attributions.append(
                BatchReductionAttribution(
                    batch_id=_string(item_data, "batch_id"),
                    quantity=_decimal_value(item_data, "quantity"),
                )
            )
        return ExternalReductionFact(
            trade_id=_string(data, "trade_id"),
            order_id=_string(data, "order_id"),
            quantity=_decimal_value(data, "quantity"),
            price=_decimal_value(data, "price"),
            reduced_at=_datetime_value(data, "reduced_at"),
            is_system=data["is_system"],
            attributions=tuple(attributions),
        )

    encode_discrepancy = staticmethod(projection_codec.encode_discrepancy)

    @classmethod
    def decode_discrepancy(cls, value: object) -> PositionDiscrepancy | None:
        if value is None:
            return None
        data = _mapping(value, "discrepancy")
        _require_keys(
            data,
            {
                "discrepancy_id",
                "key",
                "kind",
                "first_seen_at",
                "last_seen_at",
                "count",
                "input_hash",
                "details",
                "event_cut",
                "snapshot_at",
                "first_divergent_fact",
                "is_reconciled",
                "resolution_evidence",
            },
            "discrepancy",
        )
        event_cut = data.get("event_cut")
        snapshot_at = data.get("snapshot_at")
        divergent = data.get("first_divergent_fact")
        resolution = data.get("resolution_evidence")
        if divergent is not None and not isinstance(divergent, str):
            raise RecoverySchemaError("first_divergent_fact must be a string or null")
        if resolution is not None and not isinstance(resolution, str):
            raise RecoverySchemaError("resolution_evidence must be a string or null")
        return PositionDiscrepancy(
            discrepancy_id=_string(data, "discrepancy_id"),
            key=cls.decode_position_key(data.get("key")),
            kind=DiscrepancyKind(_string(data, "kind")),
            first_seen_at=_datetime_value(data, "first_seen_at"),
            last_seen_at=_datetime_value(data, "last_seen_at"),
            count=_integer_value(data, "count"),
            input_hash=_string(data, "input_hash"),
            details=_string(data, "details"),
            event_cut=(
                _datetime_value({"value": event_cut}, "value")
                if event_cut is not None
                else None
            ),
            snapshot_at=(
                _datetime_value({"value": snapshot_at}, "value")
                if snapshot_at is not None
                else None
            ),
            first_divergent_fact=divergent,
            is_reconciled=_boolean_value(data, "is_reconciled"),
            resolution_evidence=resolution,
        )

    encode_projection = staticmethod(projection_codec.encode_projection)

    @classmethod
    def decode_projection(cls, value: object) -> PositionLedgerProjection:
        data = _mapping(value, "projection")
        _require_keys(
            data,
            {
                "position_key",
                "active_episode",
                "active_batches",
                "total_active_quantity",
                "unallocated_quantity",
                "reconciliation_gap",
                "high_watermark_trade_at",
                "archived_episodes",
                "diagnostics",
                "health_status",
                "event_cut",
                "discrepancy",
                "is_comparable",
                "projection_version",
                "stream_scope",
            },
            "projection",
        )
        high_watermark = data.get("high_watermark_trade_at")
        event_cut = data.get("event_cut")
        version = data.get("projection_version")
        stream_scope = data.get("stream_scope")
        if type(data.get("is_comparable")) is not bool:
            raise RecoverySchemaError("is_comparable must be a boolean")
        if version is not None and not isinstance(version, str):
            raise RecoverySchemaError("projection_version must be a string or null")
        return PositionLedgerProjection(
            position_key=cls.decode_position_key(data.get("position_key")),
            active_episode=cls.decode_episode(data.get("active_episode")),
            active_batches=tuple(
                cls.decode_batch(item) for item in _array(data, "active_batches")
            ),
            total_active_quantity=_decimal_value(data, "total_active_quantity"),
            unallocated_quantity=_decimal_value(data, "unallocated_quantity"),
            reconciliation_gap=_decimal_value(data, "reconciliation_gap"),
            high_watermark_trade_at=(
                _datetime_value({"value": high_watermark}, "value")
                if high_watermark is not None
                else None
            ),
            archived_episodes=tuple(
                episode
                for item in _array(data, "archived_episodes")
                if (episode := cls.decode_episode(item)) is not None
            ),
            diagnostics=_string_array(data, "diagnostics"),
            health_status=PositionHealthStatus(_string(data, "health_status")),
            event_cut=(
                _datetime_value({"value": event_cut}, "value")
                if event_cut is not None
                else None
            ),
            discrepancy=cls.decode_discrepancy(data.get("discrepancy")),
            is_comparable=data["is_comparable"],
            projection_version=version,
            stream_scope=(
                cls.decode_scope(stream_scope) if stream_scope is not None else None
            ),
        )

    @classmethod
    def encode_legacy_checkpoint(
        cls, checkpoint: PositionCheckpoint
    ) -> dict[str, object]:
        return {
            "checkpoint_id": checkpoint.checkpoint_id,
            "key": cls.encode_position_key(checkpoint.key),
            "event_cut": _datetime(checkpoint.event_cut),
            "net_quantity": _decimal(checkpoint.net_quantity),
            "entry_price": _decimal(checkpoint.entry_price),
            "active_episode_id": checkpoint.active_episode_id,
            "active_batches": [
                cls.encode_batch(item) for item in checkpoint.active_batches
            ],
            "coverage_start": (
                _datetime(checkpoint.coverage_start)
                if checkpoint.coverage_start is not None
                else None
            ),
            "coverage_end": (
                _datetime(checkpoint.coverage_end)
                if checkpoint.coverage_end is not None
                else None
            ),
            "facts_hash": checkpoint.facts_hash,
        }

    @classmethod
    def decode_legacy_checkpoint(cls, value: object) -> PositionCheckpoint:
        data = _mapping(value, "legacy_checkpoint")
        _require_keys(
            data,
            {
                "checkpoint_id",
                "key",
                "event_cut",
                "net_quantity",
                "entry_price",
                "active_episode_id",
                "active_batches",
                "coverage_start",
                "coverage_end",
                "facts_hash",
            },
            "legacy checkpoint",
        )
        active_episode = data.get("active_episode_id")
        coverage_start = data.get("coverage_start")
        coverage_end = data.get("coverage_end")
        if active_episode is not None and not isinstance(active_episode, str):
            raise RecoverySchemaError("active_episode_id must be a string or null")
        return PositionCheckpoint(
            checkpoint_id=_string(data, "checkpoint_id"),
            key=cls.decode_position_key(data.get("key")),
            event_cut=_datetime_value(data, "event_cut"),
            net_quantity=_decimal_value(data, "net_quantity"),
            entry_price=_decimal_value(data, "entry_price"),
            active_episode_id=(active_episode),
            active_batches=tuple(
                cls.decode_batch(item) for item in _array(data, "active_batches")
            ),
            coverage_start=(
                _datetime_value({"value": coverage_start}, "value")
                if coverage_start is not None
                else None
            ),
            coverage_end=(
                _datetime_value({"value": coverage_end}, "value")
                if coverage_end is not None
                else None
            ),
            facts_hash=_string(data, "facts_hash"),
        )

    @classmethod
    def encode_conflict(cls, conflict: AccountFactConflict) -> dict[str, object]:
        return {
            "event_kind": conflict.event_kind,
            "event_id": conflict.event_id,
            "details": conflict.details,
            "event_at": (
                _datetime(conflict.event_at) if conflict.event_at is not None else None
            ),
        }

    @classmethod
    def decode_conflict(cls, value: object) -> AccountFactConflict:
        data = _mapping(value, "conflict")
        _require_keys(
            data, {"event_kind", "event_id", "details", "event_at"}, "conflict"
        )
        event_at = data.get("event_at")
        return AccountFactConflict(
            event_kind=_string(data, "event_kind"),
            event_id=_string(data, "event_id"),
            details=_string(data, "details"),
            event_at=(
                _datetime_value({"value": event_at}, "value")
                if event_at is not None
                else None
            ),
        )

    @classmethod
    def encode_facts(cls, facts: AccountFacts) -> dict[str, object]:
        return {
            "schema_version": ACCOUNT_FACTS_SCHEMA_VERSION,
            "position_key": cls.encode_position_key(facts.position_key),
            "fills": [cls.encode_fill(item) for item in facts.fills],
            "snapshots": [cls.encode_snapshot(item) for item in facts.snapshots],
            "exit_boundaries": [
                cls.encode_boundary(item) for item in facts.exit_boundaries
            ],
            "coverage": (
                cls.encode_coverage(facts.coverage)
                if facts.coverage is not None
                else None
            ),
            "checkpoint": (
                cls.encode_legacy_checkpoint(facts.checkpoint)
                if facts.checkpoint is not None
                else None
            ),
            "has_synthetic_fills": facts.has_synthetic_fills,
            "conflicting_fills": [
                cls.encode_fill(item) for item in facts.conflicting_fills
            ],
            "has_late_events": facts.has_late_events,
            "stream_scope": (
                cls.encode_scope(facts.stream_scope)
                if facts.stream_scope is not None
                else None
            ),
            "recovery_checkpoint": (
                cls.encode_checkpoint(facts.recovery_checkpoint)
                if facts.recovery_checkpoint is not None
                else None
            ),
            "fact_conflicts": [
                cls.encode_conflict(item) for item in facts.fact_conflicts
            ],
            "integrity_issues": list(facts.integrity_issues),
            "late_fills": [cls.encode_fill(item) for item in facts.late_fills],
            "fill_cursor_provenance": (
                cls.encode_cursor(facts.fill_cursor_provenance)
                if facts.fill_cursor_provenance is not None
                else None
            ),
            "fill_load_provenance": (
                cls.encode_fill_load_provenance(facts.fill_load_provenance)
                if facts.fill_load_provenance is not None
                else None
            ),
            "prefix_facts_complete": facts.prefix_facts_complete,
        }

    @classmethod
    def decode_facts(cls, value: object) -> AccountFacts:
        data = _mapping(value, "facts")
        _require_schema_version(
            data,
            version=ACCOUNT_FACTS_SCHEMA_VERSION,
            label="account facts",
        )
        _require_keys(
            data,
            {
                "schema_version",
                "position_key",
                "fills",
                "snapshots",
                "exit_boundaries",
                "coverage",
                "checkpoint",
                "has_synthetic_fills",
                "conflicting_fills",
                "has_late_events",
                "stream_scope",
                "recovery_checkpoint",
                "fact_conflicts",
                "integrity_issues",
                "late_fills",
                "fill_cursor_provenance",
                "fill_load_provenance",
                "prefix_facts_complete",
            },
            "account facts",
        )
        if type(data.get("has_synthetic_fills")) is not bool:
            raise RecoverySchemaError("has_synthetic_fills must be a boolean")
        if type(data.get("has_late_events")) is not bool:
            raise RecoverySchemaError("has_late_events must be a boolean")
        coverage = data.get("coverage")
        checkpoint = data.get("checkpoint")
        scope = data.get("stream_scope")
        recovery = data.get("recovery_checkpoint")
        cursor = data.get("fill_cursor_provenance")
        load_provenance = data.get("fill_load_provenance")
        if type(data.get("prefix_facts_complete")) is not bool:
            raise RecoverySchemaError("prefix_facts_complete must be a boolean")
        return AccountFacts(
            position_key=cls.decode_position_key(data.get("position_key")),
            fills=tuple(cls.decode_fill(item) for item in _array(data, "fills")),
            snapshots=tuple(
                cls.decode_snapshot(item) for item in _array(data, "snapshots")
            ),
            exit_boundaries=tuple(
                cls.decode_boundary(item) for item in _array(data, "exit_boundaries")
            ),
            coverage=cls.decode_coverage(coverage) if coverage is not None else None,
            checkpoint=(
                cls.decode_legacy_checkpoint(checkpoint)
                if checkpoint is not None
                else None
            ),
            has_synthetic_fills=data["has_synthetic_fills"],
            conflicting_fills=tuple(
                cls.decode_fill(item) for item in _array(data, "conflicting_fills")
            ),
            has_late_events=data["has_late_events"],
            stream_scope=cls.decode_scope(scope) if scope is not None else None,
            recovery_checkpoint=(
                cls.decode_checkpoint(recovery) if recovery is not None else None
            ),
            fact_conflicts=tuple(
                cls.decode_conflict(item) for item in _array(data, "fact_conflicts")
            ),
            integrity_issues=_string_array(data, "integrity_issues"),
            late_fills=tuple(
                cls.decode_fill(item) for item in _array(data, "late_fills")
            ),
            fill_cursor_provenance=(
                cls.decode_cursor(cursor) if cursor is not None else None
            ),
            fill_load_provenance=(
                cls.decode_fill_load_provenance(load_provenance)
                if load_provenance is not None
                else None
            ),
            prefix_facts_complete=data["prefix_facts_complete"],
        )

    @classmethod
    def encode_checkpoint(
        cls,
        checkpoint: PositionRecoveryCheckpoint,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": checkpoint.schema_version,
            "checkpoint_id": checkpoint.checkpoint_id,
            "key": cls.encode_position_key(checkpoint.key),
            "stream_scope": cls.encode_scope(checkpoint.stream_scope),
            "event_cut": _datetime(checkpoint.event_cut),
            "projection": cls.encode_projection(checkpoint.projection),
            "facts_hash": checkpoint.facts_hash,
            "source_revision": checkpoint.source_revision,
            "coverage": (
                cls.encode_coverage(checkpoint.coverage)
                if checkpoint.coverage is not None
                else None
            ),
            "has_conflicts": checkpoint.has_conflicts,
            "has_synthetic_fills": checkpoint.has_synthetic_fills,
            "has_late_events": checkpoint.has_late_events,
            "integrity_issues": list(checkpoint.integrity_issues),
            "projection_digest": checkpoint.projection_digest,
            "parent_checkpoint_id": checkpoint.parent_checkpoint_id,
            "parent_facts_hash": checkpoint.parent_facts_hash,
            "parent_projection_digest": checkpoint.parent_projection_digest,
            "parent_event_cut": (
                _datetime(checkpoint.parent_event_cut)
                if checkpoint.parent_event_cut is not None
                else None
            ),
            "suffix_facts_hash": checkpoint.suffix_facts_hash,
        }
        if checkpoint.schema_version >= 3:
            payload["parent_stream_scope"] = (
                cls.encode_scope(checkpoint.parent_stream_scope)
                if checkpoint.parent_stream_scope is not None
                else None
            )
        return payload

    @classmethod
    def decode_checkpoint(cls, value: object) -> PositionRecoveryCheckpoint:
        data = _mapping(value, "checkpoint")
        version = _integer_value(data, "schema_version")
        common_keys = {
            "schema_version",
            "checkpoint_id",
            "key",
            "stream_scope",
            "event_cut",
            "projection",
            "facts_hash",
            "source_revision",
            "coverage",
            "has_conflicts",
            "has_synthetic_fills",
            "has_late_events",
            "integrity_issues",
            "projection_digest",
            "parent_checkpoint_id",
            "parent_facts_hash",
            "parent_projection_digest",
            "parent_event_cut",
            "suffix_facts_hash",
        }
        required_keys = common_keys | (
            {"parent_stream_scope"} if version >= 3 else set()
        )
        _require_keys(
            data,
            required_keys,
            "checkpoint",
        )
        if version != POSITION_RECOVERY_CHECKPOINT_SCHEMA_VERSION:
            if version != 2:
                raise RecoverySchemaError(
                    f"unsupported position recovery checkpoint schema {version}"
                )
        elif "parent_stream_scope" not in data:
            raise RecoverySchemaError(
                "checkpoint schema 3 requires parent_stream_scope"
            )
        coverage = data.get("coverage")
        parent_checkpoint_id = data.get("parent_checkpoint_id")
        parent_facts_hash = data.get("parent_facts_hash")
        parent_projection_digest = data.get("parent_projection_digest")
        parent_event_cut = data.get("parent_event_cut")
        suffix_facts_hash = data.get("suffix_facts_hash")
        for name, value in (
            ("parent_checkpoint_id", parent_checkpoint_id),
            ("parent_facts_hash", parent_facts_hash),
            ("parent_projection_digest", parent_projection_digest),
            ("suffix_facts_hash", suffix_facts_hash),
        ):
            if value is not None and not isinstance(value, str):
                raise RecoverySchemaError(f"{name} must be a string or null")
        return PositionRecoveryCheckpoint(
            schema_version=version,
            checkpoint_id=_string(data, "checkpoint_id"),
            key=cls.decode_position_key(data.get("key")),
            stream_scope=cls.decode_scope(data.get("stream_scope")),
            event_cut=_datetime_value(data, "event_cut"),
            projection=cls.decode_projection(data.get("projection")),
            facts_hash=_string(data, "facts_hash"),
            source_revision=_integer_value(data, "source_revision"),
            coverage=cls.decode_coverage(coverage) if coverage is not None else None,
            has_conflicts=_boolean_value(data, "has_conflicts"),
            has_synthetic_fills=_boolean_value(data, "has_synthetic_fills"),
            has_late_events=_boolean_value(data, "has_late_events"),
            integrity_issues=_string_array(data, "integrity_issues"),
            projection_digest=_string(data, "projection_digest"),
            parent_checkpoint_id=parent_checkpoint_id,
            parent_facts_hash=parent_facts_hash,
            parent_projection_digest=parent_projection_digest,
            parent_event_cut=(
                _datetime_value({"value": parent_event_cut}, "value")
                if parent_event_cut is not None
                else None
            ),
            suffix_facts_hash=suffix_facts_hash,
            parent_stream_scope=(
                cls.decode_scope(data.get("parent_stream_scope"))
                if data.get("parent_stream_scope") is not None
                else None
            ),
        )

    compute_projection_digest = staticmethod(projection_codec.compute_projection_digest)


def _mapping(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise RecoverySchemaError(f"{label} must be an object")
    return value


def _require_keys(
    data: dict[str, object],
    keys: set[str],
    label: str,
) -> None:
    actual = set(data)
    if actual != keys:
        missing = sorted(keys - actual)
        extra = sorted(actual - keys)
        raise RecoverySchemaError(
            f"{label} fields differ from schema; missing={missing}, extra={extra}"
        )


def _require_schema_version(
    data: dict[str, object],
    *,
    version: int,
    label: str,
) -> None:
    found = data.get("schema_version")
    if type(found) is not int or found != version:
        raise RecoverySchemaError(
            f"unsupported {label} schema version {found!r}; expected {version}"
        )


def _array(data: dict[str, object], name: str) -> list[object]:
    value = data.get(name, [])
    if not isinstance(value, list):
        raise RecoverySchemaError(f"{name} must be an array")
    return value


def _string_array(data: dict[str, object], name: str) -> tuple[str, ...]:
    values = _array(data, name)
    if any(not isinstance(item, str) for item in values):
        raise RecoverySchemaError(f"{name} must contain only strings")
    return tuple(values)


def _string(data: dict[str, object], name: str) -> str:
    value = data.get(name)
    if not isinstance(value, str):
        raise RecoverySchemaError(f"{name} must be a string")
    return value


def _json_mapping(data: dict[str, object], name: str) -> dict[str, Any]:
    return _mapping(data.get(name), name)


def _decimal_value(data: dict[str, object], name: str) -> Decimal:
    value = data.get(name)
    if not isinstance(value, str):
        raise RecoverySchemaError(f"{name} must be a decimal string")
    try:
        return Decimal(value)
    except Exception as exc:
        raise RecoverySchemaError(f"{name} must be a valid decimal string") from exc


def _integer_value(data: dict[str, object], name: str) -> int:
    value = data.get(name)
    if type(value) is not int:
        raise RecoverySchemaError(f"{name} must be an integer")
    return value


def _boolean_value(data: dict[str, object], name: str) -> bool:
    value = data.get(name)
    if type(value) is not bool:
        raise RecoverySchemaError(f"{name} must be a boolean")
    return value


def _datetime_value(data: dict[str, object], name: str) -> datetime:
    value = data.get(name)
    if not isinstance(value, str):
        raise RecoverySchemaError(f"{name} must be an ISO datetime string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise RecoverySchemaError(f"{name} must be a valid ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RecoverySchemaError(f"{name} must include a timezone")
    return parsed
