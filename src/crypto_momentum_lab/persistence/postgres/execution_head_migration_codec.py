"""Strict JSON codec for records retained by the legacy head migration."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from crypto_momentum_lab.domain.account.models import (
    AccountFillEvent,
    AccountPositionSnapshot,
)
from crypto_momentum_lab.domain.execution.order_state import FuturesPositionSide
from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    AccountFillLoadProvenance,
    DiscrepancyKind,
    ExitOrderSubmissionFact,
    FactCoverageInterval,
    FactCoverageStatus,
    PositionDiscrepancy,
    PositionEpisode,
    PositionHealthStatus,
    PositionKey,
    PositionLedgerBatch,
)
from crypto_momentum_lab.domain.trading import TradeSide as StrategySide
from . import (
    execution_head_migration_projection as projection_codec,
)
from .execution_head_migration_projection import (
    LegacyPositionProjection,
    _datetime,
    _decimal,
)
from .execution_head_migration_snapshot import encode_position_snapshot


class RecoverySchemaError(ValueError):
    """Stored recovery data uses an unsupported or invalid schema."""


class PositionRecoveryCodec:
    """Encode and decode typed records used by the 0062 head migration."""

    encode_scope = staticmethod(projection_codec.encode_scope)

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
        parsed_ints: dict[str, int | None] = {}
        for name in nullable_ints:
            raw_int = data.get(name)
            if raw_int is not None and type(raw_int) is not int:
                raise RecoverySchemaError(f"{name} must be an integer or null")
            parsed_ints[name] = raw_int
        page_count = data.get("page_count")
        if type(page_count) is not int:
            raise RecoverySchemaError("page_count must be an integer")
        parsed_flags: dict[str, bool] = {}
        for name in ("page_exhausted", "truncated"):
            raw_flag = data.get(name)
            if type(raw_flag) is not bool:
                raise RecoverySchemaError(f"{name} must be a boolean")
            parsed_flags[name] = raw_flag
        checked_through = data.get("checked_through")
        if checked_through is not None and not isinstance(checked_through, str):
            raise RecoverySchemaError("checked_through must be a timestamp or null")
        return AccountFillLoadProvenance(
            stream_scope=cls.decode_scope(data.get("stream_scope")),
            load_id=_string(data, "load_id"),
            scan_origin_from_id=parsed_ints["scan_origin_from_id"],
            scan_origin_start_time_ms=parsed_ints["scan_origin_start_time_ms"],
            request_from_id=parsed_ints["request_from_id"],
            next_from_id=parsed_ints["next_from_id"],
            page_count=page_count,
            page_exhausted=parsed_flags["page_exhausted"],
            truncated=parsed_flags["truncated"],
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

    encode_snapshot = staticmethod(encode_position_snapshot)

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
        }

    @classmethod
    def decode_coverage(cls, value: object) -> FactCoverageInterval:
        data = _mapping(value, "coverage")
        keys = {
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
        }
        legacy_keys = keys | {"page_exhausted", "not_truncated"}
        if set(data) not in (keys, legacy_keys):
            _require_keys(data, keys, "coverage")
        revision = data.get("confirmed_revision")
        checkpoint_id = data.get("checkpoint_id")
        checkpoint_event_cut = data.get("checkpoint_event_cut")
        scope = data.get("stream_scope")
        observed_at = data.get("evidence_observed_at")
        source_cursor = data.get("source_cursor")
        has_known_gaps = data.get("has_known_gaps")
        if type(has_known_gaps) is not bool:
            raise RecoverySchemaError("has_known_gaps must be a boolean")
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
            has_known_gaps=has_known_gaps,
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
                "entry_order_ids",
                "entry_client_order_ids",
                "is_external",
                "exit_order_submitted_at",
            },
            "batch",
        )
        order_id = data.get("order_id")
        client_order_id = data.get("client_order_id")
        exit_at = data.get("exit_order_submitted_at")
        parsed_ids: dict[str, str | None] = {}
        for name, optional_value in (
            ("order_id", order_id),
            ("client_order_id", client_order_id),
        ):
            if optional_value is not None and not isinstance(optional_value, str):
                raise RecoverySchemaError(f"{name} must be a string or null")
            parsed_ids[name] = optional_value
        is_external = data.get("is_external")
        if type(is_external) is not bool:
            raise RecoverySchemaError("is_external must be a boolean")
        return PositionLedgerBatch(
            batch_id=_string(data, "batch_id"),
            episode_id=_string(data, "episode_id"),
            quantity=_decimal_value(data, "quantity"),
            original_quantity=_decimal_value(data, "original_quantity"),
            entry_price=_decimal_value(data, "entry_price"),
            opened_at=_datetime_value(data, "opened_at"),
            order_id=parsed_ids["order_id"],
            client_order_id=parsed_ids["client_order_id"],
            entry_order_ids=_string_array(data, "entry_order_ids"),
            entry_client_order_ids=_string_array(data, "entry_client_order_ids"),
            is_external=is_external,
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
        is_active = data.get("is_active")
        if type(is_active) is not bool:
            raise RecoverySchemaError("is_active must be a boolean")
        # The schema-1 to schema-4 head migration drops historical reductions.
        _array(data, "reductions")
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
            is_active=is_active,
            cumulative_bought=_decimal_value(data, "cumulative_bought"),
            cumulative_sold=_decimal_value(data, "cumulative_sold"),
            peak_quantity=_decimal_value(data, "peak_quantity"),
            batches=tuple(cls.decode_batch(item) for item in _array(data, "batches")),
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
    def decode_projection(cls, value: object) -> LegacyPositionProjection:
        data = _mapping(value, "projection")
        current_keys = {
            "position_key",
            "active_episode",
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
        }
        derived_keys = {"active_batches", "total_active_quantity"}
        if set(data) not in (current_keys, current_keys | derived_keys):
            _require_keys(data, current_keys, "projection")
        high_watermark = data.get("high_watermark_trade_at")
        event_cut = data.get("event_cut")
        version = data.get("projection_version")
        stream_scope = data.get("stream_scope")
        is_comparable = data.get("is_comparable")
        if type(is_comparable) is not bool:
            raise RecoverySchemaError("is_comparable must be a boolean")
        if version is not None and not isinstance(version, str):
            raise RecoverySchemaError("projection_version must be a string or null")
        active_episode = cls.decode_episode(data.get("active_episode"))
        active_batches = active_episode.active_batches if active_episode else ()
        total_active_quantity = sum(
            (batch.quantity for batch in active_batches), start=Decimal("0")
        )
        if derived_keys <= set(data):
            stored_batches = tuple(
                cls.decode_batch(item) for item in _array(data, "active_batches")
            )
            stored_quantity = _decimal_value(data, "total_active_quantity")
            if (
                stored_batches != active_batches
                or stored_quantity != total_active_quantity
            ):
                raise RecoverySchemaError(
                    "legacy projection derived fields disagree with active episode"
                )
        return LegacyPositionProjection(
            position_key=cls.decode_position_key(data.get("position_key")),
            active_episode=active_episode,
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
            is_comparable=is_comparable,
            projection_version=version,
            stream_scope=(
                cls.decode_scope(stream_scope) if stream_scope is not None else None
            ),
        )


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
    value = data[name]
    if not isinstance(value, list):
        raise RecoverySchemaError(f"{name} must be an array")
    return value


def _string_array(data: dict[str, object], name: str) -> tuple[str, ...]:
    values = _array(data, name)
    parsed: list[str] = []
    for item in values:
        if not isinstance(item, str):
            raise RecoverySchemaError(f"{name} must contain only strings")
        parsed.append(item)
    return tuple(parsed)


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
