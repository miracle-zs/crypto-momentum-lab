"""Bounded archival of receipts from permanently superseded execution epochs.

Active epochs, unsequenced identities, trade identities, order watermarks,
policy identities and exit outboxes are never deleted here. A date is an age
budget, not proof that an identity is safe to forget.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import zstandard
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.position_ledger_models import (
    AccountFactStreamScope,
    PositionKey,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
    ExecutionEvidenceReceiptRow,
    ExecutionRetiredStreamRow,
)

SCOPE_FIELDS = (
    "environment",
    "account_label",
    "symbol",
    "position_side",
    "stream_id",
    "stream_epoch",
)


def _scope_identity(scope: AccountFactStreamScope) -> tuple[str, ...]:
    return (
        scope.environment,
        scope.account_label,
        scope.symbol,
        scope.position_side.value,
        scope.stream_id,
        scope.stream_epoch,
    )


def _atomic_write(path: Path, payload: bytes) -> None:
    descriptor, name = tempfile.mkstemp(prefix=".receipt-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _receipt_values(row: ExecutionEvidenceReceiptRow) -> dict[str, object]:
    return {
        **{name: getattr(row, name) for name in SCOPE_FIELDS},
        "evidence_id": row.evidence_id,
        "sequence": row.sequence,
        "payload_digest": row.payload_digest,
        "accepted_at": row.accepted_at.astimezone(UTC).isoformat(),
    }


def archive_receipts(root: Path, records: list[dict[str, object]]) -> Path:
    """Durably write and read-verify the exact bounded batch before deletion."""
    if not records or len(records) > 1000:
        raise ValueError("archive requires 1..1000 receipts")
    content = b"".join(
        (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
        for record in records
    )
    digest = hashlib.sha256(content).hexdigest()
    missing = []
    directory = root.resolve()
    while not directory.exists():
        missing.append(directory)
        directory = directory.parent
    for directory in reversed(missing):
        directory.mkdir(exist_ok=True)
        parent = os.open(directory.parent, os.O_RDONLY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
    path = root / f"receipts-{digest}.jsonl.zst"
    compressed = zstandard.ZstdCompressor(level=3).compress(content)
    _atomic_write(path, compressed)
    stored = path.read_bytes()
    if hashlib.sha256(stored).digest() != hashlib.sha256(compressed).digest():
        raise ValueError("receipt archive checksum verification failed")
    restored = zstandard.ZstdDecompressor().decompress(stored)
    if restored != content:
        raise ValueError("receipt archive verification failed")
    manifest = {
        "format": "execution-receipt-jsonl-zstd",
        "schema_version": 1,
        "file": path.name,
        "record_count": len(records),
        "sha256": hashlib.sha256(compressed).hexdigest(),
        "content_sha256": digest,
        "compressed_bytes": len(compressed),
        "uncompressed_bytes": len(content),
    }
    manifest_path = path.with_suffix(path.suffix + ".manifest.json")
    _atomic_write(manifest_path, json.dumps(manifest, sort_keys=True).encode())
    if json.loads(manifest_path.read_text()) != manifest:
        raise ValueError("receipt manifest verification failed")
    return path


class PostgresExecutionReceiptRetention:
    def __init__(self, session_factory: async_sessionmaker, archive_root: Path):
        self._session_factory = session_factory
        self._archive_root = archive_root

    @staticmethod
    async def _lock_retired_stream(
        session: AsyncSession,
        key: PositionKey,
        identity: tuple[str, ...],
        before: datetime,
    ) -> tuple[str, ExecutionRetiredStreamRow | None]:
        await session.execute(text("SET LOCAL statement_timeout = '5s'"))
        await session.execute(text("SET LOCAL synchronous_commit = ON"))
        locked = await session.scalar(
            text("SELECT pg_try_advisory_xact_lock(hashtext(:lock_key))"),
            {"lock_key": f"execution_position:{key.canonical_id}"},
        )
        if not locked:
            return "busy", None
        retired = await session.get(ExecutionRetiredStreamRow, identity)
        head = await session.get(ExecutionBookHeadRow, identity[:4])
        if retired is None or retired.retired_at >= before or head is None:
            return "protected", None
        if (head.stream_id, head.stream_epoch) == identity[4:]:
            raise ValueError("retired stream is still the current execution head")
        return "ready", retired

    async def prune_retired_stream(
        self,
        scope: AccountFactStreamScope,
        *,
        before: datetime,
        batch_size: int = 500,
        apply: bool = False,
    ) -> dict[str, object]:
        if before.tzinfo is None or not 1 <= batch_size <= 1000:
            raise ValueError("aware cutoff and batch size 1..1000 required")
        identity = _scope_identity(scope)
        key = PositionKey(
            scope.environment, scope.account_label, scope.symbol, scope.position_side
        )
        result: dict[str, object] = {
            "mode": "applied" if apply else "dry_run",
            "deleted": 0,
        }
        conditions = tuple(
            getattr(ExecutionEvidenceReceiptRow, name) == value
            for name, value in zip(SCOPE_FIELDS, identity, strict=True)
        )
        async with self._session_factory() as session, session.begin():
            status, retired = await self._lock_retired_stream(
                session, key, identity, before
            )
            if retired is None:
                return {**result, "status": status}
            rows = list(
                await session.scalars(
                    select(ExecutionEvidenceReceiptRow)
                    .where(
                        *conditions,
                        ExecutionEvidenceReceiptRow.sequence.is_not(None),
                        ExecutionEvidenceReceiptRow.accepted_at < before,
                    )
                    .order_by(ExecutionEvidenceReceiptRow.evidence_id)
                    .limit(batch_size)
                )
            )
            result.update(status="ready", candidates=len(rows))
            records = [_receipt_values(row) for row in rows]
            if not apply:
                return result

        # File IO must not hold the live position's transaction lock. Recheck
        # the permanent fence and exact row contents in a new transaction.
        if records:
            path = await asyncio.to_thread(
                archive_receipts, self._archive_root, records
            )
            result["archive"] = str(path)
        async with self._session_factory() as session, session.begin():
            status, retired = await self._lock_retired_stream(
                session, key, identity, before
            )
            if retired is None:
                return {**result, "status": status}
            if records:
                rows = list(
                    await session.scalars(
                        select(ExecutionEvidenceReceiptRow)
                        .where(
                            *conditions,
                            ExecutionEvidenceReceiptRow.evidence_id.in_(
                                [record["evidence_id"] for record in records]
                            ),
                        )
                        .order_by(ExecutionEvidenceReceiptRow.evidence_id)
                    )
                )
                if [_receipt_values(row) for row in rows] != records:
                    return {**result, "status": "changed"}
                for row in rows:
                    await session.delete(row)
                await session.flush()
                result["deleted"] = len(rows)
            remaining = await session.scalar(
                select(ExecutionEvidenceReceiptRow.evidence_id)
                .where(*conditions, ExecutionEvidenceReceiptRow.sequence.is_not(None))
                .limit(1)
            )
            if remaining is None:
                retired.receipts_archived_at = datetime.now(UTC)
                result["status"] = "completed"
        return result
