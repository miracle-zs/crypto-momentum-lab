import hashlib
import json
from datetime import UTC, datetime

import pytest
import zstandard

from crypto_momentum_lab.persistence.postgres.execution_receipt_retention import (
    _receipt_values,
    archive_receipts,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionEvidenceReceiptRow,
)


def receipt():
    return _receipt_values(
        ExecutionEvidenceReceiptRow(
            environment="live",
            account_label="primary",
            symbol="BTCUSDT",
            position_side="BOTH",
            stream_id="source",
            stream_epoch="old",
            evidence_id="event",
            payload_digest="digest",
            sequence=1,
            accepted_at=datetime(2026, 10, 1, tzinfo=UTC),
        )
    )


def test_archive_round_trip_and_manifest(tmp_path):
    path = archive_receipts(tmp_path / "receipts", [receipt()])
    compressed = path.read_bytes()
    content = zstandard.ZstdDecompressor().decompress(compressed)
    assert json.loads(content)["evidence_id"] == "event"
    manifest = json.loads(path.with_suffix(path.suffix + ".manifest.json").read_text())
    assert manifest["record_count"] == 1
    assert manifest["sha256"] == hashlib.sha256(compressed).hexdigest()
    assert manifest["content_sha256"] == hashlib.sha256(content).hexdigest()
    assert archive_receipts(tmp_path / "receipts", [receipt()]) == path


def test_corrupted_archive_is_rejected_before_deletion(tmp_path, monkeypatch):
    from crypto_momentum_lab.persistence.postgres import (
        execution_receipt_retention as module,
    )

    write = module._atomic_write

    def corrupt(path, data):
        write(path, b"invalid" if path.suffix == ".zst" else data)

    monkeypatch.setattr(module, "_atomic_write", corrupt)
    with pytest.raises(ValueError, match="checksum"):
        archive_receipts(tmp_path, [receipt()])
    assert not list(tmp_path.glob("*.manifest.json"))


@pytest.mark.parametrize("count", [0, 1001])
def test_archive_batch_is_bounded(tmp_path, count):
    with pytest.raises(ValueError, match="1..1000"):
        archive_receipts(tmp_path, [receipt()] * count)
