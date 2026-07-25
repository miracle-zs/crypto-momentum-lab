import asyncio
from dataclasses import replace
from pathlib import Path

import zstandard

from crypto_momentum_lab.domain.market.models import RawEnvelope
from crypto_momentum_lab.persistence.raw_files.archive import serialize_envelope
from crypto_momentum_lab.persistence.raw_files.recovery import (
    recover_archive_root,
    recover_temporary_archive,
)


async def test_recovery_preserves_complete_records_and_quarantines_source(
    tmp_path: Path,
    raw_envelope: RawEnvelope,
) -> None:
    temporary = write_truncated_archive(tmp_path, raw_envelope)

    result = await recover_temporary_archive(
        temporary,
        archive_root=tmp_path,
        environment="test",
        capture_version="test",
    )

    assert result.manifest.row_count == 1
    assert result.manifest.recovery_status == "recovered"
    assert result.discarded_bytes > 0
    assert result.quarantined_path.exists()
    assert result.quarantined_path.suffix == ".quarantined"
    assert not temporary.exists()


async def test_recovery_commits_multiple_batches_without_serial_timer_waits(
    tmp_path: Path,
    raw_envelope: RawEnvelope,
) -> None:
    temporary = _temporary_path(tmp_path)
    temporary.parent.mkdir(parents=True)
    rows = b"".join(
        serialize_envelope(replace(raw_envelope, local_sequence=index))
        for index in range(1, 252)
    )
    temporary.write_bytes(zstandard.ZstdCompressor(level=1).compress(rows))

    result = await asyncio.wait_for(
        recover_temporary_archive(
            temporary,
            archive_root=tmp_path,
            environment="test",
            capture_version="test",
        ),
        timeout=2,
    )

    assert result.manifest.row_count == 251


async def test_recovery_quarantines_empty_temporary_and_continues(
    tmp_path: Path,
) -> None:
    temporary = _temporary_path(tmp_path)
    temporary.parent.mkdir(parents=True)
    temporary.write_bytes(b"partial-frame")

    results = await recover_archive_root(
        tmp_path,
        environment="test",
        capture_version="test",
    )

    assert results == ()
    assert not temporary.exists()
    assert tuple((tmp_path / ".recovery-quarantine").rglob("*.quarantined"))


def write_truncated_archive(
    tmp_path: Path,
    raw_envelope: RawEnvelope,
) -> Path:
    temporary = _temporary_path(tmp_path)
    temporary.parent.mkdir(parents=True)
    frame = zstandard.ZstdCompressor(level=1).compress(
        serialize_envelope(raw_envelope)
    )
    temporary.write_bytes(frame + b"partial-frame")
    return temporary


def _temporary_path(tmp_path: Path) -> Path:
    return (
        tmp_path
        / "exchange=binance-usdm"
        / "date=2026-06-15"
        / "stream=aggTrade"
        / "symbol=BTCUSDT"
        / "hour=02"
        / (
            "00000000-0000-0000-0000-000000000001-"
            "00000000000000000001.jsonl.zst.tmp"
        )
    )
