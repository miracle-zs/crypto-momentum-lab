from datetime import UTC, date, datetime
from pathlib import Path

from crypto_momentum_lab.persistence.raw_files.retention import (
    delete_archive_files,
    retention_cutoff_date,
)


def archive_path(day: date, suffix: str = ".jsonl.zst") -> Path:
    return Path(
        f"exchange=binance-usdm/date={day.isoformat()}/"
        "stream=aggTrade/symbol=BTCUSDT/hour=00/"
        f"archive{suffix}"
    )


def test_retention_cutoff_keeps_seven_utc_calendar_days() -> None:
    cutoff = retention_cutoff_date(
        now=datetime(2026, 7, 31, 12, 0, tzinfo=UTC),
        retention_days=7,
    )

    assert cutoff == date(2026, 7, 25)


def test_delete_archive_files_only_removes_expired_finalized_files(
    tmp_path: Path,
) -> None:
    expired = archive_path(date(2026, 7, 24))
    retained = archive_path(date(2026, 7, 25))
    temporary = archive_path(date(2026, 7, 24), ".jsonl.zst.tmp")
    for relative in (expired, retained, temporary):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"archive")

    result = delete_archive_files(
        tmp_path,
        [str(expired), str(retained), str(temporary)],
        cutoff_date=date(2026, 7, 25),
    )

    assert result.removable_paths == (str(expired),)
    assert result.deleted_bytes == len(b"archive")
    assert not (tmp_path / expired).exists()
    assert (tmp_path / retained).exists()
    assert (tmp_path / temporary).exists()
