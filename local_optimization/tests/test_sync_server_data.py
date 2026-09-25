"""Tests for sync_latest_server_data.py functionality."""

from __future__ import annotations

import csv
import gzip
from pathlib import Path
from unittest.mock import patch

from local_optimization.sync_latest_server_data import (
    check_server_health,
    discover_remote_dates,
    get_local_dates_summary,
    partition_table_by_accounts,
)


def test_discover_remote_dates_parsing() -> None:
    """Verify parsing of remote directory discovery script output."""
    mock_output = (
        "Warning: Permanently added ... to known hosts\n"
        "date=2026-09-03: 17\n"
        "date=2026-09-04: 24\n"
        "date=2026-09-21: 24\n"
        "invalid_line\n"
    )
    with patch(
        "local_optimization.sync_latest_server_data.run_ssh_command",
        return_value=mock_output,
    ):
        dates = discover_remote_dates()
        assert dates == {
            "2026-09-03": 17,
            "2026-09-04": 24,
            "2026-09-21": 24,
        }


def test_get_local_dates_summary(tmp_path: Path) -> None:
    """Verify local dates summary counts hour subdirectories correctly."""
    d1 = tmp_path / "date=2026-09-20"
    d1.mkdir()
    (d1 / "hour=00").mkdir()
    (d1 / "hour=01").mkdir()

    d2 = tmp_path / "date=2026-09-21"
    d2.mkdir()
    for h in range(24):
        (d2 / f"hour={h:02d}").mkdir()

    with patch(
        "local_optimization.sync_latest_server_data.ALL_PARQUET_DIR",
        tmp_path,
    ):
        summary = get_local_dates_summary()
        assert summary == {
            "2026-09-20": 2,
            "2026-09-21": 24,
        }


def test_partition_table_by_accounts(tmp_path: Path) -> None:
    """Verify streaming partition separates rows cleanly across 4 accounts."""
    live_latest = tmp_path / "live_latest"
    live_latest.mkdir()

    # Create mock account_balance_snapshots.csv.gz
    gz_bal = live_latest / "account_balance_snapshots.csv.gz"
    with gzip.open(gz_bal, "wt", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["account_label", "asset", "wallet_balance", "observed_at"])
        writer.writerow(["primary", "USDT", "1000.0", "2026-09-21T10:00:00Z"])
        writer.writerow(["primary", "BTC", "0.5", "2026-09-21T10:00:00Z"])  # Non-USDT
        writer.writerow(["account-2", "USDT", "2000.0", "2026-09-21T10:00:00Z"])
        writer.writerow(["account-3", "USDT", "3000.0", "2026-09-21T10:00:00Z"])
        writer.writerow(["account-4", "USDT", "4000.0", "2026-09-21T10:00:00Z"])

    with patch(
        "local_optimization.sync_latest_server_data.LIVE_LATEST_DIR",
        live_latest,
    ):
        partition_table_by_accounts("account_balance_snapshots", gz_bal)

    # Verify primary got 1 USDT row, BTC row excluded
    pri_csv = live_latest / "primary" / "account_balance_usdt.csv"
    assert pri_csv.exists()
    with open(pri_csv, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) == 1
        assert rows[0]["account_label"] == "primary"
        assert rows[0]["asset"] == "USDT"
        assert rows[0]["wallet_balance"] == "1000.0"

    # Verify acc01 (account-2) got 1 row
    acc01_csv = live_latest / "acc01" / "account_balance_usdt.csv"
    assert acc01_csv.exists()
    with open(acc01_csv, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) == 1
        assert rows[0]["account_label"] == "account-2"
        assert rows[0]["wallet_balance"] == "2000.0"


def test_check_server_health() -> None:
    """Verify health check logic."""
    with patch(
        "local_optimization.sync_latest_server_data.run_ssh_command"
    ) as mock_ssh:
        mock_ssh.side_effect = [
            "Linux\n12\n",
            "1\n",
        ]
        assert check_server_health() is True
