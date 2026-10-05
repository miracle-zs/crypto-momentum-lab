from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_housekeeping_only_prunes_host_operational_artifacts() -> None:
    script = (ROOT / "deploy/ops/cml_housekeeping.sh").read_text(encoding="utf-8")

    assert "crash-logs" in script
    assert "-xdev" in script
    assert "table-archive" not in script
    assert "postgres-data" not in script
    assert "docker ps -q" in script
    assert "--no-trunc" in script
    assert "app_image_retention_count" in script


def test_housekeeping_timer_is_daily_and_persistent() -> None:
    timer = (ROOT / "deploy/ops/cml-housekeeping.timer").read_text(encoding="utf-8")

    assert "Asia/Shanghai" in timer
    assert "Persistent=true" in timer
    assert "WantedBy=timers.target" in timer
