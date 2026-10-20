from pathlib import Path

ROOT = Path(__file__).parents[3]
SERVICE = ROOT / "deploy/ops/cml-ops-monitor.service"


def test_ops_monitor_service_bounds_commands_above_live_stop_grace() -> None:
    service = SERVICE.read_text(encoding="utf-8")

    assert (
        "ExecStart=/usr/bin/python3 "
        "/opt/crypto-momentum-lab/deploy/ops/cml_ops_monitor.py "
        "--command-timeout-seconds 120"
    ) in service
