from pathlib import Path

RUNBOOK = Path("docs/runbooks/shadow-operation-session.md")


def test_shadow_runbook_contains_required_commands_and_safety_rule() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "cml-execution-account sync-once" in text
    assert "cml-shadow-operation run" in text
    assert "cml-shadow-operation report" in text
    assert "cml-shadow-operation drill" in text
    assert "No Binance write endpoint" in text
    assert "would-submit" in text
    assert "suppression" in text
