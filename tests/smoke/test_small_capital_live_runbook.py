from pathlib import Path

RUNBOOK = Path("docs/runbooks/small-capital-live-session.md")


def test_runbook_requires_live_submit_disable_after_session() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "Disable the live-submit configuration immediately" in text


def test_runbook_mentions_real_money_confirmation() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "--i-understand-this-places-real-orders" in text
    assert "ENABLE SMALL LIVE TRADING" in text
    assert "EMERGENCY FLATTEN LIVE ACCOUNT" in text
