import asyncio

import pytest

import crypto_momentum_lab.live_rollout.shadow_preflight as shadow_preflight
from crypto_momentum_lab.persistence.postgres.shadow_repository import (
    PostgresShadowRepository,
)


async def test_shadow_preflight_accepts_an_old_matching_session() -> None:
    class FakeSession:
        def __init__(self) -> None:
            self.statement = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def scalar(self, statement):
            self.statement = statement
            return "shadow-old"

    class FakeFactory:
        def __init__(self, session: FakeSession) -> None:
            self.session = session

        def __call__(self):
            return self.session

    session = FakeSession()
    factory = FakeFactory(session)

    assert await PostgresShadowRepository(factory).has_matching_completed_session(
        strategy_name="orderflow_impulse",
        strategy_config_hash="a" * 64,
    )
    assert session.statement is not None
    assert "ended_at >=" not in str(session.statement)


async def test_missing_shadow_preflight_only_logs_a_warning(
    monkeypatch,
) -> None:
    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def scalar(self, _statement):
            return None

    class FakeFactory:
        def __call__(self):
            return FakeSession()

    warnings = []

    class FakeLogger:
        def warning(self, event, **kwargs):
            warnings.append((event, kwargs))

    monkeypatch.setattr(shadow_preflight, "log", FakeLogger())

    await shadow_preflight.warn_if_shadow_preflight_missing(
        PostgresShadowRepository(FakeFactory()),
        strategy_name="orderflow_impulse",
        strategy_config_hash="a" * 64,
        account_label="primary",
        session_id="live-1",
    )

    assert warnings == [
        (
            "live_shadow_preflight_missing",
            {
                "account_label": "primary",
                "session_id": "live-1",
                "strategy_name": "orderflow_impulse",
                "strategy_config_hash": "a" * 64,
            },
        )
    ]


@pytest.mark.asyncio
async def test_acknowledged_missing_shadow_preflight_logs_info(
    monkeypatch,
) -> None:
    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def scalar(self, _statement):
            return None

    class FakeFactory:
        def __call__(self):
            return FakeSession()

    events = []

    class FakeLogger:
        def info(self, event, **kwargs):
            events.append(("info", event, kwargs))

        def warning(self, event, **kwargs):
            events.append(("warning", event, kwargs))

    monkeypatch.setattr(shadow_preflight, "log", FakeLogger())

    await shadow_preflight.warn_if_shadow_preflight_missing(
        PostgresShadowRepository(FakeFactory()),
        strategy_name="orderflow_impulse",
        strategy_config_hash="a" * 64,
        account_label="primary",
        session_id="live-1",
        acknowledged=True,
    )

    assert events == [
        (
            "info",
            "live_shadow_preflight_missing_acknowledged",
            {
                "account_label": "primary",
                "session_id": "live-1",
                "strategy_name": "orderflow_impulse",
                "strategy_config_hash": "a" * 64,
            },
        )
    ]


async def test_matching_evidence_does_not_log(monkeypatch) -> None:
    class Reader:
        async def has_matching_completed_session(
            self, *, strategy_name: str, strategy_config_hash: str
        ) -> bool:
            assert (strategy_name, strategy_config_hash) == ("strategy", "hash")
            return True

    class NoLog:
        def info(self, *args, **kwargs):
            pytest.fail("matched evidence must not log missing preflight")

        warning = info

    monkeypatch.setattr(shadow_preflight, "log", NoLog())
    await shadow_preflight.warn_if_shadow_preflight_missing(
        Reader(),
        strategy_name="strategy",
        strategy_config_hash="hash",
        account_label="primary",
        session_id="s1",
        acknowledged=True,
    )


@pytest.mark.parametrize(
    "error", [ConnectionError("offline"), asyncio.CancelledError()]
)
async def test_evidence_read_failure_propagates(error: BaseException) -> None:
    class Reader:
        async def has_matching_completed_session(
            self, *, strategy_name: str, strategy_config_hash: str
        ) -> bool:
            raise error

    with pytest.raises(type(error)):
        await shadow_preflight.warn_if_shadow_preflight_missing(
            Reader(),
            strategy_name="strategy",
            strategy_config_hash="hash",
            account_label="primary",
            session_id="s1",
        )
