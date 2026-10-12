"""Fact-integrity aggregation: current risk versus retained historical proof."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from crypto_momentum_lab.operator_dashboard.fact_integrity_queries import (
    load_fact_integrity,
)

NOW = datetime(2026, 10, 12, 18, 0, tzinfo=UTC)


class _Result:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def all(self) -> list[object]:
        return list(self._rows)


class _Session:
    def __init__(self, heads, checkpoints, suffix_rows) -> None:
        self._scalars = [heads, checkpoints]
        self._execute = suffix_rows

    async def scalars(self, _statement):
        return _Result(self._scalars.pop(0))

    async def execute(self, _statement):
        return _Result(self._execute)


class _Sessions:
    def __init__(self, session) -> None:
        self._session = session

    def __call__(self):
        return self

    async def __aenter__(self):
        return self._session

    async def __aexit__(self, *_args) -> None:
        return None


class _Coverage:
    def __init__(self, end_at: datetime) -> None:
        self.end_at = end_at

    @property
    def is_authoritative(self) -> bool:
        return True

    def covers(self, cut: datetime) -> bool:
        return cut <= self.end_at


def _checkpoint(
    *,
    account: str,
    symbol: str,
    side: str,
    active_quantity: Decimal,
    checkpoint_id: str = "cp-1",
):
    return SimpleNamespace(
        key=SimpleNamespace(
            environment="live",
            account_label=account,
            symbol=symbol,
            position_side=SimpleNamespace(value=side),
        ),
        event_cut=NOW - timedelta(seconds=30),
        projection=SimpleNamespace(
            total_active_quantity=active_quantity,
            reconciliation_gap=Decimal("0"),
            health_status=SimpleNamespace(value="READY"),
        ),
        coverage=_Coverage(NOW - timedelta(seconds=30)),
        has_conflicts=False,
        has_synthetic_fills=False,
        has_late_events=False,
        integrity_issues=(),
        checkpoint_id=checkpoint_id,
    )


def _head(
    *,
    account: str,
    symbol: str,
    side: str,
    revision: int,
    active_hint: bool,
    unresolved: bool,
):
    return SimpleNamespace(
        account_label=account,
        symbol=symbol,
        position_side=side,
        stream_id="account_event_hub",
        stream_epoch="epoch-1",
        updated_at=NOW,
        state_payload={
            "journal_revision": revision,
            "recovery_checkpoint": {"checkpoint_id": "cp-1"},
            "recovery_command_ids": ["cmd-1"] if unresolved else [],
            "active_reservation_ids": [],
        },
        _active_hint=active_hint,
    )


def _checkpoint_row(account: str, symbol: str, side: str):
    return SimpleNamespace(
        account_label=account,
        symbol=symbol,
        position_side=side,
        stream_id="account_event_hub",
        stream_epoch="epoch-1",
        checkpoint_id="cp-1",
        source_revision=10,
        event_cut=NOW - timedelta(seconds=30),
        payload={},
    )


def _run(*, heads, checkpoints, suffix_rows, accounts, checkpoint_for):
    session = _Session(heads, checkpoints, suffix_rows)
    with patch(
        "crypto_momentum_lab.operator_dashboard.fact_integrity_queries"
        ".PositionRecoveryCodec.decode_checkpoint",
        side_effect=checkpoint_for,
    ), patch(
        "crypto_momentum_lab.operator_dashboard.fact_integrity_queries"
        ".recovery_checkpoint_head_binding",
        return_value={"checkpoint_id": "cp-1"},
    ):
        import asyncio

        return asyncio.run(
            load_fact_integrity(_Sessions(session), accounts=accounts, now=NOW)
        )


def test_closed_historical_object_does_not_unknown_current_health() -> None:
    """A stale closed object keeps its own evidence without blocking the account."""
    heads = [
        _head(
            account="account-2",
            symbol="BTCUSDT",
            side="LONG",
            revision=13,
            active_hint=False,
            unresolved=False,
        )
    ]
    checkpoints = [_checkpoint_row("account-2", "BTCUSDT", "LONG")]
    # The closed object's suffix is incomplete, so its own proof is unknown.
    integrity = _run(
        heads=heads,
        checkpoints=checkpoints,
        suffix_rows=[],
        accounts={"account-2": SimpleNamespace(position_count=0)},
        checkpoint_for=lambda payload: _checkpoint(
            account="account-2",
            symbol="BTCUSDT",
            side="LONG",
            active_quantity=Decimal("0"),
        ),
    )

    summary = integrity["account-2"]
    assert summary.gaps == 0
    assert summary.observed_at is None
    assert summary.reasons == ()
    assert summary.historical_reasons == ("journal_suffix_incomplete",)


def test_open_position_suffix_inside_verified_cut_is_proven() -> None:
    heads = [
        _head(
            account="primary",
            symbol="WUSDT",
            side="LONG",
            revision=13,
            active_hint=True,
            unresolved=False,
        )
    ]
    checkpoints = [_checkpoint_row("primary", "WUSDT", "LONG")]
    suffix_rows = [
        (
            "primary", "WUSDT", "LONG", "account_event_hub", "epoch-1",
            3, 13, NOW - timedelta(seconds=30),
        )
    ]
    integrity = _run(
        heads=heads,
        checkpoints=checkpoints,
        suffix_rows=suffix_rows,
        accounts={"primary": SimpleNamespace(position_count=1)},
        checkpoint_for=lambda payload: _checkpoint(
            account="primary",
            symbol="WUSDT",
            side="LONG",
            active_quantity=Decimal("3184.7"),
        ),
    )

    summary = integrity["primary"]
    assert summary.gaps == 0
    assert summary.observed_at == NOW - timedelta(seconds=30)
    assert summary.reasons == ()


def test_unresolved_work_keeps_a_closed_object_in_current_scope() -> None:
    heads = [
        _head(
            account="account-2",
            symbol="WUSDT",
            side="LONG",
            revision=13,
            active_hint=False,
            unresolved=True,
        )
    ]
    checkpoints = [_checkpoint_row("account-2", "WUSDT", "LONG")]
    integrity = _run(
        heads=heads,
        checkpoints=checkpoints,
        suffix_rows=[],
        accounts={"account-2": SimpleNamespace(position_count=0)},
        checkpoint_for=lambda payload: _checkpoint(
            account="account-2",
            symbol="WUSDT",
            side="LONG",
            active_quantity=Decimal("0"),
        ),
    )

    summary = integrity["account-2"]
    assert summary.gaps is None
    assert summary.reasons == ("journal_suffix_incomplete",)
    assert summary.unknown_scopes == ("WUSDT:LONG",)


@pytest.mark.parametrize(
    ("active_quantity", "position_count", "expected_reason"),
    [
        (Decimal("1"), 0, "active_position_count_mismatch"),
    ],
)
def test_active_position_count_mismatch_is_reported(
    active_quantity: Decimal,
    position_count: int,
    expected_reason: str,
) -> None:
    heads = [
        _head(
            account="primary",
            symbol="WUSDT",
            side="LONG",
            revision=13,
            active_hint=True,
            unresolved=False,
        )
    ]
    checkpoints = [
        _checkpoint_row("primary", "WUSDT", "LONG"),
    ]
    session = _Session(heads, checkpoints, [])
    with patch(
        "crypto_momentum_lab.operator_dashboard.fact_integrity_queries"
        ".PositionRecoveryCodec.decode_checkpoint",
        side_effect=lambda payload: _checkpoint(
            account="primary",
            symbol="WUSDT",
            side="LONG",
            active_quantity=active_quantity,
        ),
    ), patch(
        "crypto_momentum_lab.operator_dashboard.fact_integrity_queries"
        ".recovery_checkpoint_head_binding",
        return_value={"checkpoint_id": "cp-1"},
    ):
        import asyncio

        integrity = asyncio.run(
            load_fact_integrity(
                _Sessions(session),
                accounts={"primary": SimpleNamespace(position_count=position_count)},
                now=NOW,
            )
        )

    summary = integrity["primary"]
    assert summary.gaps is None
    assert expected_reason in summary.reasons


def test_integrity_details_name_the_missing_evidence_and_scope() -> None:
    from crypto_momentum_lab.operator_dashboard.fact_integrity_queries import (
        FactIntegritySummary,
    )
    from crypto_momentum_lab.operator_dashboard.overview_queries import (
        _fact_integrity_details,
    )

    assert (
        _fact_integrity_details(
            FactIntegritySummary(gaps=0, observed_at=NOW)
        )
        is None
    )
    # No proof produced at all keeps the default "unconfirmed" wording.
    assert _fact_integrity_details(None) is None
    unknown = _fact_integrity_details(
        FactIntegritySummary(
            gaps=None,
            observed_at=None,
            reasons=("journal_suffix_incomplete",),
            unknown_scopes=("WUSDT:LONG",),
        )
    )
    assert unknown == (
        "fact_integrity_journal_suffix_incomplete_scope=WUSDT:LONG"
    )


def test_integrity_details_report_closed_object_evidence_gaps() -> None:
    from crypto_momentum_lab.operator_dashboard.fact_integrity_queries import (
        FactIntegritySummary,
    )
    from crypto_momentum_lab.operator_dashboard.overview_queries import (
        _fact_integrity_details,
    )

    # Current risk is proven while a closed object still lacks suffix evidence.
    assert _fact_integrity_details(
        FactIntegritySummary(
            gaps=0,
            observed_at=NOW,
            historical_reasons=("journal_suffix_incomplete",),
        )
    ) == "fact_integrity_historical_journal_suffix_incomplete"
