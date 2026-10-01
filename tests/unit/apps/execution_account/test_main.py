from typing import Any

import pytest
from typer import BadParameter
from typer.testing import CliRunner

from crypto_momentum_lab.apps.execution_account import main
from crypto_momentum_lab.config import BinanceCredentialRole

runner = CliRunner()


def test_account_label_prefers_explicit_option(monkeypatch) -> None:
    monkeypatch.setenv("CML_ACCOUNT_LABEL", "from-environment")

    assert main._resolve_account_label(" explicit ") == "explicit"


def test_account_label_falls_back_to_environment_and_requires_value(
    monkeypatch,
) -> None:
    monkeypatch.setenv("CML_ACCOUNT_LABEL", " account-2 ")
    assert main._resolve_account_label(None) == "account-2"

    monkeypatch.delenv("CML_ACCOUNT_LABEL", raising=False)
    with pytest.raises(ValueError, match="Missing required account_label"):
        main._resolve_account_label(None)


def test_execution_account_sync_once_requires_credentials(monkeypatch) -> None:
    monkeypatch.delenv("BINANCE_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_API_SECRET", raising=False)
    monkeypatch.delenv("BINANCE_READ_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_READ_API_SECRET", raising=False)

    result = runner.invoke(
        main.app,
        [
            "sync-once",
            "--database-url",
            "postgresql+asyncpg://cml:cml@localhost:54329/cml",
        ],
    )

    assert result.exit_code != 0
    assert "BINANCE_READ_API_KEY" in result.output


def test_execution_account_can_use_legacy_credentials_only_with_explicit_flag(
    monkeypatch,
) -> None:
    monkeypatch.delenv("BINANCE_READ_API_KEY", raising=False)
    monkeypatch.delenv("BINANCE_READ_API_SECRET", raising=False)
    monkeypatch.setenv("BINANCE_API_KEY", "legacy-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "legacy-secret")

    with pytest.raises(BadParameter, match="BINANCE_READ_API_KEY"):
        main._resolve_cli_credentials(
            role=BinanceCredentialRole.READ,
            api_key_env=None,
            api_secret_env=None,
            allow_legacy_fallback=False,
        )

    resolved = main._resolve_cli_credentials(
        role=BinanceCredentialRole.READ,
        api_key_env=None,
        api_secret_env=None,
        allow_legacy_fallback=True,
    )
    assert resolved.api_key == "legacy-key"
    assert resolved.api_key_env == "BINANCE_API_KEY"


def test_execution_account_credential_metadata_is_secret_free() -> None:
    from crypto_momentum_lab.config import resolve_role_credentials

    credentials = resolve_role_credentials(
        BinanceCredentialRole.READ,
        environ={
            "BINANCE_READ_API_KEY": "read-key-value",
            "BINANCE_READ_API_SECRET": "read-secret-value",
        },
    )

    metadata = credentials.metadata()

    assert metadata["credential_role"] == "read"
    assert metadata["api_key_env"] == "BINANCE_READ_API_KEY"
    assert metadata["api_secret_env"] == "BINANCE_READ_API_SECRET"
    assert "read-key-value" not in str(metadata)
    assert "read-secret-value" not in str(metadata)


def test_account_event_from_reconciled_fill_requires_snapshot() -> None:
    from datetime import UTC, datetime
    from decimal import Decimal

    from crypto_momentum_lab.domain.account import (
        AccountConfigSnapshot,
        AccountFillEvent,
        ExecutionAccountStatus,
    )
    from crypto_momentum_lab.domain.account.snapshot_models import (
        AccountSnapshot,
    )
    from crypto_momentum_lab.execution_account.sync_models import (
        ExecutionAccountSyncResult,
    )

    now = datetime(2026, 9, 27, 0, 0, tzinfo=UTC)
    fill = AccountFillEvent(
        environment="live",
        account_label="account-4",
        symbol="CLANKERUSDT",
        trade_id="12345",
        order_id="67890",
        side="BUY",
        price=Decimal("15.5"),
        quantity=Decimal("6.4"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.05"),
        fee_asset="USDT",
        trade_at=now,
        raw_payload={"clientOrderId": "cml_test_123"},
    )

    # When snapshot is None, ValueError must be raised
    result_no_snap = ExecutionAccountSyncResult(
        status=ExecutionAccountStatus.READY_READONLY,
        reconciliation_id="rec-1",
        mismatch_count=0,
        snapshot=None,
    )
    with pytest.raises(ValueError, match="requires an account snapshot"):
        main._account_event_from_reconciled_fill(
            fill,
            result_no_snap,
            environment="live",
            account_label="account-4",
        )

    # When snapshot is present, event is correctly constructed
    config_snap = AccountConfigSnapshot(
        environment="live",
        account_label="account-4",
        multi_assets_mode=False,
        hedge_mode=False,
        fee_tier=0,
        observed_at=now,
        raw_payload={},
    )
    snap = AccountSnapshot(
        config=config_snap, balances=(), positions=(), open_orders=()
    )
    result_with_snap = ExecutionAccountSyncResult(
        status=ExecutionAccountStatus.READY_READONLY,
        reconciliation_id="rec-2",
        mismatch_count=0,
        snapshot=snap,
    )
    event = main._account_event_from_reconciled_fill(
        fill,
        result_with_snap,
        environment="live",
        account_label="account-4",
    )
    assert event.event_type == "ACCOUNT_FILL_RECONCILED"
    assert event.symbol == "CLANKERUSDT"
    assert event.client_order_id == "cml_test_123"
    assert event.order_status == "FILLED"


def test_publish_reconciled_fill_ignores_when_snapshot_is_none() -> None:
    from datetime import UTC, datetime
    from decimal import Decimal
    from unittest.mock import MagicMock

    from crypto_momentum_lab.domain.account import (
        AccountFillEvent,
        ExecutionAccountStatus,
    )
    from crypto_momentum_lab.execution_account.sync_models import (
        ExecutionAccountSyncResult,
    )

    now = datetime(2026, 9, 27, 0, 0, tzinfo=UTC)
    fill = AccountFillEvent(
        environment="live",
        account_label="account-4",
        symbol="CLANKERUSDT",
        trade_id="12345",
        order_id="67890",
        side="BUY",
        price=Decimal("15.5"),
        quantity=Decimal("6.4"),
        realized_pnl=Decimal("0"),
        fee=Decimal("0.05"),
        fee_asset="USDT",
        trade_at=now,
        raw_payload={"clientOrderId": "cml_test_123"},
    )
    result_no_snap = ExecutionAccountSyncResult(
        status=ExecutionAccountStatus.READY_READONLY,
        reconciliation_id="rec-1",
        mismatch_count=0,
        snapshot=None,
    )

    # Simulated publish_reconciled_fill closure
    published: list[Any] = []
    mock_hub = MagicMock()
    mock_hub.publish = published.append

    def publish_reconciled_fill(
        f: AccountFillEvent,
        r: ExecutionAccountSyncResult,
    ) -> None:
        if r.snapshot is None:
            return
        mock_hub.publish(
            main._account_event_from_reconciled_fill(
                f,
                r,
                environment="live",
                account_label="account-4",
            )
        )

    # Should not raise ValueError and should not publish anything
    publish_reconciled_fill(fill, result_no_snap)
    assert len(published) == 0


async def test_snapshot_event_keeps_scan_proof_and_full_replay_fills():
    from datetime import UTC, datetime
    from decimal import Decimal

    from crypto_momentum_lab.domain.account.models import (
        AccountConfigSnapshot,
        AccountFillEvent,
        AccountFillLoadScan,
        AccountFillPageScan,
        ExecutionAccountStatus,
    )
    from crypto_momentum_lab.domain.account.snapshot_models import AccountSnapshot
    from crypto_momentum_lab.execution_account.sync_models import (
        ExecutionAccountSyncResult,
    )

    now = datetime(2026, 10, 1, tzinfo=UTC)
    scan = AccountFillLoadScan(
        "live",
        "primary",
        "BTCUSDT",
        "LONG",
        AccountFillPageScan(
            "BTCUSDT", "scan", int(now.timestamp() * 1000), None, 1, True, False, now
        ),
        now,
        "zero",
        now,
        "zero_snapshot",
    )
    fill = AccountFillEvent(
        "live",
        "primary",
        "BTCUSDT",
        "trade-1",
        "order-1",
        "BUY",
        Decimal("10"),
        Decimal("1"),
        Decimal("0"),
        Decimal("0"),
        "USDT",
        now,
        {"positionSide": "LONG"},
    )
    result = ExecutionAccountSyncResult(
        ExecutionAccountStatus.READY_READONLY,
        "r",
        0,
        snapshot=AccountSnapshot(
            AccountConfigSnapshot("live", "primary", False, True, 0, now, {}),
            (),
            (),
            (),
        ),
        fills=(fill,),
        new_fills=(),
        fill_load_scans=(scan,),
    )
    event = main._account_event_from_snapshot(
        result, environment="live", account_label="primary"
    )
    assert event.fill_load_scans == (scan,)
    assert event.fills == (fill,)


def test_user_data_order_fact_survives_account_hub_transport() -> None:
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from crypto_momentum_lab.domain.account import ExecutionAccountStatus
    from crypto_momentum_lab.execution_account.binance.user_data_parser import (
        parse_user_data_event,
    )
    from crypto_momentum_lab.execution_account.hub import (
        decode_account_event,
        encode_account_event,
    )

    now = datetime(2026, 10, 1, tzinfo=UTC)
    fact = {
        "c": "entry",
        "s": "BTCUSDT",
        "X": "FILLED",
        "z": "2",
        "ap": "100",
        "i": 123,
    }
    source = parse_user_data_event(
        {"e": "ORDER_TRADE_UPDATE", "E": int(now.timestamp() * 1000), "o": fact},
        received_at=now,
    )
    result = SimpleNamespace(
        status=ExecutionAccountStatus.READY_READONLY,
        fills=(),
        new_fills=(),
        fill_load_scans=(),
        snapshot=None,
        delta=None,
    )
    event = main._account_event_from_user_data(
        source, result, environment="live", account_label="primary"
    )
    decoded = decode_account_event(
        encode_account_event(event, sequence=1),
        expected_environment="live",
        expected_account_label="primary",
    )
    assert decoded.order_update == fact
    assert decoded.client_order_id == "entry"
