from datetime import UTC, datetime
from decimal import Decimal

from crypto_momentum_lab.domain.account import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.execution_account.binance.user_data import (
    parse_user_data_event,
)
from crypto_momentum_lab.execution_account.daemon import (
    UserDataAccountSyncConfig,
    UserDataAccountSyncDaemon,
)
from crypto_momentum_lab.execution_account.sync import (
    AccountSnapshot,
    ExecutionAccountSyncResult,
)


class FakeStream:
    def __init__(self) -> None:
        self.handler = None
        self.stop_count = 0

    def set_handler(self, on_event) -> None:
        self.handler = on_event

    async def run(self) -> None:
        return None

    async def stop(self) -> None:
        self.stop_count += 1


class FakeService:
    def __init__(self, snapshot: AccountSnapshot) -> None:
        self.snapshot = snapshot
        self.sync_calls = 0
        self.persisted = []
        self.heartbeats = []

    async def sync_once(
        self,
        *,
        observed_at,
        publish_transient_states,
        include_fills,
    ):
        self.sync_calls += 1
        return ExecutionAccountSyncResult(
            status=ExecutionAccountStatus.READY_READONLY,
            reconciliation_id=f"reconciliation-{self.sync_calls}",
            mismatch_count=0,
            snapshot=self.snapshot,
        )

    async def persist_user_data_event(self, *, snapshot, event, fills=()):
        self.persisted.append((snapshot, event, fills))
        return ExecutionAccountSyncResult(
            status=ExecutionAccountStatus.READY_READONLY,
            reconciliation_id="event-reconciliation",
            mismatch_count=0,
            snapshot=snapshot,
        )

    async def publish_user_data_heartbeat(self, *, observed_at):
        self.heartbeats.append(observed_at)


async def test_user_data_daemon_persists_events_and_reconciles_unknown_state() -> None:
    snapshot = _snapshot()
    service = FakeService(snapshot)
    daemon = UserDataAccountSyncDaemon(
        service=service,
        stream=FakeStream(),
        config=UserDataAccountSyncConfig(
            rest_reconciliation_interval_seconds=60,
            heartbeat_interval_seconds=30,
        ),
        clock=lambda: datetime(2026, 7, 4, 0, 0, 2, tzinfo=UTC),
    )

    await daemon._reconcile(include_fills=True)
    await daemon._on_event(
        parse_user_data_event(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 1783123201000,
                "a": {
                    "B": [{"a": "USDT", "wb": "101", "cw": "81"}],
                    "P": [],
                },
            },
            received_at=datetime(2026, 7, 4, 0, 0, 1, tzinfo=UTC),
        )
    )
    assert len(service.persisted) == 1
    assert service.persisted[0][0].balances[0].wallet_balance == Decimal("101")

    await daemon._on_event(
        parse_user_data_event(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 1783123202000,
                "a": {
                    "B": [],
                    "P": [
                        {
                            "s": "ETHUSDT",
                            "pa": "1",
                            "ep": "3000",
                            "up": "0",
                            "ps": "BOTH",
                        }
                    ],
                },
            },
            received_at=datetime(2026, 7, 4, 0, 0, 2, tzinfo=UTC),
        )
    )
    assert service.sync_calls == 2

    await daemon._publish_heartbeat()
    assert len(service.heartbeats) == 1


def _snapshot() -> AccountSnapshot:
    observed_at = datetime(2026, 7, 4, 0, 0, tzinfo=UTC)
    return AccountSnapshot(
        config=AccountConfigSnapshot(
            environment="live",
            account_label="primary",
            multi_assets_mode=False,
            hedge_mode=False,
            can_trade=True,
            fee_tier=0,
            observed_at=observed_at,
            raw_payload={},
        ),
        balances=(
            AccountBalanceSnapshot(
                environment="live",
                account_label="primary",
                asset="USDT",
                wallet_balance=Decimal("100"),
                available_balance=Decimal("80"),
                unrealized_pnl=Decimal("0"),
                observed_at=observed_at,
                raw_payload={},
            ),
        ),
        positions=(),
        open_orders=(),
    )
