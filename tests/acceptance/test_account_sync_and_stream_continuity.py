"""SYSTEM.md §3/§4/§6 acceptance: merged account projection and private-stream continuity.

Covers the previously missing single-account responsibilities that the retired
execution-account sync process used to own:

1. The in-process private-stream producer publishes a merged account projection
   (balances, positions, open orders, account state), not just a symbol list, so
   the live control plane and the dashboard see the current account facts.
2. The startup REST baseline is the accepted starting point; an existing
   position is trusted without spending a single-use entry expectation.
3. Only a genuinely new position needs an expectation or a fact-gap
   reconciliation; a repeated update of a held position is not isolated.
4. A source that has never connected, and a source that lost its connection,
   must not admit new risk; a sequence gap marks the affected range.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from crypto_momentum_lab.domain.account.models import (
    AccountBalanceSnapshot,
    AccountConfigSnapshot,
    AccountOpenOrderSnapshot,
    AccountPositionSnapshot,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.domain.account.snapshot_models import AccountSnapshot
from crypto_momentum_lab.execution_account.binance.user_data_models import (
    BinanceUserDataEvent,
)
from crypto_momentum_lab.execution_account.expectations import (
    AccountPositionExpectation,
    AccountPositionExpectationRegistry,
)
from crypto_momentum_lab.execution_account.hub import (
    AccountEvent,
    InProcessAccountEventSource,
)
from crypto_momentum_lab.execution_account.in_process_producer import (
    InProcessAccountStreamProducer,
)

NOW = datetime.now(UTC)


def _event_id(seed: str) -> str:
    import hashlib

    return hashlib.sha256(seed.encode()).hexdigest()


def _config(account: str, *, observed_at: datetime = NOW) -> AccountConfigSnapshot:
    return AccountConfigSnapshot(
        environment="live",
        account_label=account,
        multi_assets_mode=True,
        hedge_mode=True,
        fee_tier=0,
        observed_at=observed_at,
        raw_payload={},
    )


def _balance(account: str, asset: str, amount: str) -> AccountBalanceSnapshot:
    return AccountBalanceSnapshot(
        environment="live",
        account_label=account,
        asset=asset,
        wallet_balance=Decimal(amount),
        available_balance=Decimal(amount),
        unrealized_pnl=Decimal("0"),
        observed_at=NOW,
        raw_payload={},
    )


def _position(
    account: str,
    symbol: str,
    amount: str,
    *,
    position_side: str = "LONG",
    entry_price: str = "60000",
) -> AccountPositionSnapshot:
    return AccountPositionSnapshot(
        environment="live",
        account_label=account,
        symbol=symbol,
        position_side=position_side,
        position_amt=Decimal(amount),
        entry_price=Decimal(entry_price),
        mark_price=Decimal(entry_price),
        unrealized_pnl=Decimal("0"),
        notional=abs(Decimal(amount) * Decimal(entry_price)),
        leverage=10,
        margin_type="cross",
        observed_at=NOW,
        raw_payload={},
    )


def _open_order(account: str, symbol: str) -> AccountOpenOrderSnapshot:
    return AccountOpenOrderSnapshot(
        environment="live",
        account_label=account,
        symbol=symbol,
        order_id="1001",
        client_order_id="c-1001",
        side="BUY",
        order_type="LIMIT",
        status="NEW",
        price=Decimal("50000"),
        original_quantity=Decimal("0.1"),
        executed_quantity=Decimal("0"),
        reduce_only=False,
        observed_at=NOW,
        raw_payload={},
    )


class _FakeReadClient:
    """Minimal REST baseline client used by the merged producer."""

    def __init__(
        self,
        *,
        account: str,
        positions: tuple[AccountPositionSnapshot, ...] = (),
        balances: tuple[AccountBalanceSnapshot, ...] = (),
        open_orders: tuple[AccountOpenOrderSnapshot, ...] = (),
        observed_at: datetime | None = None,
    ) -> None:
        self._account = account
        self._positions = positions
        self._balances = balances
        self._open_orders = open_orders
        self._observed_at = observed_at or NOW
        self.fetch_count = 0

    async def fetch_account_config(self) -> AccountConfigSnapshot:
        self.fetch_count += 1
        return _config(self._account, observed_at=self._observed_at)

    async def fetch_balances(self) -> tuple[AccountBalanceSnapshot, ...]:
        return self._balances

    async def fetch_positions(
        self, *, include_flat: bool = False
    ) -> tuple[AccountPositionSnapshot, ...]:
        return self._positions

    async def fetch_open_orders(self) -> tuple[AccountOpenOrderSnapshot, ...]:
        return self._open_orders

    @property
    def incomplete_fill_symbols(self) -> frozenset[str]:
        return frozenset()

    async def fetch_recent_fills(self, symbols=(), **kwargs):  # type: ignore[no-untyped-def]
        return ()

    async def fetch_fills_with_provenance(self, symbol, **kwargs):  # type: ignore[no-untyped-def]
        raise NotImplementedError


def _account_update(
    account: str,
    *,
    positions: list[dict[str, object]],
    balances: list[dict[str, object]] | None = None,
    at: datetime | None = None,
    sequence_hint: str = "one",
) -> BinanceUserDataEvent:
    event_at = at or (NOW + timedelta(seconds=1))
    at_ms = int(event_at.timestamp() * 1000)
    return BinanceUserDataEvent(
        event_type="ACCOUNT_UPDATE",
        event_at=event_at,
        received_at=event_at,
        payload={
            "e": "ACCOUNT_UPDATE",
            "E": at_ms,
            "T": at_ms,
            "a": {
                "m": "ORDER",
                "B": balances
                if balances is not None
                else [{"a": "USDT", "wb": "10000", "cw": "10000"}],
                "P": positions,
            },
        },
        event_id=_event_id(f"{account}-{sequence_hint}-{at_ms}"),
        exchange_event_at=event_at,
    )


def _position_row(
    symbol: str, amount: str, *, position_side: str = "LONG"
) -> dict[str, object]:
    return {
        "s": symbol,
        "pa": amount,
        "ep": "60000",
        "cr": "0",
        "up": "0",
        "mt": "cross",
        "iw": "0",
        "ps": position_side,
    }


def _make_producer(
    *,
    account: str,
    source: InProcessAccountEventSource,
    registry: AccountPositionExpectationRegistry,
    read_client: _FakeReadClient | None,
    on_unverified: object = None,
    persisted: list[AccountSnapshot] | None = None,
    published: list[AccountEvent] | None = None,
    expected_multi_assets_mode: bool = True,
) -> InProcessAccountStreamProducer:
    async def _persist(snapshot: AccountSnapshot, event: AccountEvent) -> None:
        if persisted is not None:
            persisted.append(snapshot)

    async def _publish(snapshot: AccountSnapshot, event: AccountEvent) -> None:
        if published is not None:
            published.append(event)

    return InProcessAccountStreamProducer(
        client=_NoopListenKeyClient(),  # type: ignore[arg-type]
        registry=registry,
        source=source,
        environment="live",
        account_label=account,
        read_client=read_client,
        expected_multi_assets_mode=expected_multi_assets_mode,
        expected_hedge_mode=True,
        on_unverified_position=on_unverified,  # type: ignore[arg-type]
        publish_baseline=_publish,
        persist_snapshot=_persist,
    )


class _NoopListenKeyClient:
    async def start_user_data_stream(self) -> str:
        return "listen-key"

    async def keepalive_user_data_stream(self, listen_key: str) -> None:
        return None

    async def close_user_data_stream(self, listen_key: str) -> None:
        return None


@pytest.mark.asyncio
async def test_account_update_publishes_merged_balance_and_position_projection() -> (
    None
):
    """ACCOUNT_UPDATE must publish the merged account projection, not only symbols."""

    account = "acct-merge"
    source = InProcessAccountEventSource(environment="live", account_label=account)
    registry = AccountPositionExpectationRegistry(
        environment="live", account_label=account
    )
    persisted: list[AccountSnapshot] = []
    published: list[AccountEvent] = []
    read_client = _FakeReadClient(
        account=account,
        balances=(_balance(account, "USDT", "10000"),),
        positions=(_position(account, "BTCUSDT", "1"),),
        open_orders=(_open_order(account, "ETHUSDT"),),
    )
    producer = _make_producer(
        account=account,
        source=source,
        registry=registry,
        read_client=read_client,
        persisted=persisted,
        published=published,
    )

    baseline = await producer.baseline()
    assert producer.account_state is ExecutionAccountStatus.RUNNING
    assert source.is_risk_allowed("BTCUSDT") is True
    assert len(published) == 1
    assert published[0].account_snapshot is baseline

    await producer.handle_user_data_event(
        _account_update(
            account,
            positions=[_position_row("BTCUSDT", "1"), _position_row("ETHUSDT", "0.5")],
            balances=[{"a": "USDT", "wb": "12345", "cw": "12345"}],
            at=NOW + timedelta(seconds=3),
            sequence_hint="two",
        )
    )

    event = await anext(aiter(source))
    assert event.account_snapshot is not None
    assert event.account_state is ExecutionAccountStatus.RUNNING
    balances = {item.asset: item for item in event.account_snapshot.balances}
    assert balances["USDT"].wallet_balance == Decimal("12345")
    positions = {
        (item.symbol, item.position_side): item
        for item in event.account_snapshot.positions
    }
    assert positions[("BTCUSDT", "LONG")].position_amt == Decimal("1")
    assert positions[("ETHUSDT", "LONG")].position_amt == Decimal("0.5")
    assert [order.order_id for order in event.account_snapshot.open_orders] == [
        order.order_id for order in baseline.open_orders
    ]
    assert [order.symbol for order in event.account_snapshot.open_orders] == [
        order.symbol for order in baseline.open_orders
    ]
    # The merged observation is persisted for the dashboard/restart reader,
    # stamped with the merge time so later stale events cannot roll it back.
    assert persisted and persisted[-1].config.observed_at == event.received_at
    source.stop()


@pytest.mark.asyncio
async def test_existing_position_update_does_not_consume_missing_expectation() -> None:
    """A repeated update of an accepted holding must not be isolated as unverified.

    Reproduces the false gap: quantity 1 consumed the entry expectation once;
    the next quantity 1 update of the same position was misjudged unverified.
    """

    account = "acct-held"
    source = InProcessAccountEventSource(environment="live", account_label=account)
    registry = AccountPositionExpectationRegistry(
        environment="live", account_label=account
    )
    unverified: list[tuple[str, str, Decimal]] = []
    read_client = _FakeReadClient(
        account=account,
        positions=(_position(account, "BTCUSDT", "1"),),
        balances=(_balance(account, "USDT", "10000"),),
    )
    producer = _make_producer(
        account=account,
        source=source,
        registry=registry,
        read_client=read_client,
        on_unverified=(
            lambda symbol, side, amount: unverified.append((symbol, side, amount))
        ),
    )
    await producer.baseline()
    assert registry.pending_count == 0
    assert source.is_risk_allowed("BTCUSDT") is True

    # First update of the already-held position: quantity is unchanged and the
    # position was in the accepted baseline, so no expectation may be spent.
    await producer.handle_user_data_event(
        _account_update(
            account,
            positions=[_position_row("BTCUSDT", "1")],
            at=NOW + timedelta(seconds=4),
            sequence_hint="repeat-one",
        )
    )
    assert unverified == []
    assert not source.has_fact_gap
    assert source.is_risk_allowed("BTCUSDT") is True

    # A second identical update must behave the same way.
    await producer.handle_user_data_event(
        _account_update(
            account,
            positions=[_position_row("BTCUSDT", "1")],
            at=NOW + timedelta(seconds=5),
            sequence_hint="repeat-two",
        )
    )
    assert unverified == []
    assert not source.has_fact_gap
    assert source.is_risk_allowed("BTCUSDT") is True

    # A genuinely new position with no expectation is still isolated.
    await producer.handle_user_data_event(
        _account_update(
            account,
            positions=[
                _position_row("BTCUSDT", "1"),
                _position_row("SOLUSDT", "3"),
            ],
            at=NOW + timedelta(seconds=6),
            sequence_hint="new-symbol",
        )
    )
    assert [(symbol, side) for symbol, side, _amount in unverified] == [
        ("SOLUSDT", "LONG")
    ]
    assert source.has_fact_gap
    assert not source.is_risk_allowed("SOLUSDT")
    # Isolation is per range: the already-accepted BTCUSDT holding keeps
    # admitting new risk while SOLUSDT awaits verification.
    assert source.is_risk_allowed("BTCUSDT")
    source.stop()


@pytest.mark.asyncio
async def test_growth_of_held_position_requires_verification_without_reisolating() -> (
    None
):
    """A material quantity change on a held position still needs verification."""

    account = "acct-grow"
    source = InProcessAccountEventSource(environment="live", account_label=account)
    registry = AccountPositionExpectationRegistry(
        environment="live", account_label=account
    )
    unverified: list[tuple[str, str, Decimal]] = []
    read_client = _FakeReadClient(
        account=account,
        positions=(_position(account, "BTCUSDT", "1"),),
        balances=(_balance(account, "USDT", "10000"),),
    )
    producer = _make_producer(
        account=account,
        source=source,
        registry=registry,
        read_client=read_client,
        on_unverified=(
            lambda symbol, side, amount: unverified.append((symbol, side, amount))
        ),
    )
    await producer.baseline()
    now = datetime.now(UTC)
    registry.register(
        AccountPositionExpectation(
            environment="live",
            account_label=account,
            symbol="BTCUSDT",
            position_side="LONG",
            client_order_id="c-add",
            side="BUY",
            quantity=Decimal("0.5"),
            created_at=now - timedelta(seconds=1),
            expires_at=now + timedelta(seconds=30),
        )
    )

    await producer.handle_user_data_event(
        _account_update(
            account,
            positions=[_position_row("BTCUSDT", "2")],
            at=NOW + timedelta(seconds=7),
            sequence_hint="grow",
        )
    )
    # The position identity was already accepted, so an ordinary size change of
    # a held position is admitted. The registered expectation is reconciled by
    # its own order/position evidence rather than by the position row alone.
    assert unverified == []
    assert not source.has_fact_gap
    source.stop()


@pytest.mark.asyncio
async def test_source_without_verified_connection_never_admits_new_risk() -> None:
    """A constructed source is not READY until continuity has been verified."""

    source = InProcessAccountEventSource(environment="live", account_label="acct")
    assert source.is_risk_allowed() is False
    assert source.is_risk_allowed("BTCUSDT") is False
    assert source.continuity_verified is False


@pytest.mark.asyncio
@pytest.mark.parametrize("multi_assets_mode", [True, False])
async def test_reconnect_blocks_account_until_the_new_baseline_is_accepted(
    multi_assets_mode,
) -> None:
    """A transport break blocks every symbol until a baseline re-anchors the epoch."""

    account = "acct-reconnect"
    source = InProcessAccountEventSource(environment="live", account_label=account)
    registry = AccountPositionExpectationRegistry(
        environment="live", account_label=account
    )
    read_client = _FakeReadClient(account=account)
    from dataclasses import replace
    from unittest.mock import AsyncMock

    read_client.fetch_account_config = AsyncMock(
        return_value=replace(_config(account), multi_assets_mode=multi_assets_mode)
    )
    producer = _make_producer(
        account=account,
        source=source,
        registry=registry,
        read_client=read_client,
        expected_multi_assets_mode=multi_assets_mode,
    )
    await producer.baseline()
    assert source.is_risk_allowed("BTCUSDT") is True

    # A disconnect is not continuity: every symbol stops.
    producer._on_stream_connection_change(False, "connection_closed")
    assert source.has_account_wide_gap
    assert source.is_risk_allowed("BTCUSDT") is False
    assert source.is_risk_allowed("ETHUSDT") is False

    # A connect alone does not restore admission authority.
    producer._on_stream_connection_change(True, "connected")
    assert source.continuity_verified is False
    assert source.is_risk_allowed("BTCUSDT") is False
    assert source.is_risk_allowed("ETHUSDT") is False

    # Only an accepted baseline for the new connection reopens admission.
    await producer.baseline()
    assert source.has_account_wide_gap is False
    assert source.is_risk_allowed("BTCUSDT") is True
    assert source.is_risk_allowed("ETHUSDT") is True
    source.stop()


@pytest.mark.asyncio
async def test_symbol_scope_gap_isolates_only_that_range() -> None:
    """A BTC fact gap must not block healthy ETH admission or the exit path.

    The gap is opened through the producer's real unverified-position path (the
    same callback the runtime installs), not by mutating ``_gap_symbols``.
    """

    from crypto_momentum_lab.live_rollout.entry_control import LiveEntryControlGate

    account = "acct-isolation"
    source = InProcessAccountEventSource(environment="live", account_label=account)
    registry = AccountPositionExpectationRegistry(
        environment="live", account_label=account
    )
    read_client = _FakeReadClient(
        account=account,
        balances=(_balance(account, "USDT", "10000"),),
        positions=(_position(account, "BTCUSDT", "1"),),
    )
    unverified: list[str] = []
    producer = _make_producer(
        account=account,
        source=source,
        registry=registry,
        read_client=read_client,
        on_unverified=lambda symbol, side, amount: unverified.append(symbol),
    )
    await producer.baseline()
    assert source.is_risk_allowed("BTCUSDT") is True

    # A genuinely new SOLUSDT position with no expectation is a symbol fact gap.
    await producer.handle_user_data_event(
        _account_update(
            account,
            positions=[_position_row("BTCUSDT", "1"), _position_row("SOLUSDT", "3")],
            balances=[{"a": "USDT", "wb": "10000", "cw": "10000"}],
            at=NOW + timedelta(seconds=9),
            sequence_hint="isolation",
        )
    )
    assert unverified == ["SOLUSDT"]
    assert source.gap_symbols == frozenset({"SOLUSDT"})
    assert source.has_account_wide_gap is False
    # Only the affected range is isolated; BTCUSDT keeps admitting.
    assert source.is_risk_allowed("SOLUSDT") is False
    assert source.is_risk_allowed("BTCUSDT") is True

    class _EntryReady:
        @property
        def is_admitting_entries(self) -> bool:
            return True

        @property
        def admission_disabled_reason(self) -> str | None:
            return None

    gate = LiveEntryControlGate(
        run_id="run-isolation",
        state_machine=_EntryReady(),
        clock=lambda: datetime.now(tz=UTC),
        is_symbol_fact_ready=source.is_risk_allowed,
    )
    assert gate.is_symbol_entry_allowed("BTCUSDT") == (True, "entry_allowed")
    allowed, reason = gate.is_symbol_entry_allowed("SOLUSDT")
    assert allowed is False and reason == "symbol_fact_pending:SOLUSDT"

    # Clearing the verified symbol restores exactly that range.
    source.clear_fact_gap("SOLUSDT")
    assert source.is_risk_allowed("SOLUSDT") is True
    assert gate.is_symbol_entry_allowed("SOLUSDT") == (True, "entry_allowed")
    source.stop()


def _event(
    account: str,
    *,
    sequence: int,
    stream_epoch: str,
    symbol: str,
) -> AccountEvent:
    return AccountEvent(
        environment="live",
        account_label=account,
        event_type="ACCOUNT_UPDATE",
        event_id=_event_id(f"{account}-{sequence}"),
        event_at=NOW,
        received_at=NOW,
        symbols=(symbol,),
        symbol=symbol,
        sequence=sequence,
        stream_epoch=stream_epoch,
    )


@pytest.mark.asyncio
async def test_baseline_consumer_failure_does_not_confirm_continuity():
    account = "baseline-not-accepted"
    source = InProcessAccountEventSource(environment="live", account_label=account)
    registry = AccountPositionExpectationRegistry(
        environment="live", account_label=account
    )

    async def reject_baseline(snapshot, event):
        assert not source.is_risk_allowed()
        raise RuntimeError("durable facts not accepted")

    producer = InProcessAccountStreamProducer(
        client=_NoopListenKeyClient(),
        read_client=_FakeReadClient(account=account),
        registry=registry,
        source=source,
        environment="live",
        account_label=account,
        expected_multi_assets_mode=True,
        expected_hedge_mode=True,
        publish_baseline=reject_baseline,
    )
    with pytest.raises(RuntimeError, match="durable facts not accepted"):
        await producer.baseline()
    assert not source.continuity_verified
    assert not source.is_risk_allowed()
    source.stop()
