"""Offline probes for entry-fact ordering and exit wakeups; no network or DB.

Run from the checkout using its .venv Python. The persistence fixture is an
in-memory unit of work; Book, context classification and exit scheduling are
the production implementations. Exit code 1 means an invariant still fails.
This diagnostic is intentionally outside the normal pytest suite.
"""

import asyncio
import json
import sys
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from crypto_momentum_lab.domain.account import AccountPositionSnapshot
from crypto_momentum_lab.domain.execution.evidence_models import (
    ExecutionCumulativeOrderReport,
)
from crypto_momentum_lab.domain.execution.execution_book import ExecutionBook
from crypto_momentum_lab.domain.execution.order_state import (
    ExchangeOrderEvent,
    ExchangeOrderState,
)
from crypto_momentum_lab.domain.execution.trade_command import (
    TradeCommand,
    TradeCommandType,
)
from crypto_momentum_lab.domain.strategy import EntryType, StrategySide
from crypto_momentum_lab.live_rollout.context import exit_position_block_reason
from crypto_momentum_lab.live_rollout.exit_channels import LiveExitChannelRuntime
from crypto_momentum_lab.live_rollout.postgres_runtime import (
    PostgresLiveContextProvider,
)
from tests.unit.execution.test_terminal_settlement import (
    NOW,
    SCOPE,
    ObservationUnitOfWork,
    evidence,
    fill,
)
from tests.unit.live_rollout.test_postgres_runtime import _runtime_context


async def probe_context(case: str) -> dict:
    book = ExecutionBook(execution_unit_of_work=ObservationUnitOfWork())
    book._persistence_failed = False
    await book.observe(evidence("initial"))
    if case != "external_position":
        command = TradeCommand(
            "entry",
            SCOPE.to_position_key(),
            TradeCommandType.ENTRY,
            StrategySide.LONG,
            EntryType.MARKET,
            Decimal("2"),
            created_at=NOW,
        )
        book.register_prepared_command(command, SCOPE)
        state = (
            ExchangeOrderState.ACKNOWLEDGED
            if case == "snapshot_before_trade"
            else ExchangeOrderState.FILLED
        )
        await book.observe(
            evidence(
                "entry-report",
                order_event=ExchangeOrderEvent(
                    "entry-report",
                    "entry",
                    state,
                    NOW,
                    "exchange-1",
                    {},
                ),
                cumulative_order=(
                    None
                    if state == ExchangeOrderState.ACKNOWLEDGED
                    else ExecutionCumulativeOrderReport(
                        "entry",
                        Decimal("2"),
                        Decimal("200"),
                        NOW,
                    )
                ),
            )
        )
    snapshot = AccountPositionSnapshot(
        "live",
        "primary",
        "BTCUSDT",
        "LONG",
        Decimal("2"),
        Decimal("100"),
        Decimal("100"),
        Decimal("0"),
        Decimal("200"),
        5,
        "cross",
        NOW,
        {},
    )
    if case == "snapshot_before_trade":
        await book.observe(evidence("position-before-trade", snapshot=snapshot))
    if case == "trade_applied":
        await book.observe(
            evidence(
                "real-trade",
                fill=replace(fill("buy", "2", entry=True), order_id="exchange-1"),
            )
        )

    # These are the same narrow provider setup and context fixture used by the
    # existing unit suite. No DB load or production account is involved.
    provider = object.__new__(PostgresLiveContextProvider)
    provider._account_label = "primary"
    provider._execution_book = book
    provider._cache_epoch = 7
    context = replace(
        _runtime_context(),
        context_epoch=7,
        open_position_symbols=frozenset({"BTCUSDT"}),
        pending_position_symbols=frozenset(),
        unmanaged_position_symbols=frozenset(),
        account_snapshot=SimpleNamespace(positions=(snapshot,)),
    )
    provider._cached_context = context
    result = await provider._with_execution_book(
        context,
        SimpleNamespace(bucket_end=NOW),
    )
    if case == "cache_fact_update":
        assert result.pending_position_symbols == frozenset({"BTCUSDT"})
        await book.observe(
            evidence(
                "real-trade",
                fill=replace(fill("buy", "2", entry=True), order_id="exchange-1"),
            )
        )
        result = await provider._with_execution_book(
            context,
            SimpleNamespace(bucket_end=NOW),
        )
    view = await book.read(SCOPE)
    if case == "external_position":
        passed = "BTCUSDT" in result.unmanaged_position_symbols
    elif case in {"trade_applied", "cache_fact_update"}:
        passed = (
            bool(result.managed_positions) and not result.unmanaged_position_symbols
        )
    else:
        passed = (
            "BTCUSDT" in result.pending_position_symbols
            and "BTCUSDT" not in result.unmanaged_position_symbols
        )
    return {
        "case": case,
        "passed": passed,
        "known_command_waits_for_trades": book.command_requires_recovery("entry"),
        "pending": sorted(result.pending_position_symbols),
        "unmanaged": sorted(result.unmanaged_position_symbols),
        "managed_count": len(result.managed_positions),
        "exit_block": exit_position_block_reason(result, "BTCUSDT"),
        "view_health": view.health_status.value,
        "is_comparable": view.is_comparable,
        "diagnostics": view.diagnostics,
    }


async def probe_quote_wakeup() -> dict:
    calls = []

    class Daemon:
        managed_position_symbols = frozenset({"BTCUSDT"})

        async def process_market_quote(self, quote, state):
            calls.append(quote.price)
            return "pending_live_positions:BTCUSDT" if len(calls) == 1 else None

    runtime = LiveExitChannelRuntime(
        daemon=Daemon(),
        latest_market_quotes=SimpleNamespace(observe=lambda quote: None),
        latest_market_states=SimpleNamespace(
            for_symbols=lambda symbols: (SimpleNamespace(symbol="BTCUSDT"),),
        ),
        is_transient_error=lambda error: False,
    )

    async def quotes():
        yield SimpleNamespace(symbol="BTCUSDT", price=100)
        runtime.note_account_facts_changed()
        yield SimpleNamespace(symbol="BTCUSDT", price=101)

    await runtime.run_quote_channel(source=quotes())
    return {
        "case": "facts_change_before_quote_retry_deadline",
        "passed": calls == [100, 101],
        "prices_evaluated": calls,
    }


async def main() -> int:
    results = [
        await probe_context(case)
        for case in (
            "receipt_before_trade",
            "snapshot_before_trade",
            "external_position",
            "trade_applied",
        )
    ]
    results.append(await probe_quote_wakeup())
    for result in results:
        print(json.dumps(result, ensure_ascii=False))
    failed = sum(not result["passed"] for result in results)
    print(f"{len(results) - failed} controls/probes passed, {failed} invariants failed")
    return int(failed > 0)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
