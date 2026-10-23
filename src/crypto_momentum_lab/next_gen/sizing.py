"""Account-scoped entry sizing; only daily and cycle anchors are durable."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from crypto_momentum_lab.live_rollout.runtime_manifest import (
        LiveRuntimeExecutionInputs,
    )
    from crypto_momentum_lab.next_gen.engine import AccountState


class EntrySizingUnavailable(ValueError):
    """The configured sizing rule cannot currently quote an entry amount."""


@dataclass(frozen=True, slots=True)
class EntrySizingSnapshot:
    mode: str
    equity_fraction: Decimal
    equity: Decimal
    sampled_at: datetime

    @property
    def notional(self) -> Decimal:
        return self.equity * self.equity_fraction


class EntrySizer:
    """Choose each new entry's notional without changing existing positions.

    Daily anchors roll at UTC midnight on the first available account/market
    observation. Flat anchors can change only while the entire account has no
    positions or unconfirmed orders. Both survive a process restart.
    """

    def __init__(
        self, state: AccountState, config: LiveRuntimeExecutionInputs
    ) -> None:
        self._state = state
        self._config = config
        self._snapshot: EntrySizingSnapshot | None = None
        self._loaded = False
        self._lock = asyncio.Lock()

    def _equity(self) -> Decimal:
        state = self._state
        if not state.ws_connected or state.private_stream_gap:
            raise EntrySizingUnavailable("private_account_state_unavailable")
        if state.multi_assets_mode is not False:
            raise EntrySizingUnavailable("single_asset_account_not_confirmed")
        wallet = state.wallet_balances.get("USDT")
        if wallet is None:
            raise EntrySizingUnavailable("usdt_wallet_unavailable")
        equity = wallet
        for position in state.exchange_positions.values():
            price = state.latest_prices.get(position.symbol)
            entry_price = position.entry_price
            if price is None or not price.is_finite() or price <= 0:
                raise EntrySizingUnavailable(
                    f"position_price_unavailable:{position.symbol}"
                )
            if entry_price is None or not entry_price.is_finite() or entry_price <= 0:
                raise EntrySizingUnavailable(
                    f"position_cost_unavailable:{position.symbol}"
                )
            # Binance positionAmt is signed, including SHORT positions.
            equity += position.position_amt * (price - entry_price)
        if not equity.is_finite() or equity <= 0:
            raise EntrySizingUnavailable("account_equity_not_positive")
        return equity

    def _is_flat(self) -> bool:
        state = self._state
        return (
            not state.exchange_positions
            and not any(batch.quantity > 0 for batch in state.batches.values())
            and not any(not order.is_terminal for order in state.orders.values())
        )

    async def refresh(self, now: datetime) -> None:
        """Maintain daily/cycle anchors; unchanged anchors do not write to DB."""
        config = self._config
        if config.sizing_mode in {"fixed", "per_trade"}:
            return
        fraction = config.equity_fraction
        if fraction is None:
            raise EntrySizingUnavailable("equity_fraction_not_configured")
        repository = self._state.db
        if repository is None:
            raise EntrySizingUnavailable("sizing_repository_unavailable")
        now = now.astimezone(UTC)
        async with self._lock:
            if not self._loaded:
                saved = await repository.async_get_entry_sizing(
                    self._state.environment, self._state.account_label
                )
                if (
                    saved is not None
                    and saved.mode == config.sizing_mode
                    and saved.equity_fraction == fraction
                ):
                    self._snapshot = saved
                self._loaded = True
            previous = self._snapshot
            if config.sizing_mode == "daily":
                if (
                    previous is not None
                    and previous.sampled_at.astimezone(UTC).date() >= now.date()
                ):
                    return
            elif not self._is_flat():
                if previous is None:
                    raise EntrySizingUnavailable("flat_sizing_requires_empty_account")
                return
            equity = self._equity()
            if (
                config.sizing_mode == "flat_reset"
                and previous is not None
                and previous.equity == equity
            ):
                return
            snapshot = EntrySizingSnapshot(config.sizing_mode, fraction, equity, now)
            await repository.async_save_entry_sizing(
                self._state.environment, self._state.account_label, snapshot
            )
            self._snapshot = snapshot

    async def entry_notional(self, now: datetime) -> Decimal:
        config = self._config
        if config.sizing_mode == "fixed":
            return config.target_notional
        if config.sizing_mode == "per_trade":
            fraction = config.equity_fraction
            if fraction is None:
                raise EntrySizingUnavailable("equity_fraction_not_configured")
            self._snapshot = EntrySizingSnapshot(
                config.sizing_mode, fraction, self._equity(), now.astimezone(UTC)
            )
        else:
            await self.refresh(now)
        if self._snapshot is None:
            raise EntrySizingUnavailable("entry_sizing_anchor_unavailable")
        return self._snapshot.notional

    def describe(self) -> dict[str, Any]:
        """Expose the actual sizing basis for status and entry signal records."""
        config = self._config
        snapshot = self._snapshot
        notional = (
            config.target_notional
            if config.sizing_mode == "fixed"
            else None if snapshot is None else snapshot.notional
        )
        return {
            "mode": config.sizing_mode,
            "equity_fraction": (
                None if config.equity_fraction is None else str(config.equity_fraction)
            ),
            "target_notional": None if notional is None else str(notional),
            "basis_equity": None if snapshot is None else str(snapshot.equity),
            "sampled_at": None if snapshot is None else snapshot.sampled_at.isoformat(),
        }
