"""Unit tests for live_rollout/market_assembly.py."""

from __future__ import annotations

import asyncio
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crypto_momentum_lab.domain.strategy.position_exit import PositionExitMode
from crypto_momentum_lab.live_rollout.hub_cursor import LiveHubCursorState
from crypto_momentum_lab.live_rollout.market_assembly import (
    assemble_live_candle_sources,
    assemble_live_channel_sources,
    assemble_live_quote_volume,
    assemble_live_startup_market_buffer,
    build_live_market_state_stream,
)
from crypto_momentum_lab.live_rollout.runtime_session import ResourceOwnershipRegistry


@pytest.mark.asyncio
async def test_assemble_live_quote_volume_success() -> None:
    ownership = ResourceOwnershipRegistry(run_id="test_run")
    with (
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.WebSocketMarketQuoteVolumeSource"
        ) as mock_src_cls,
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.WebSocketQuoteVolumeProvider"
        ) as mock_provider_cls,
    ):
        mock_provider = MagicMock()
        mock_provider.start = AsyncMock()
        mock_provider.stop = AsyncMock()
        mock_provider_cls.return_value = mock_provider

        provider = await assemble_live_quote_volume(
            market_quote_volume_hub_url="wss://test-volume",
            market_environment="live",
            session_id="sess_123",
            ownership_registry=ownership,
        )

        assert provider is mock_provider
        mock_src_cls.assert_called_once_with(
            url="wss://test-volume",
            environment="live",
            consumer_id="live-volume:sess_123",
        )
        mock_provider.start.assert_awaited_once()


@pytest.mark.asyncio
async def test_assemble_live_quote_volume_failure_cleans_up() -> None:
    ownership = ResourceOwnershipRegistry(run_id="test_run")
    with (
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.WebSocketMarketQuoteVolumeSource"
        ),
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.WebSocketQuoteVolumeProvider"
        ) as mock_provider_cls,
    ):
        mock_provider = MagicMock()
        mock_provider.start = AsyncMock(
            side_effect=RuntimeError("connection failed")
        )
        mock_provider.stop = AsyncMock()
        mock_provider_cls.return_value = mock_provider

        with pytest.raises(RuntimeError, match="connection failed"):
            await assemble_live_quote_volume(
                market_quote_volume_hub_url="wss://test-volume",
                market_environment="live",
                session_id="sess_123",
                ownership_registry=ownership,
            )

        mock_provider.stop.assert_awaited_once()


def test_assemble_live_candle_sources_exit_mode_15m() -> None:
    ownership = ResourceOwnershipRegistry(run_id="test_run")
    with (
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.BinanceRestClosedCandle15mSource"
        ) as mock_src_cls,
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.BinanceClosedCandle15mFeed"
        ) as mock_feed_cls,
    ):
        mock_src = MagicMock()
        mock_feed = MagicMock()
        mock_src_cls.return_value = mock_src
        mock_feed_cls.return_value = mock_feed

        assembly = assemble_live_candle_sources(
            exit_mode=PositionExitMode.CANDLE_15M,
            base_url="https://fapi.binance.com",
            market_websocket_url="wss://fstream.binance.com",
            market_environment="live",
            session_id="sess_123",
            require_price_above_ema5=False,
            require_price_above_ema10=False,
            ownership_registry=ownership,
        )

        assert assembly.candle_source is mock_src
        assert assembly.closed_candle_feed is mock_feed
        assert assembly.ema_provider is None


def test_assemble_live_candle_sources_ema_only() -> None:
    ownership = ResourceOwnershipRegistry(run_id="test_run")
    with (
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.BinanceRestClosedCandle15mSource"
        ) as mock_src_cls,
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.ClosedCandleEmaProvider"
        ) as mock_ema_cls,
    ):
        mock_src = MagicMock()
        mock_ema = MagicMock()
        mock_src_cls.return_value = mock_src
        mock_ema_cls.return_value = mock_ema

        assembly = assemble_live_candle_sources(
            exit_mode=cast(PositionExitMode, "none"),
            base_url="https://fapi.binance.com",
            market_websocket_url="wss://fstream.binance.com",
            market_environment="live",
            session_id="sess_123",
            require_price_above_ema5=True,
            require_price_above_ema10=False,
            ownership_registry=ownership,
        )

        assert assembly.candle_source is mock_src
        assert assembly.closed_candle_feed is None
        assert assembly.ema_provider is mock_ema


@pytest.mark.asyncio
async def test_assemble_live_startup_market_buffer_hub_mode() -> None:
    cursor_state = LiveHubCursorState()
    cursor_state.restore({"stream_id": "stream-1", "sequence": 42})

    with (
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.WebSocketMarketStateSource"
        ) as mock_src_cls,
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.collect_startup_market_states",
            new_callable=AsyncMock,
        ),
    ):
        mock_source = MagicMock()
        mock_src_cls.return_value = mock_source

        assembly = assemble_live_startup_market_buffer(
            market_state_source="hub",
            market_state_hub_url="wss://hub.test",
            market_environment="live",
            session_id="sess_123",
            hub_cursor_state=cursor_state,
            max_states=100,
        )

        assert assembly.buffer is not None
        assert assembly.hub_source is mock_source
        assert assembly.task is not None
        mock_source.set_resume_cursor.assert_called_once_with(
            stream_id="stream-1",
            sequence=42,
        )

        # Test connection change listener delegation
        control_plane_cb = MagicMock()
        assembly.set_control_plane_listener(control_plane_cb)
        assembly.on_connection_change(True, "hub_connected")
        assert assembly.buffer.connection_available is True
        control_plane_cb.assert_called_once_with(True, "hub_connected")

        assembly.task.cancel()
        try:
            await assembly.task
        except asyncio.CancelledError:
            pass


def test_assemble_live_startup_market_buffer_non_hub_mode() -> None:
    cursor_state = LiveHubCursorState()
    assembly = assemble_live_startup_market_buffer(
        market_state_source="postgres",
        market_state_hub_url="",
        market_environment="live",
        session_id="sess_123",
        hub_cursor_state=cursor_state,
    )
    assert assembly.buffer is None
    assert assembly.hub_source is None
    assert assembly.task is None


def test_assemble_live_channel_sources_and_stop_all() -> None:
    with (
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.WebSocketMarketQuoteSource"
        ) as mock_quote_cls,
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.WebSocketAccountEventSource"
        ) as mock_account_cls,
        patch(
            "crypto_momentum_lab.live_rollout.market_assembly.WebSocketRiskControlSource"
        ) as mock_risk_cls,
    ):
        mock_hub = MagicMock()
        mock_quote = MagicMock()
        mock_account = MagicMock()
        mock_risk = MagicMock()

        mock_quote_cls.return_value = mock_quote
        mock_account_cls.return_value = mock_account
        mock_risk_cls.return_value = mock_risk

        sources = assemble_live_channel_sources(
            market_state_source="hub",
            market_quote_hub_url="wss://quote.hub",
            market_environment="live",
            account_event_hub_url="wss://account.hub",
            account_label="test_acc",
            session_id="sess_123",
            on_account_recovery=MagicMock(),
            hub_source=mock_hub,
            risk_control_enabled=True,
            risk_control_hub_url="wss://risk.hub",
            strategy_name="test_strat",
        )

        assert sources.hub_source is mock_hub
        assert sources.quote_source is mock_quote
        assert sources.account_source is mock_account
        assert sources.risk_control_source is mock_risk

        sources.stop_all()

        mock_hub.stop.assert_called_once()
        mock_quote.stop.assert_called_once()
        mock_account.stop.assert_called_once()
        mock_risk.stop.assert_called_once()


def test_build_live_market_state_stream_hub_vs_postgres() -> None:
    mock_strategy = MagicMock()
    mock_repo = MagicMock()

    # 1. Hub mode with buffer
    mock_buffer = MagicMock()

    stream = build_live_market_state_stream(
        market_state_source="hub",
        startup_market_buffer=mock_buffer,
        strategy=mock_strategy,
        state_repository=mock_repo,
        market_environment="live",
        max_runtime_seconds=3600.0,
        poll_interval_seconds=1.0,
        market_cursor=None,
    )
    assert stream is mock_buffer.stream.return_value
    mock_buffer.stream.assert_called_once()

    # 2. Postgres mode
    with patch(
        "crypto_momentum_lab.live_rollout.market_assembly.poll_live_market_states"
    ) as mock_poll:
        stream2 = build_live_market_state_stream(
            market_state_source="postgres",
            startup_market_buffer=None,
            strategy=mock_strategy,
            state_repository=mock_repo,
            market_environment="live",
            max_runtime_seconds=3600.0,
            poll_interval_seconds=1.0,
            market_cursor=None,
        )
        assert stream2 is mock_poll.return_value
        mock_poll.assert_called_once()
