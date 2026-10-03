"""Risk-control command persistence, publication, and CLI configuration."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from uuid import uuid4

import typer
from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.config import resolve_database_url
from crypto_momentum_lab.domain.live_rollout import (
    LiveSessionState,
    LiveSessionTransition,
    RollbackCommand,
)
from crypto_momentum_lab.execution_account.risk_control_hub import (
    RiskControlAction,
    RiskControlEvent,
    WebSocketRiskControlPublisher,
)
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_execution_database_engine,
)


async def _load_transition(
    database_url: str,
    session_id: str,
) -> LiveSessionTransition | None:
    engine = create_execution_database_engine(database_url)
    try:
        return await PostgresLiveRolloutRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        ).load_latest_transition(session_id)
    finally:
        await engine.dispose()


async def _save_transition(
    database_url: str,
    session_id: str,
    operator: str,
    strategy_config_hash: str,
    risk_config_hash: str,
    state: LiveSessionState,
    reason: str,
    *,
    strategy_scope: tuple[str, str, str] | None = None,
) -> LiveSessionTransition:
    now = datetime.now(tz=UTC)
    transition = LiveSessionTransition(
        transition_id=f"transition-{uuid4()}",
        session_id=session_id,
        state=state,
        occurred_at=now,
        operator=operator,
        strategy_config_hash=strategy_config_hash,
        risk_config_hash=risk_config_hash,
        reason=reason,
        details={},
    )
    engine = create_execution_database_engine(database_url)
    try:
        await PostgresLiveRolloutRepository(
            async_sessionmaker(engine, expire_on_commit=False),
            strategy_scope=strategy_scope,
        ).save_transition(transition)
    finally:
        await engine.dispose()
    return transition


async def _publish_risk_control_event(
    *,
    url: str,
    token: str | None,
    event: RiskControlEvent,
) -> RiskControlEvent:
    publisher = WebSocketRiskControlPublisher(
        url=url,
        token=token or os.environ.get("CML_RISK_CONTROL_HUB_TOKEN") or None,
    )
    return await publisher.publish(event)


def _issue_one_shot_risk_control_command(
    *,
    action: RiskControlAction,
    command_type: str,
    confirmation_text: str,
    reason: str,
    session_id: str,
    operator: str,
    idempotency_key: str,
    confirmation: str,
    account_label: str,
    strategy: str,
    risk_control_hub_url: str,
    risk_control_hub_token: str | None,
    database_url: str | None,
) -> None:
    if confirmation != confirmation_text:
        raise typer.BadParameter(f"--confirmation must equal '{confirmation_text}'")
    if not session_id.strip():
        raise typer.BadParameter("--session-id must not be empty")
    if not operator.strip():
        raise typer.BadParameter("--operator must not be empty")
    if not idempotency_key.strip():
        raise typer.BadParameter("--idempotency-key must not be empty")
    resolved_url = (
        risk_control_hub_url.strip()
        or os.environ.get("CML_RISK_CONTROL_HUB_URL", "").strip()
    )
    if not resolved_url:
        raise typer.BadParameter(
            "--risk-control-hub-url or CML_RISK_CONTROL_HUB_URL is required"
        )

    command = asyncio.run(
        _load_or_save_risk_control_command(
            database_url=_execution_database_url(database_url),
            command_type=command_type,
            requested_by=operator,
            confirmation_text=confirmation,
            idempotency_key=idempotency_key,
            account_label=account_label,
            strategy_name=strategy,
            session_id=session_id,
        )
    )
    if command.status != "requested":
        typer.echo(
            json.dumps(
                {
                    "command_id": command.command_id,
                    "status": command.status,
                    "idempotency_key": command.idempotency_key,
                },
                sort_keys=True,
            )
        )
        return

    event = RiskControlEvent(
        environment="live",
        account_label=account_label,
        strategy_name=strategy,
        session_id=session_id,
        action=action,
        event_id=command.command_id,
        command_id=command.command_id,
        reason=reason,
        issued_at=command.requested_at,
        details={
            "command_type": command_type,
            "idempotency_key": command.idempotency_key,
        },
    )
    try:
        published = asyncio.run(
            _publish_risk_control_event(
                url=resolved_url,
                token=risk_control_hub_token,
                event=event,
            )
        )
    except Exception as error:
        typer.echo(
            json.dumps(
                {
                    "command_id": command.command_id,
                    "status": "requested",
                    "publish_error": type(error).__name__,
                    "retry_with_same_idempotency_key": True,
                },
                sort_keys=True,
            )
        )
        raise typer.Exit(code=1) from error
    typer.echo(
        json.dumps(
            {
                "action": action.value,
                "command_id": command.command_id,
                "sequence": published.sequence,
                "status": "published",
                "stream_epoch": published.stream_epoch,
            },
            sort_keys=True,
        )
    )


async def _load_or_save_risk_control_command(
    *,
    database_url: str,
    command_type: str,
    requested_by: str,
    confirmation_text: str,
    idempotency_key: str,
    account_label: str,
    strategy_name: str,
    session_id: str,
) -> RollbackCommand:
    now = datetime.now(tz=UTC)
    engine = create_execution_database_engine(database_url)
    repository = PostgresLiveRolloutRepository(
        async_sessionmaker(engine, expire_on_commit=False)
    )
    try:
        existing = await repository.load_command_by_idempotency(idempotency_key)
        if existing is not None:
            _require_matching_risk_control_command(
                existing,
                command_type=command_type,
                account_label=account_label,
                strategy_name=strategy_name,
                session_id=session_id,
            )
            return existing
        command = RollbackCommand(
            command_id=f"command-{uuid4()}",
            command_type=command_type,
            requested_by=requested_by,
            confirmation_text=confirmation_text,
            requested_at=now,
            idempotency_key=idempotency_key,
            account_label=account_label,
            strategy_name=strategy_name,
            session_id=session_id,
            status="requested",
            completed_at=None,
            failure_reason=None,
        )
        if await repository.save_command(command):
            return command
        existing = await repository.load_command_by_idempotency(idempotency_key)
        if existing is None:
            raise RuntimeError("risk-control command insert was not observable")
        _require_matching_risk_control_command(
            existing,
            command_type=command_type,
            account_label=account_label,
            strategy_name=strategy_name,
            session_id=session_id,
        )
        return existing
    finally:
        await engine.dispose()


def _require_matching_risk_control_command(
    command: RollbackCommand,
    *,
    command_type: str,
    account_label: str,
    strategy_name: str,
    session_id: str,
) -> None:
    if (
        command.command_type != command_type
        or command.account_label != account_label
        or command.strategy_name != strategy_name
        or command.session_id != session_id
    ):
        raise ValueError(
            "idempotency key is already bound to a different risk-control command"
        )


def _execution_database_url(value: str | None) -> str:
    return _resolve_database_url(value, "CML_EXECUTION_DATABASE_URL")


def _market_database_url(value: str | None) -> str:
    return _resolve_database_url(value, "CML_MARKET_DATABASE_URL")


def _observability_database_url(value: str | None) -> str:
    return _resolve_database_url(value, "CML_OBSERVABILITY_DATABASE_URL")


def _resolve_database_url(value: str | None, plane_env_var: str) -> str:
    resolved = resolve_database_url(
        value,
        plane_env_var,
        "CML_DATABASE_URL",
    )
    if not resolved:
        raise typer.BadParameter(
            f"--database-url or {plane_env_var} or CML_DATABASE_URL is required"
        )
    return resolved
