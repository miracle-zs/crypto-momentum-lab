import asyncio
import json
import os
import re
import signal
from collections.abc import (
    Awaitable,
    Callable,
)
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated
from uuid import uuid4

import structlog
import typer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.config import (
    BinanceCredentialRole,
    CredentialResolutionError,
    ResolvedBinanceCredentials,
    resolve_database_url,
    resolve_role_credentials,
)
from crypto_momentum_lab.domain.execution import (
    FuturesPositionSide,
    OrderExecutionPlan,
)
from crypto_momentum_lab.domain.live_rollout import (
    LiveOperatorApproval,
    LiveSessionState,
    LiveSessionTransition,
    RollbackCommand,
)
from crypto_momentum_lab.domain.risk import (
    RiskConfigSnapshot,
    TradingLease,
    TradingLeaseState,
)
from crypto_momentum_lab.domain.strategy import (
    EntryType,
)
from crypto_momentum_lab.execution_account.risk_control_hub import (
    RiskControlAction,
    RiskControlEvent,
    WebSocketRiskControlPublisher,
)
from crypto_momentum_lab.live_rollout.commands import (
    CANCEL_ALL_OPEN_ENTRIES_COMMAND,
    CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION,
    EMERGENCY_FLATTEN_COMMAND,
    EMERGENCY_FLATTEN_CONFIRMATION,
)
from crypto_momentum_lab.live_rollout.daemon import (
    LiveDaemonResult,
)
from crypto_momentum_lab.live_rollout.missing_order_resolution import (
    resolve_missing_live_order as _resolve_missing_live_order,
)
from crypto_momentum_lab.live_rollout.missing_order_resolution import (
    validate_missing_order_resolution as _validate_missing_order_resolution_impl,
)
from crypto_momentum_lab.live_rollout.plan_runner import (
    run_live_plan as _run_live_plan,
)
from crypto_momentum_lab.live_rollout.profile import LiveOrderFlowImpulseProfile
from crypto_momentum_lab.live_rollout.runtime_config import (
    _LIVE_ENTRY_LIMIT_TTL_SECONDS,
    _LIVE_ENTRY_ORDER_TYPE,
    _LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT,
    _LIVE_ENTRY_PRICE_ABOVE_EMA5,
    _LIVE_ENTRY_PRICE_ABOVE_EMA10,
    _LIVE_MARKET_WEBSOCKET_URL,
    _live_strategy_config_hash,
)
from crypto_momentum_lab.live_rollout.runtime_manifest import (
    LiveRuntimeAccount,
)
from crypto_momentum_lab.live_rollout.runtime_options import (
    LiveRunOptions,
    LiveRuntimeOptionsError,
    resolve_live_runtime_config,
)
from crypto_momentum_lab.live_rollout.runtime_options import (
    parse_exchange_operations as _parse_runtime_exchange_operations,
)
from crypto_momentum_lab.live_rollout.runtime_options import (
    resolve_live_entry_positive_gainer_top_count as _resolve_runtime_top_count,
)
from crypto_momentum_lab.live_rollout.runtime_options import (
    resolve_live_profile_options as _resolve_runtime_profile_options,
)
from crypto_momentum_lab.live_rollout.runtime_options import (
    resolve_manifest_decimal_option as _resolve_runtime_manifest_decimal,
)
from crypto_momentum_lab.live_rollout.runtime_options import (
    resolve_manifest_operations as _resolve_runtime_manifest_operations,
)
from crypto_momentum_lab.live_rollout.runtime_options import (
    resolve_manifest_option as _resolve_runtime_manifest_option,
)
from crypto_momentum_lab.live_rollout.runtime_options import (
    runtime_manifest_account_for_cli as _runtime_options_manifest_account,
)
from crypto_momentum_lab.live_rollout.runtime_options import (
    runtime_manifest_strategy_config_hash as _runtime_options_manifest_hash,
)
from crypto_momentum_lab.live_rollout.runtime_orchestrator import (
    _session_is_draining,
    _warn_if_shadow_preflight_missing,
)
from crypto_momentum_lab.live_rollout.runtime_orchestrator import (
    run_live_daemon as _run_live_daemon,
)
from crypto_momentum_lab.live_rollout.startup_resilience import (
    run_with_live_startup_backoff as _run_with_live_startup_backoff,
)
from crypto_momentum_lab.persistence.postgres.live_rollout_repository import (
    PostgresLiveRolloutRepository,
)
from crypto_momentum_lab.persistence.postgres.models import (
    OrderIntentExecutionRow,
)
from crypto_momentum_lab.persistence.postgres.order_repository import (
    PostgresOrderRepository,
)
from crypto_momentum_lab.persistence.postgres.risk_repository import (
    PostgresRiskRepository,
)
from crypto_momentum_lab.persistence.postgres.runtime_context import (
    load_latest_account_state as _latest_account_state,
)
from crypto_momentum_lab.persistence.postgres.runtime_context import (
    load_latest_risk_config as _latest_risk_config,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_execution_database_engine,
)
from crypto_momentum_lab.strategy_runner.position_exit import (
    PositionExitMode,
)

app = typer.Typer(no_args_is_help=True)
log = structlog.get_logger()


_PREPARE_CONFIRMATION = "PREPARE LIVE RISK GATES"
_RENEW_LEASE_CONFIRMATION = "RENEW LIVE RISK LEASE"
_RESOLVE_MISSING_ORDER_CONFIRMATION = "RESOLVE MISSING LIVE ORDER"
_LIVE_ENTRY_POLICY_MODES = frozenset({"legacy", "compare_only", "enforce"})
# These two columns are retained by the existing risk-config schema for paper
# and shadow sessions. Live execution no longer enforces state-age limits; the
# large compatibility value makes that explicit without a destructive schema
# migration.
_LIVE_UNENFORCED_STATE_AGE_SECONDS = 1_000_000_000.0
_GIT_COMMIT_HASH_LENGTH = 40
_CONFIG_HASH_LENGTH = 64
_HEX_HASH_PATTERN = re.compile(r"^[0-9a-f]+$")


async def _run_live_with_signal_handlers(
    run_once: Callable[[asyncio.Event], Awaitable[LiveDaemonResult]],
) -> LiveDaemonResult:
    """Convert process stop signals into the live runtime's normal exit lane."""

    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []

    def request_stop(received_signal: signal.Signals) -> None:
        if not stop_requested.is_set():
            log.warning(
                "live_shutdown_signal_received",
                signal=received_signal.name,
            )
        stop_requested.set()

    for received_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(
                received_signal,
                request_stop,
                received_signal,
            )
        except (NotImplementedError, RuntimeError, ValueError):
            log.warning(
                "live_shutdown_signal_handler_unavailable",
                signal=received_signal.name,
            )
        else:
            registered_signals.append(received_signal)

    try:
        return await _run_with_live_startup_backoff(
            lambda: run_once(stop_requested),
            stop_requested=stop_requested,
        )
    finally:
        for received_signal in registered_signals:
            loop.remove_signal_handler(received_signal)


@dataclass(frozen=True, slots=True)
class _PreflightRuntimeStrategyConfig:
    """Runtime strategy inputs used by the preflight hash diagnostic."""

    profile: LiveOrderFlowImpulseProfile
    entry_positive_gainer_top_count: int
    require_price_above_ema5: bool
    require_price_above_ema10: bool
    entry_policy_mode: str
    entry_order_type: EntryType
    entry_limit_ttl_seconds: int

    @property
    def entry_policy_enforce(self) -> bool:
        return self.entry_policy_mode == "enforce"


@app.callback()
def live_rollout_app() -> None:
    """Operate explicitly approved small-capital live sessions."""


@app.command("strategy-config-hash")
def strategy_config_hash_command(
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    runtime_manifest: Annotated[
        Path | None,
        typer.Option(
            "--runtime-manifest",
            help="Derive hash inputs from the desired runtime manifest.",
        ),
    ] = None,
    impulse_window_buckets: Annotated[
        int | None,
        typer.Option("--impulse-window-buckets", min=2),
    ] = None,
    confirmation_buckets: Annotated[
        int | None,
        typer.Option("--confirmation-buckets", min=1),
    ] = None,
    min_return_pct: Annotated[
        str | None,
        typer.Option("--min-return-pct"),
    ] = None,
    min_imbalance: Annotated[
        str | None,
        typer.Option("--min-imbalance"),
    ] = None,
    min_intensity: Annotated[
        str | None,
        typer.Option("--min-intensity"),
    ] = None,
    min_notional_5m_vs_30m: Annotated[
        str | None,
        typer.Option("--min-notional-5m-vs-30m"),
    ] = None,
    cooldown_buckets: Annotated[
        int | None,
        typer.Option("--cooldown-buckets", min=0),
    ] = None,
    entry_positive_gainer_top_count: Annotated[
        int | None,
        typer.Option("--entry-positive-gainer-top-count", min=1),
    ] = None,
    entry_price_above_ema5: Annotated[
        bool,
        typer.Option("--entry-price-above-ema5/--no-entry-price-above-ema5"),
    ] = _LIVE_ENTRY_PRICE_ABOVE_EMA5,
    entry_price_above_ema10: Annotated[
        bool,
        typer.Option("--entry-price-above-ema10/--no-entry-price-above-ema10"),
    ] = _LIVE_ENTRY_PRICE_ABOVE_EMA10,
    entry_policy_enforce: Annotated[
        bool,
        typer.Option(
            "--entry-policy-enforce/--no-entry-policy-enforce",
            help="Use the shared Policy for real entry eligibility decisions.",
        ),
    ] = False,
    entry_order_type: Annotated[
        EntryType,
        typer.Option("--entry-order-type"),
    ] = _LIVE_ENTRY_ORDER_TYPE,
    entry_limit_ttl_seconds: Annotated[
        int,
        typer.Option("--entry-limit-ttl-seconds", min=601),
    ] = _LIVE_ENTRY_LIMIT_TTL_SECONDS,
) -> None:
    if runtime_manifest is not None:
        manifest_account = _runtime_manifest_account_for_cli(
            runtime_manifest,
            account_label=account_label,
            strategy=strategy,
        )
        typer.echo(_runtime_manifest_strategy_config_hash(manifest_account))
        return
    profile = _resolve_live_profile_options(
        impulse_window_buckets=impulse_window_buckets,
        confirmation_buckets=confirmation_buckets,
        min_return_pct=min_return_pct,
        min_imbalance=min_imbalance,
        min_intensity=min_intensity,
        min_notional_5m_vs_30m=min_notional_5m_vs_30m,
        cooldown_buckets=cooldown_buckets,
    )
    entry_positive_gainer_top_count = _resolve_live_entry_positive_gainer_top_count(
        entry_positive_gainer_top_count
    )
    typer.echo(
        _live_strategy_config_hash(
            strategy,
            profile=profile,
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=entry_price_above_ema5,
            require_price_above_ema10=entry_price_above_ema10,
            entry_policy_enforce=entry_policy_enforce,
            entry_order_type=entry_order_type,
            entry_limit_ttl_seconds=entry_limit_ttl_seconds,
        )
    )


@app.command("prepare")
def prepare_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    runtime_manifest: Annotated[
        Path | None,
        typer.Option(
            "--runtime-manifest",
            help="Validate this account against the desired runtime manifest.",
        ),
    ] = None,
    impulse_window_buckets: Annotated[
        int | None,
        typer.Option("--impulse-window-buckets", min=2),
    ] = None,
    confirmation_buckets: Annotated[
        int | None,
        typer.Option("--confirmation-buckets", min=1),
    ] = None,
    min_return_pct: Annotated[
        str | None,
        typer.Option("--min-return-pct"),
    ] = None,
    min_imbalance: Annotated[
        str | None,
        typer.Option("--min-imbalance"),
    ] = None,
    min_intensity: Annotated[
        str | None,
        typer.Option("--min-intensity"),
    ] = None,
    min_notional_5m_vs_30m: Annotated[
        str | None,
        typer.Option("--min-notional-5m-vs-30m"),
    ] = None,
    cooldown_buckets: Annotated[
        int | None,
        typer.Option("--cooldown-buckets", min=0),
    ] = None,
    lease_owner: Annotated[str, typer.Option("--lease-owner")] = "live-worker",
    git_commit_hash: Annotated[
        str,
        typer.Option("--git-commit-hash"),
    ] = "",
    migration_revision: Annotated[
        str,
        typer.Option("--migration-revision"),
    ] = "",
    lease_ttl_seconds: Annotated[
        int,
        typer.Option("--lease-ttl-seconds", min=180),
    ] = 300,
    max_order_notional: Annotated[
        str,
        typer.Option("--max-order-notional"),
    ] = "unlimited",
    max_gross_notional: Annotated[
        str,
        typer.Option("--max-gross-notional"),
    ] = "unlimited",
    max_daily_loss: Annotated[
        str,
        typer.Option("--max-daily-loss"),
    ] = "unlimited",
    max_open_positions: Annotated[
        str,
        typer.Option("--max-open-positions"),
    ] = "unlimited",
    entry_positive_gainer_top_count: Annotated[
        int,
        typer.Option("--entry-positive-gainer-top-count", min=1),
    ] = _LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT,
    entry_price_above_ema5: Annotated[
        bool,
        typer.Option("--entry-price-above-ema5/--no-entry-price-above-ema5"),
    ] = _LIVE_ENTRY_PRICE_ABOVE_EMA5,
    entry_price_above_ema10: Annotated[
        bool,
        typer.Option("--entry-price-above-ema10/--no-entry-price-above-ema10"),
    ] = _LIVE_ENTRY_PRICE_ABOVE_EMA10,
    entry_policy_enforce: Annotated[
        bool,
        typer.Option(
            "--entry-policy-enforce/--no-entry-policy-enforce",
            help="Use the shared Policy for real entry eligibility decisions.",
        ),
    ] = False,
    entry_order_type: Annotated[
        EntryType,
        typer.Option("--entry-order-type"),
    ] = _LIVE_ENTRY_ORDER_TYPE,
    entry_limit_ttl_seconds: Annotated[
        int,
        typer.Option("--entry-limit-ttl-seconds", min=601),
    ] = _LIVE_ENTRY_LIMIT_TTL_SECONDS,
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
) -> None:
    if confirmation != _PREPARE_CONFIRMATION:
        raise typer.BadParameter(f"--confirmation must equal '{_PREPARE_CONFIRMATION}'")
    manifest_account = (
        None
        if runtime_manifest is None
        else _runtime_manifest_account_for_cli(
            runtime_manifest,
            account_label=account_label,
            strategy=strategy,
        )
    )
    configured_git_commit = (
        git_commit_hash.strip()
        or os.environ.get(
            "CML_CODE_COMMIT",
            "",
        ).strip()
    )
    if manifest_account is not None:
        manifest_git_commit = _validate_hex_hash(
            manifest_account.image_commit,
            "runtime manifest image_commit",
            _GIT_COMMIT_HASH_LENGTH,
        )
        if (
            configured_git_commit
            and configured_git_commit.lower() != manifest_git_commit
        ):
            raise typer.BadParameter("git commit does not match the runtime manifest")
        configured_git_commit = manifest_git_commit
    git_commit_hash = _validate_hex_hash(
        configured_git_commit,
        "--git-commit-hash or CML_CODE_COMMIT",
        _GIT_COMMIT_HASH_LENGTH,
    )
    if manifest_account is not None:
        configured_migration_revision = (
            migration_revision.strip()
            or os.environ.get(
                "CML_LIVE_MIGRATION_REVISION",
                "",
            ).strip()
        )
        if (
            configured_migration_revision
            and configured_migration_revision != manifest_account.migration_revision
        ):
            raise typer.BadParameter(
                "migration revision does not match the runtime manifest"
            )
    if manifest_account is not None and lease_owner != manifest_account.lease_owner:
        raise typer.BadParameter("lease owner does not match the runtime manifest")
    if manifest_account is None:
        profile = _resolve_live_profile_options(
            impulse_window_buckets=impulse_window_buckets,
            confirmation_buckets=confirmation_buckets,
            min_return_pct=min_return_pct,
            min_imbalance=min_imbalance,
            min_intensity=min_intensity,
            min_notional_5m_vs_30m=min_notional_5m_vs_30m,
            cooldown_buckets=cooldown_buckets,
        )
    else:
        strategy_inputs = manifest_account.strategy_inputs
        profile = strategy_inputs.profile
        entry_positive_gainer_top_count = (
            strategy_inputs.entry_positive_gainer_top_count
        )
        entry_price_above_ema5 = strategy_inputs.require_price_above_ema5
        entry_price_above_ema10 = strategy_inputs.require_price_above_ema10
        entry_policy_enforce = strategy_inputs.entry_policy_enforce
        entry_order_type = strategy_inputs.entry_order_type
        entry_limit_ttl_seconds = strategy_inputs.entry_limit_ttl_seconds
    strategy_config_hash = _live_strategy_config_hash(
        strategy,
        profile=profile,
        entry_positive_gainer_top_count=entry_positive_gainer_top_count,
        require_price_above_ema5=entry_price_above_ema5,
        require_price_above_ema10=entry_price_above_ema10,
        entry_policy_enforce=entry_policy_enforce,
        entry_order_type=entry_order_type,
        entry_limit_ttl_seconds=entry_limit_ttl_seconds,
    )
    if (
        manifest_account is not None
        and strategy_config_hash
        != _runtime_manifest_strategy_config_hash(manifest_account)
    ):
        raise typer.BadParameter(
            "prepared strategy inputs do not match the runtime manifest hash"
        )
    payload = asyncio.run(
        _prepare_live_risk_gates(
            database_url=_database_url(database_url),
            account_label=account_label,
            strategy_name=strategy,
            lease_owner=lease_owner,
            code_generation=git_commit_hash,
            lease_ttl_seconds=lease_ttl_seconds,
            max_order_notional=_parse_optional_decimal_limit(
                max_order_notional,
                "--max-order-notional",
            ),
            max_gross_notional=_parse_optional_decimal_limit(
                max_gross_notional,
                "--max-gross-notional",
            ),
            max_daily_loss=_parse_optional_decimal_limit(
                max_daily_loss,
                "--max-daily-loss",
            ),
            max_open_positions=_parse_optional_integer_limit(
                max_open_positions,
                "--max-open-positions",
            ),
            profile=profile,
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=entry_price_above_ema5,
            require_price_above_ema10=entry_price_above_ema10,
            entry_policy_enforce=entry_policy_enforce,
            entry_order_type=entry_order_type,
            entry_limit_ttl_seconds=entry_limit_ttl_seconds,
        )
    )
    typer.echo(json.dumps(payload, sort_keys=True))


@app.command("renew-lease")
def renew_lease_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    lease_owner: Annotated[str, typer.Option("--lease-owner")] = "live-worker",
    lease_ttl_seconds: Annotated[
        int,
        typer.Option("--lease-ttl-seconds", min=300),
    ] = 3600,
    git_commit_hash: Annotated[
        str,
        typer.Option("--git-commit-hash"),
    ] = "",
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
) -> None:
    if confirmation != _RENEW_LEASE_CONFIRMATION:
        raise typer.BadParameter(
            f"--confirmation must equal '{_RENEW_LEASE_CONFIRMATION}'"
        )
    normalized_git_commit_hash = git_commit_hash.strip()
    code_generation = (
        None
        if not normalized_git_commit_hash
        else _validate_hex_hash(
            normalized_git_commit_hash,
            "--git-commit-hash",
            _GIT_COMMIT_HASH_LENGTH,
        )
    )
    payload = asyncio.run(
        _renew_live_lease(
            database_url=_database_url(database_url),
            account_label=account_label,
            strategy_name=strategy,
            lease_owner=lease_owner,
            lease_ttl_seconds=lease_ttl_seconds,
            code_generation=code_generation,
        )
    )
    typer.echo(json.dumps(payload, sort_keys=True))


@app.command("approve")
def approve_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    strategy_config_hash: Annotated[str, typer.Option("--strategy-config-hash")] = "",
    risk_config_hash: Annotated[str, typer.Option("--risk-config-hash")] = "",
    git_commit_hash: Annotated[str, typer.Option("--git-commit-hash")] = "",
    migration_revision: Annotated[str, typer.Option("--migration-revision")] = "",
    notional_cap: Annotated[str, typer.Option("--notional-cap")] = "unlimited",
    max_open_positions: Annotated[
        str, typer.Option("--max-open-positions")
    ] = "unlimited",
    max_daily_loss: Annotated[str, typer.Option("--max-daily-loss")] = "unlimited",
    approver: Annotated[str, typer.Option("--approver")] = "",
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
    expires_in_minutes: Annotated[str, typer.Option("--expires-in-minutes")] = "never",
) -> None:
    strategy_config_hash = _validate_hex_hash(
        strategy_config_hash,
        "--strategy-config-hash",
        _CONFIG_HASH_LENGTH,
    )
    risk_config_hash = _validate_hex_hash(
        risk_config_hash,
        "--risk-config-hash",
        _CONFIG_HASH_LENGTH,
    )
    git_commit_hash = _validate_hex_hash(
        git_commit_hash,
        "--git-commit-hash",
        _GIT_COMMIT_HASH_LENGTH,
    )
    migration_revision = migration_revision.strip()
    if not migration_revision:
        raise typer.BadParameter("--migration-revision must not be empty")
    configured_strategy_hash = (
        os.environ.get("CML_LIVE_STRATEGY_CONFIG_HASH", "").strip().lower()
    )
    if (
        configured_strategy_hash
        and configured_strategy_hash != "unset"
        and strategy_config_hash != configured_strategy_hash
    ):
        raise typer.BadParameter(
            "--strategy-config-hash does not match the configured Live runtime hash"
        )
    resolved_database_url = _database_url(database_url)
    latest_risk_hash = asyncio.run(
        _latest_risk_config_hash(resolved_database_url, account_label)
    )
    if risk_config_hash != latest_risk_hash:
        raise typer.BadParameter(
            "--risk-config-hash does not match the latest persisted risk config"
        )
    now = datetime.now(tz=UTC)
    approval = LiveOperatorApproval(
        approval_id=f"approval-{uuid4()}",
        account_label=account_label,
        strategy_name=strategy,
        strategy_config_hash=strategy_config_hash,
        risk_config_hash=risk_config_hash,
        git_commit_hash=git_commit_hash,
        database_migration_revision=migration_revision,
        approved_notional_cap=_parse_optional_decimal_limit(
            notional_cap,
            "--notional-cap",
        ),
        approved_max_open_positions=_parse_optional_integer_limit(
            max_open_positions,
            "--max-open-positions",
        ),
        approved_max_daily_loss=_parse_optional_decimal_limit(
            max_daily_loss,
            "--max-daily-loss",
        ),
        approver_name=approver,
        approval_text=confirmation,
        expires_at=_parse_approval_expiration(now, expires_in_minutes),
        created_at=now,
    )
    asyncio.run(_save_approval(resolved_database_url, approval))
    typer.echo(f"Live approval recorded: {approval.approval_id}")


@app.command("approve-runtime")
def approve_runtime_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    git_commit_hash: Annotated[str, typer.Option("--git-commit-hash")] = "",
    migration_revision: Annotated[str, typer.Option("--migration-revision")] = "",
    notional_cap: Annotated[str, typer.Option("--notional-cap")] = "unlimited",
    max_open_positions: Annotated[
        str, typer.Option("--max-open-positions")
    ] = "unlimited",
    max_daily_loss: Annotated[str, typer.Option("--max-daily-loss")] = "unlimited",
    approver: Annotated[str, typer.Option("--approver")] = "",
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
    expires_in_minutes: Annotated[str, typer.Option("--expires-in-minutes")] = "never",
) -> None:
    """Record approval values derived from the running account environment."""

    resolved_database_url = _database_url(database_url)
    strategy_config_hash = _validate_hex_hash(
        _runtime_strategy_config_hash(strategy),
        "runtime strategy config hash",
        _CONFIG_HASH_LENGTH,
    )
    configured_strategy_hash = (
        os.environ.get("CML_LIVE_STRATEGY_CONFIG_HASH", "").strip().lower()
    )
    if (
        configured_strategy_hash
        and configured_strategy_hash != "unset"
        and configured_strategy_hash != strategy_config_hash
    ):
        raise typer.BadParameter(
            "configured Live strategy hash does not match the runtime hash"
        )
    risk_config_hash = asyncio.run(
        _latest_risk_config_hash(resolved_database_url, account_label)
    )
    git_commit_hash = _validate_hex_hash(
        git_commit_hash.strip() or os.environ.get("CML_CODE_COMMIT", ""),
        "--git-commit-hash or CML_CODE_COMMIT",
        _GIT_COMMIT_HASH_LENGTH,
    )
    migration_revision = (
        migration_revision.strip()
        or os.environ.get("CML_LIVE_MIGRATION_REVISION", "").strip()
    )
    if not migration_revision:
        raise typer.BadParameter(
            "--migration-revision or CML_LIVE_MIGRATION_REVISION is required"
        )
    now = datetime.now(tz=UTC)
    approval = LiveOperatorApproval(
        approval_id=f"approval-{uuid4()}",
        account_label=account_label,
        strategy_name=strategy,
        strategy_config_hash=strategy_config_hash,
        risk_config_hash=risk_config_hash,
        git_commit_hash=git_commit_hash,
        database_migration_revision=migration_revision,
        approved_notional_cap=_parse_optional_decimal_limit(
            notional_cap,
            "--notional-cap",
        ),
        approved_max_open_positions=_parse_optional_integer_limit(
            max_open_positions,
            "--max-open-positions",
        ),
        approved_max_daily_loss=_parse_optional_decimal_limit(
            max_daily_loss,
            "--max-daily-loss",
        ),
        approver_name=approver,
        approval_text=confirmation,
        expires_at=_parse_approval_expiration(now, expires_in_minutes),
        created_at=now,
    )
    asyncio.run(_save_approval(resolved_database_url, approval))
    typer.echo(
        json.dumps(
            {
                "approval_id": approval.approval_id,
                "account_label": account_label,
                "strategy_config_hash": strategy_config_hash,
                "risk_config_hash": risk_config_hash,
                "git_commit_hash": git_commit_hash,
                "database_migration_revision": migration_revision,
            },
            sort_keys=True,
        )
    )


@app.command("refresh-approval-runtime")
def refresh_approval_runtime_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    git_commit_hash: Annotated[str, typer.Option("--git-commit-hash")] = "",
    migration_revision: Annotated[str, typer.Option("--migration-revision")] = "",
) -> None:
    """Refresh an active approval while preserving its operator limits."""

    resolved_database_url = _database_url(database_url)
    now = datetime.now(tz=UTC)
    current_approval = asyncio.run(
        _load_active_approval(
            resolved_database_url,
            account_label,
            strategy,
            now,
        )
    )
    if current_approval is None:
        raise typer.BadParameter(
            f"active approval is missing for {account_label}/{strategy}"
        )
    strategy_config_hash = _validate_hex_hash(
        _runtime_strategy_config_hash(strategy),
        "runtime strategy config hash",
        _CONFIG_HASH_LENGTH,
    )
    configured_strategy_hash = (
        os.environ.get("CML_LIVE_STRATEGY_CONFIG_HASH", "").strip().lower()
    )
    if (
        configured_strategy_hash
        and configured_strategy_hash != "unset"
        and configured_strategy_hash != strategy_config_hash
    ):
        raise typer.BadParameter(
            "configured Live strategy hash does not match the runtime hash"
        )
    risk_config_hash = asyncio.run(
        _latest_risk_config_hash(resolved_database_url, account_label)
    )
    git_commit_hash = _validate_hex_hash(
        git_commit_hash.strip() or os.environ.get("CML_CODE_COMMIT", ""),
        "--git-commit-hash or CML_CODE_COMMIT",
        _GIT_COMMIT_HASH_LENGTH,
    )
    migration_revision = (
        migration_revision.strip()
        or os.environ.get("CML_LIVE_MIGRATION_REVISION", "").strip()
    )
    if not migration_revision:
        raise typer.BadParameter(
            "--migration-revision or CML_LIVE_MIGRATION_REVISION is required"
        )
    refreshed_approval = replace(
        current_approval,
        approval_id=f"approval-{uuid4()}",
        strategy_config_hash=strategy_config_hash,
        risk_config_hash=risk_config_hash,
        git_commit_hash=git_commit_hash,
        database_migration_revision=migration_revision,
        created_at=now,
    )
    asyncio.run(_save_approval(resolved_database_url, refreshed_approval))
    typer.echo(
        json.dumps(
            {
                "approval_id": refreshed_approval.approval_id,
                "account_label": account_label,
                "strategy_config_hash": strategy_config_hash,
                "risk_config_hash": risk_config_hash,
                "git_commit_hash": git_commit_hash,
                "database_migration_revision": migration_revision,
                "preserved_notional_cap": (
                    None
                    if refreshed_approval.approved_notional_cap is None
                    else str(refreshed_approval.approved_notional_cap)
                ),
                "preserved_max_open_positions": (
                    refreshed_approval.approved_max_open_positions
                ),
                "preserved_max_daily_loss": (
                    None
                    if refreshed_approval.approved_max_daily_loss is None
                    else str(refreshed_approval.approved_max_daily_loss)
                ),
            },
            sort_keys=True,
        )
    )


def _runtime_manifest_account_for_cli(
    path: Path,
    *,
    account_label: str,
    strategy: str,
) -> LiveRuntimeAccount:
    try:
        return _runtime_options_manifest_account(
            path,
            account_label=account_label,
            strategy=strategy,
        )
    except LiveRuntimeOptionsError as error:
        raise typer.BadParameter(str(error)) from error


def _runtime_manifest_strategy_config_hash(
    account: LiveRuntimeAccount,
) -> str:
    try:
        return _runtime_options_manifest_hash(account)
    except LiveRuntimeOptionsError as error:
        raise typer.BadParameter(str(error)) from error


def _resolve_manifest_option[T](
    value: T | None,
    expected: T,
    option_name: str,
) -> T:
    """Use manifest values while rejecting an explicitly conflicting option."""

    try:
        return _resolve_runtime_manifest_option(value, expected, option_name)
    except LiveRuntimeOptionsError as error:
        raise typer.BadParameter(str(error)) from error


def _resolve_manifest_decimal_option(
    value: str | None,
    expected: Decimal,
    option_name: str,
) -> str:
    try:
        return _resolve_runtime_manifest_decimal(value, expected, option_name)
    except LiveRuntimeOptionsError as error:
        raise typer.BadParameter(str(error)) from error


def _resolve_manifest_operations(
    value: str | None,
    expected: str,
    option_name: str,
) -> str:
    try:
        return _resolve_runtime_manifest_operations(value, expected, option_name)
    except LiveRuntimeOptionsError as error:
        raise typer.BadParameter(str(error)) from error


@app.command("preflight")
def preflight_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    runtime_manifest: Annotated[
        Path | None,
        typer.Option(
            "--runtime-manifest",
            help="Validate this account against the desired runtime manifest.",
        ),
    ] = None,
    strict: Annotated[bool, typer.Option("--strict")] = False,
    expected_git_commit: Annotated[
        str | None, typer.Option("--expected-git-commit")
    ] = None,
    expected_migration_revision: Annotated[
        str | None, typer.Option("--expected-migration-revision")
    ] = None,
) -> None:
    manifest_account = None
    if runtime_manifest is not None:
        manifest_account = _runtime_manifest_account_for_cli(
            runtime_manifest,
            account_label=account_label,
            strategy=strategy,
        )
        manifest_git_commit = _validate_hex_hash(
            manifest_account.image_commit,
            "runtime manifest image_commit",
            _GIT_COMMIT_HASH_LENGTH,
        )
        if expected_git_commit is None:
            expected_git_commit = manifest_git_commit
        elif expected_git_commit.lower() != manifest_git_commit:
            raise typer.BadParameter(
                "--expected-git-commit does not match the runtime manifest"
            )
        if expected_migration_revision is None:
            expected_migration_revision = manifest_account.migration_revision
        elif expected_migration_revision != manifest_account.migration_revision:
            raise typer.BadParameter(
                "--expected-migration-revision does not match the runtime manifest"
            )
    if expected_git_commit is not None:
        expected_git_commit = _validate_hex_hash(
            expected_git_commit,
            "--expected-git-commit",
            _GIT_COMMIT_HASH_LENGTH,
        )
    if expected_migration_revision is not None:
        expected_migration_revision = expected_migration_revision.strip()
        if not expected_migration_revision:
            raise typer.BadParameter("--expected-migration-revision must not be empty")
    payload = asyncio.run(
        _preflight_summary(
            _database_url(database_url),
            account_label,
            strategy,
            expected_git_commit=expected_git_commit,
            expected_migration_revision=expected_migration_revision,
            expected_lease_owner=(
                None if manifest_account is None else manifest_account.lease_owner
            ),
            expected_strategy_config_hash=(
                None
                if manifest_account is None
                else _runtime_manifest_strategy_config_hash(manifest_account)
            ),
        )
    )
    typer.echo(json.dumps(payload, sort_keys=True))
    if strict and payload["preflight_ok"] is not True:
        raise typer.Exit(code=1)


@app.command("approval-precheck")
def approval_precheck_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    expected_git_commit: Annotated[
        str, typer.Option("--expected-git-commit")
    ] = "",
    expected_migration_revision: Annotated[
        str, typer.Option("--expected-migration-revision")
    ] = "",
    strict: Annotated[bool, typer.Option("--strict")] = False,
) -> None:
    """Validate the active approval's target commit and migration binding."""

    expected_git_commit = _validate_hex_hash(
        expected_git_commit,
        "--expected-git-commit",
        _GIT_COMMIT_HASH_LENGTH,
    )
    expected_migration_revision = expected_migration_revision.strip()
    if not expected_migration_revision:
        raise typer.BadParameter("--expected-migration-revision must not be empty")
    payload = asyncio.run(
        _approval_binding_summary(
            _database_url(database_url),
            account_label,
            strategy,
            expected_git_commit=expected_git_commit,
            expected_migration_revision=expected_migration_revision,
        )
    )
    typer.echo(json.dumps(payload, sort_keys=True))
    if strict and payload["approval_precheck_ok"] is not True:
        raise typer.Exit(code=1)


@app.command("resolve-missing-order")
def resolve_missing_order_command(
    client_order_id: Annotated[str, typer.Option("--client-order-id")],
    operator: Annotated[str, typer.Option("--operator")],
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    min_missing_age_seconds: Annotated[
        int,
        typer.Option("--min-missing-age-seconds", min=60),
    ] = 600,
    base_url: Annotated[str, typer.Option("--base-url")] = "https://fapi.binance.com",
    api_key_env: Annotated[str, typer.Option("--api-key-env")] = "BINANCE_API_KEY",
    api_secret_env: Annotated[
        str, typer.Option("--api-secret-env")
    ] = "BINANCE_API_SECRET",
) -> None:
    """Resolve one old reduce-only order after proving it is absent on Binance."""
    if confirmation != _RESOLVE_MISSING_ORDER_CONFIRMATION:
        raise typer.BadParameter(
            f"--confirmation must equal '{_RESOLVE_MISSING_ORDER_CONFIRMATION}'"
        )
    if not client_order_id.strip():
        raise typer.BadParameter("--client-order-id must not be empty")
    if not operator.strip():
        raise typer.BadParameter("--operator must not be empty")
    api_key = os.environ.get(api_key_env)
    api_secret = os.environ.get(api_secret_env)
    if not api_key or not api_secret:
        raise typer.BadParameter(f"{api_key_env} and {api_secret_env} are required")
    payload = asyncio.run(
        _resolve_missing_live_order(
            database_url=_database_url(database_url),
            account_label=account_label,
            client_order_id=client_order_id,
            operator=operator,
            min_missing_age_seconds=min_missing_age_seconds,
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
        )
    )
    typer.echo(json.dumps(payload, sort_keys=True, default=str))


@app.command("submit-plan")
def submit_plan_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "compression_breakout",
    session_id: Annotated[str, typer.Option("--session-id")] = "live-manual",
    operator: Annotated[str, typer.Option("--operator")] = "",
    lease_owner: Annotated[str, typer.Option("--lease-owner")] = "live-worker",
    strategy_config_hash: Annotated[str, typer.Option("--strategy-config-hash")] = "",
    git_commit_hash: Annotated[str, typer.Option("--git-commit-hash")] = "",
    migration_revision: Annotated[str, typer.Option("--migration-revision")] = "",
    order_plan_json: Annotated[
        Path | None, typer.Option("--order-plan-json", exists=True, dir_okay=False)
    ] = None,
    account_event_hub_url: Annotated[
        str,
        typer.Option("--account-event-hub-url"),
    ] = "ws://execution-account-live:8767",
    base_url: Annotated[str, typer.Option("--base-url")] = "https://fapi.binance.com",
    api_key_env: Annotated[str, typer.Option("--api-key-env")] = "BINANCE_API_KEY",
    api_secret_env: Annotated[
        str, typer.Option("--api-secret-env")
    ] = "BINANCE_API_SECRET",
    entry_leverage: Annotated[
        int, typer.Option("--entry-leverage", min=1, max=125)
    ] = 1,
    margin_type: Annotated[
        str,
        typer.Option(
            "--margin-type",
            help="Entry margin mode: CROSSED or ISOLATED.",
        ),
    ] = "CROSSED",
    confirmation: Annotated[
        bool, typer.Option("--i-understand-this-places-real-orders")
    ] = False,
) -> None:
    if not confirmation:
        raise typer.BadParameter("--i-understand-this-places-real-orders is required")
    if order_plan_json is None:
        raise typer.BadParameter("--order-plan-json is required")
    api_key = os.environ.get(api_key_env)
    api_secret = os.environ.get(api_secret_env)
    if not api_key or not api_secret:
        raise typer.BadParameter(f"{api_key_env} and {api_secret_env} are required")
    plan = _load_plan(order_plan_json)
    result = asyncio.run(
        _run_live_plan(
            database_url=_database_url(database_url),
            account_label=account_label,
            strategy_name=strategy,
            session_id=session_id,
            operator=operator,
            lease_owner=lease_owner,
            strategy_config_hash=strategy_config_hash,
            git_commit_hash=git_commit_hash,
            migration_revision=migration_revision,
            plan=plan,
            account_event_hub_url=account_event_hub_url,
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            entry_leverage=entry_leverage,
            margin_type=margin_type,
            load_latest_risk_config=_latest_risk_config,
            load_latest_account_state=_latest_account_state,
            load_approved_intent_notional=_approved_intent_notional,
            session_is_draining=_session_is_draining,
            warn_if_shadow_preflight_missing=_warn_if_shadow_preflight_missing,
        )
    )
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("run")
def run_command(
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    runtime_manifest: Annotated[
        Path | None,
        typer.Option(
            "--runtime-manifest",
            help="Use the desired runtime manifest for this worker.",
        ),
    ] = None,
    impulse_window_buckets: Annotated[
        int | None,
        typer.Option("--impulse-window-buckets", min=2),
    ] = None,
    confirmation_buckets: Annotated[
        int | None,
        typer.Option("--confirmation-buckets", min=1),
    ] = None,
    min_return_pct: Annotated[
        str | None,
        typer.Option("--min-return-pct"),
    ] = None,
    min_imbalance: Annotated[
        str | None,
        typer.Option("--min-imbalance"),
    ] = None,
    min_intensity: Annotated[
        str | None,
        typer.Option("--min-intensity"),
    ] = None,
    min_notional_5m_vs_30m: Annotated[
        str | None,
        typer.Option("--min-notional-5m-vs-30m"),
    ] = None,
    cooldown_buckets: Annotated[
        int | None,
        typer.Option("--cooldown-buckets", min=0),
    ] = None,
    market_environment: Annotated[
        str,
        typer.Option("--market-environment"),
    ] = "research",
    market_state_source: Annotated[
        str,
        typer.Option(
            "--market-state-source",
            help="Realtime source: hub (default) or postgres (explicit recovery mode).",
        ),
    ] = "hub",
    market_state_hub_url: Annotated[
        str,
        typer.Option("--market-state-hub-url"),
    ] = "ws://market-data:8766",
    market_quote_hub_url: Annotated[
        str,
        typer.Option("--market-quote-hub-url"),
    ] = "ws://market-data:8768",
    market_quote_volume_hub_url: Annotated[
        str,
        typer.Option("--market-quote-volume-hub-url"),
    ] = "ws://market-data:8768",
    market_websocket_url: Annotated[
        str,
        typer.Option(
            "--market-websocket-url",
            help="Direct Binance market WebSocket used by closed-candle exits.",
        ),
    ] = _LIVE_MARKET_WEBSOCKET_URL,
    account_event_hub_url: Annotated[
        str,
        typer.Option("--account-event-hub-url"),
    ] = "ws://execution-account-live:8767",
    risk_control_hub_url: Annotated[
        str,
        typer.Option(
            "--risk-control-hub-url",
            help=(
                "Low-volume operator control stream. PostgreSQL remains the "
                "durable authority."
            ),
        ),
    ] = "ws://execution-account-live:8769",
    session_id: Annotated[str | None, typer.Option("--session-id")] = None,
    operator: Annotated[str, typer.Option("--operator")] = "",
    lease_owner: Annotated[str | None, typer.Option("--lease-owner")] = None,
    strategy_config_hash: Annotated[str, typer.Option("--strategy-config-hash")] = "",
    git_commit_hash: Annotated[str, typer.Option("--git-commit-hash")] = "",
    migration_revision: Annotated[str, typer.Option("--migration-revision")] = "",
    max_runtime_seconds: Annotated[
        int, typer.Option("--max-runtime-seconds", min=1)
    ] = 3600,
    poll_interval_seconds: Annotated[
        float, typer.Option("--poll-interval-seconds", min=0.1)
    ] = 0.25,
    checkpoint_every_states: Annotated[
        int, typer.Option("--checkpoint-every-states", min=1)
    ] = 100,
    hedge_mode: Annotated[
        bool | None,
        typer.Option("--hedge-mode/--one-way-mode"),
    ] = None,
    exit_mode: Annotated[
        PositionExitMode | None,
        typer.Option("--exit-mode"),
    ] = None,
    take_profit_pct: Annotated[
        str | None,
        typer.Option("--take-profit-pct"),
    ] = None,
    stop_loss_pct: Annotated[
        str | None,
        typer.Option("--stop-loss-pct"),
    ] = None,
    entry_long_only: Annotated[
        bool | None,
        typer.Option("--entry-long-only/--entry-all-sides"),
    ] = None,
    entry_positive_gainer_top_count: Annotated[
        int | None,
        typer.Option("--entry-positive-gainer-top-count", min=1),
    ] = None,
    entry_price_above_ema5: Annotated[
        bool | None,
        typer.Option("--entry-price-above-ema5/--no-entry-price-above-ema5"),
    ] = None,
    entry_price_above_ema10: Annotated[
        bool | None,
        typer.Option("--entry-price-above-ema10/--no-entry-price-above-ema10"),
    ] = None,
    entry_order_type: Annotated[
        EntryType | None,
        typer.Option("--entry-order-type"),
    ] = None,
    entry_limit_ttl_seconds: Annotated[
        int | None,
        typer.Option("--entry-limit-ttl-seconds", min=601),
    ] = None,
    candle_grace_bars: Annotated[
        int | None,
        typer.Option("--candle-grace-bars", min=0),
    ] = None,
    candle_grace_decision_profit_pct: Annotated[
        str | None,
        typer.Option("--candle-grace-decision-profit-pct"),
    ] = None,
    candle_grace_profit_pct: Annotated[
        str | None,
        typer.Option("--candle-grace-profit-pct"),
    ] = None,
    base_url: Annotated[str, typer.Option("--base-url")] = "https://fapi.binance.com",
    api_key_env: Annotated[
        str | None,
        typer.Option(
            "--api-key-env",
            help="Override the trade credential key environment variable.",
        ),
    ] = None,
    api_secret_env: Annotated[
        str | None,
        typer.Option(
            "--api-secret-env",
            help="Override the trade credential secret environment variable.",
        ),
    ] = None,
    allow_legacy_credential_fallback: Annotated[
        bool,
        typer.Option(
            "--allow-legacy-credential-fallback/--no-allow-legacy-credential-fallback",
            help=("Temporarily fall back to BINANCE_API_KEY/SECRET during migration."),
        ),
    ] = False,
    entry_leverage: Annotated[
        int | None, typer.Option("--entry-leverage", min=1, max=125)
    ] = None,
    margin_type: Annotated[
        str | None,
        typer.Option(
            "--margin-type",
            help="Entry margin mode: CROSSED or ISOLATED.",
        ),
    ] = None,
    entry_policy_compare_only: Annotated[
        bool | None,
        typer.Option(
            "--entry-policy-compare-only/--no-entry-policy-compare-only",
            help=(
                "Record legacy-vs-Policy entry differences without changing "
                "order decisions."
            ),
        ),
    ] = None,
    entry_policy_enforce: Annotated[
        bool | None,
        typer.Option(
            "--entry-policy-enforce/--no-entry-policy-enforce",
            help="Use the shared Policy for real entry eligibility decisions.",
        ),
    ] = None,
    acknowledge_missing_shadow_preflight: Annotated[
        bool,
        typer.Option(
            "--acknowledge-missing-shadow-preflight",
            help=(
                "Acknowledge the advisory when no matching completed Shadow "
                "session exists."
            ),
        ),
    ] = False,
    persist_exchange_operations: Annotated[
        str | None,
        typer.Option(
            "--persist-exchange-operations",
            help=(
                "Comma-separated exchange operations to persist (default: "
                "submit,cancel); use 'all' for a temporary full-operation "
                "diagnostic capture."
            ),
        ),
    ] = None,
    confirmation: Annotated[
        bool, typer.Option("--i-understand-this-places-real-orders")
    ] = False,
) -> None:
    if entry_policy_compare_only and entry_policy_enforce:
        raise typer.BadParameter(
            "--entry-policy-compare-only and "
            "--entry-policy-enforce are mutually exclusive"
        )
    if not confirmation:
        raise typer.BadParameter("--i-understand-this-places-real-orders is required")
    credentials = _resolve_live_cli_credentials(
        api_key_env=api_key_env,
        api_secret_env=api_secret_env,
        allow_legacy_fallback=allow_legacy_credential_fallback,
    )
    log.info(
        "binance_credentials_resolved",
        command="live-run",
        environment="live",
        account_label=account_label,
        **credentials.metadata(),
    )

    try:
        runtime_config = resolve_live_runtime_config(
            LiveRunOptions(
                database_url=database_url,
                account_label=account_label,
                strategy=strategy,
                runtime_manifest=runtime_manifest,
                impulse_window_buckets=impulse_window_buckets,
                confirmation_buckets=confirmation_buckets,
                min_return_pct=min_return_pct,
                min_imbalance=min_imbalance,
                min_intensity=min_intensity,
                min_notional_5m_vs_30m=min_notional_5m_vs_30m,
                cooldown_buckets=cooldown_buckets,
                market_environment=market_environment,
                market_state_source=market_state_source,
                market_state_hub_url=market_state_hub_url,
                market_quote_hub_url=market_quote_hub_url,
                market_quote_volume_hub_url=market_quote_volume_hub_url,
                market_websocket_url=market_websocket_url,
                account_event_hub_url=account_event_hub_url,
                risk_control_hub_url=risk_control_hub_url,
                session_id=session_id,
                operator=operator,
                lease_owner=lease_owner,
                strategy_config_hash=strategy_config_hash,
                git_commit_hash=git_commit_hash,
                migration_revision=migration_revision,
                max_runtime_seconds=max_runtime_seconds,
                poll_interval_seconds=poll_interval_seconds,
                checkpoint_every_states=checkpoint_every_states,
                hedge_mode=hedge_mode,
                exit_mode=exit_mode,
                take_profit_pct=take_profit_pct,
                stop_loss_pct=stop_loss_pct,
                entry_long_only=entry_long_only,
                entry_leverage=entry_leverage,
                margin_type=margin_type,
                entry_positive_gainer_top_count=entry_positive_gainer_top_count,
                entry_price_above_ema5=entry_price_above_ema5,
                entry_price_above_ema10=entry_price_above_ema10,
                entry_order_type=entry_order_type,
                entry_limit_ttl_seconds=entry_limit_ttl_seconds,
                candle_grace_bars=candle_grace_bars,
                candle_grace_decision_profit_pct=candle_grace_decision_profit_pct,
                candle_grace_profit_pct=candle_grace_profit_pct,
                base_url=base_url,
                entry_policy_compare_only=entry_policy_compare_only,
                entry_policy_enforce=entry_policy_enforce,
                acknowledge_missing_shadow_preflight=(
                    acknowledge_missing_shadow_preflight
                ),
                persist_exchange_operations=persist_exchange_operations,
            ),
            credentials=credentials,
        )
    except LiveRuntimeOptionsError as error:
        raise typer.BadParameter(str(error)) from error

    async def run_once(stop_requested: asyncio.Event) -> LiveDaemonResult:
        return await _run_live_daemon(
            config=runtime_config,
            shutdown_requested=stop_requested,
        )

    result = asyncio.run(_run_live_with_signal_handlers(run_once))
    typer.echo(json.dumps(asdict(result), default=str, sort_keys=True))


@app.command("status")
def status_command(
    session_id: Annotated[str, typer.Option("--session-id")],
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    transition = asyncio.run(_load_transition(_database_url(database_url), session_id))
    typer.echo(
        json.dumps(
            None if transition is None else asdict(transition),
            default=str,
        )
    )


@app.command("disable-new-entries")
def disable_new_entries_command(
    session_id: Annotated[str, typer.Option("--session-id")],
    operator: Annotated[str, typer.Option("--operator")],
    strategy_config_hash: Annotated[str, typer.Option("--strategy-config-hash")],
    risk_config_hash: Annotated[str, typer.Option("--risk-config-hash")],
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    risk_control_hub_url: Annotated[
        str,
        typer.Option("--risk-control-hub-url"),
    ] = "",
    risk_control_hub_token: Annotated[
        str | None,
        typer.Option(
            "--risk-control-hub-token",
            help="Optional token; CML_RISK_CONTROL_HUB_TOKEN is used when omitted.",
        ),
    ] = None,
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    resolved_risk_control_hub_url = (
        risk_control_hub_url.strip()
        or os.environ.get("CML_RISK_CONTROL_HUB_URL", "").strip()
    )
    transition = asyncio.run(
        _save_transition(
            _database_url(database_url),
            session_id,
            operator,
            strategy_config_hash,
            risk_config_hash,
            LiveSessionState.DRAINING,
            "operator_disabled_new_entries",
        )
    )
    push_error: Exception | None = None
    if resolved_risk_control_hub_url:
        event = RiskControlEvent(
            environment="live",
            account_label=account_label,
            strategy_name=strategy,
            session_id=session_id,
            action=RiskControlAction.DRAIN,
            event_id=transition.transition_id,
            command_id=transition.transition_id,
            reason="operator_disabled_new_entries",
            issued_at=transition.occurred_at,
            details={"transition_id": transition.transition_id},
        )
        try:
            asyncio.run(
                _publish_risk_control_event(
                    url=resolved_risk_control_hub_url,
                    token=risk_control_hub_token,
                    event=event,
                )
            )
        except Exception as error:
            push_error = error
            log.warning(
                "risk_control_push_failed_db_fallback_active",
                error_type=type(error).__name__,
            )
    if push_error is None:
        typer.echo("Live session is draining")
    else:
        typer.echo(
            "Live session is draining "
            "(risk-control push unavailable; PostgreSQL fallback remains active)"
        )


@app.command("cancel-all-open-entries")
def cancel_all_open_entries_command(
    session_id: Annotated[str, typer.Option("--session-id")],
    operator: Annotated[str, typer.Option("--operator")],
    idempotency_key: Annotated[str, typer.Option("--idempotency-key")],
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    risk_control_hub_url: Annotated[
        str,
        typer.Option("--risk-control-hub-url"),
    ] = "",
    risk_control_hub_token: Annotated[
        str | None,
        typer.Option(
            "--risk-control-hub-token",
            help="Optional token; CML_RISK_CONTROL_HUB_TOKEN is used when omitted.",
        ),
    ] = None,
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    """Durably request cancellation of all live opening orders."""

    _issue_one_shot_risk_control_command(
        action=RiskControlAction.CANCEL_ALL_OPEN_ENTRIES,
        command_type=CANCEL_ALL_OPEN_ENTRIES_COMMAND,
        confirmation_text=CANCEL_ALL_OPEN_ENTRIES_CONFIRMATION,
        reason="operator_cancelled_all_open_entries",
        session_id=session_id,
        operator=operator,
        idempotency_key=idempotency_key,
        confirmation=confirmation,
        account_label=account_label,
        strategy=strategy,
        risk_control_hub_url=risk_control_hub_url,
        risk_control_hub_token=risk_control_hub_token,
        database_url=database_url,
    )


@app.command("request-flatten")
def request_flatten_command(
    session_id: Annotated[str, typer.Option("--session-id")],
    operator: Annotated[str, typer.Option("--operator")],
    idempotency_key: Annotated[str, typer.Option("--idempotency-key")],
    confirmation: Annotated[str, typer.Option("--confirmation")] = "",
    account_label: Annotated[str, typer.Option("--account-label")] = "primary",
    strategy: Annotated[str, typer.Option("--strategy")] = "orderflow_impulse",
    risk_control_hub_url: Annotated[
        str,
        typer.Option("--risk-control-hub-url"),
    ] = "",
    risk_control_hub_token: Annotated[
        str | None,
        typer.Option(
            "--risk-control-hub-token",
            help="Optional token; CML_RISK_CONTROL_HUB_TOKEN is used when omitted.",
        ),
    ] = None,
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    """Durably request a reduce-only flatten through the live exit lane."""

    _issue_one_shot_risk_control_command(
        action=RiskControlAction.REQUEST_FLATTEN,
        command_type=EMERGENCY_FLATTEN_COMMAND,
        confirmation_text=EMERGENCY_FLATTEN_CONFIRMATION,
        reason="operator_requested_flatten",
        session_id=session_id,
        operator=operator,
        idempotency_key=idempotency_key,
        confirmation=confirmation,
        account_label=account_label,
        strategy=strategy,
        risk_control_hub_url=risk_control_hub_url,
        risk_control_hub_token=risk_control_hub_token,
        database_url=database_url,
    )


@app.command("report")
def report_command(
    session_id: Annotated[str, typer.Option("--session-id")],
    database_url: Annotated[str | None, typer.Option("--database-url")] = None,
) -> None:
    transition = asyncio.run(_load_transition(_database_url(database_url), session_id))
    typer.echo(
        json.dumps(
            None if transition is None else asdict(transition),
            default=str,
        )
    )


def _validate_missing_order_resolution(
    *,
    state: str,
    reduce_only: bool,
    exchange_order_id: str | None,
    created_at: datetime,
    now: datetime,
    order_quantity: Decimal,
    executed_quantity: Decimal,
    position_quantity: Decimal,
    exchange_order_found: bool,
    matching_open_order_found: bool,
    min_missing_age_seconds: float,
) -> None:
    """Compatibility entry point for the operator safety guard."""
    _validate_missing_order_resolution_impl(
        state=state,
        reduce_only=reduce_only,
        exchange_order_id=exchange_order_id,
        created_at=created_at,
        now=now,
        order_quantity=order_quantity,
        executed_quantity=executed_quantity,
        position_quantity=position_quantity,
        exchange_order_found=exchange_order_found,
        matching_open_order_found=matching_open_order_found,
        min_missing_age_seconds=min_missing_age_seconds,
    )




def _load_plan(path: Path) -> OrderExecutionPlan:
    payload = json.loads(path.read_text(encoding="utf-8"))
    plan = OrderExecutionPlan(
        intent_id=str(payload["intent_id"]),
        run_id=str(payload["run_id"]),
        client_order_id=str(payload["client_order_id"]),
        symbol=str(payload["symbol"]),
        side=str(payload["side"]),
        order_type=str(payload["order_type"]),
        quantity=Decimal(str(payload["quantity"])),
        price=None if payload.get("price") is None else Decimal(str(payload["price"])),
        reduce_only=bool(payload["reduce_only"]),
        created_at=datetime.fromisoformat(str(payload["created_at"])),
        position_side=FuturesPositionSide(
            str(payload.get("position_side", FuturesPositionSide.BOTH.value))
        ),
        quantized=bool(payload.get("quantized", False)),
        time_in_force=(
            None
            if payload.get("time_in_force") is None
            else str(payload["time_in_force"])
        ),
        expires_at=(
            None
            if payload.get("expires_at") is None
            else datetime.fromisoformat(str(payload["expires_at"]))
        ),
    )
    if not plan.quantized:
        raise typer.BadParameter("order plan must be quantized")
    return plan




def _validate_hex_hash(
    raw_value: str,
    option_name: str,
    expected_length: int,
) -> str:
    """Normalize and validate an operator-supplied immutable hash value."""

    value = raw_value.strip().lower()
    if len(value) != expected_length or _HEX_HASH_PATTERN.fullmatch(value) is None:
        raise typer.BadParameter(
            f"{option_name} must be exactly {expected_length} lowercase hex characters"
        )
    return value


def _runtime_strategy_config_hash(strategy_name: str) -> str:
    """Compute the hash from the same environment values used by Live run."""

    runtime_config = _preflight_runtime_strategy_config()
    return _live_strategy_config_hash(
        strategy_name,
        profile=runtime_config.profile,
        entry_positive_gainer_top_count=runtime_config.entry_positive_gainer_top_count,
        require_price_above_ema5=runtime_config.require_price_above_ema5,
        require_price_above_ema10=runtime_config.require_price_above_ema10,
        entry_policy_enforce=runtime_config.entry_policy_enforce,
        entry_order_type=runtime_config.entry_order_type,
        entry_limit_ttl_seconds=runtime_config.entry_limit_ttl_seconds,
    )


async def _latest_risk_config_hash(
    database_url: str,
    account_label: str,
) -> str:
    engine = create_execution_database_engine(database_url)
    try:
        config = await _latest_risk_config(
            async_sessionmaker(engine, expire_on_commit=False),
            account_label,
        )
        return _validate_hex_hash(
            config.config_hash,
            "latest risk config hash",
            _CONFIG_HASH_LENGTH,
        )
    finally:
        await engine.dispose()


async def _load_active_approval(
    database_url: str,
    account_label: str,
    strategy_name: str,
    now: datetime,
) -> LiveOperatorApproval | None:
    engine = create_execution_database_engine(database_url)
    try:
        return await PostgresLiveRolloutRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        ).load_active_approval(
            account_label=account_label,
            strategy_name=strategy_name,
            now=now,
        )
    finally:
        await engine.dispose()


async def _approval_binding_summary(
    database_url: str,
    account_label: str,
    strategy_name: str,
    *,
    expected_git_commit: str,
    expected_migration_revision: str,
) -> dict[str, object]:
    """Check only the approval identity needed before non-Live convergence."""

    approval = await _load_active_approval(
        database_url=database_url,
        account_label=account_label,
        strategy_name=strategy_name,
        now=datetime.now(tz=UTC),
    )
    normalized_git_commit = expected_git_commit.strip().lower()
    normalized_migration_revision = expected_migration_revision.strip()
    approved_git_commit_hash = (
        None if approval is None else approval.git_commit_hash
    )
    approved_migration_revision = (
        None if approval is None else approval.database_migration_revision
    )
    checks = {
        "approval_present": approval is not None,
        "approval_git_commit_matches_expected": (
            approval is not None
            and approval.git_commit_hash.strip().lower() == normalized_git_commit
        ),
        "approval_migration_matches_expected": (
            approval is not None
            and approval.database_migration_revision == normalized_migration_revision
        ),
    }
    errors = [name for name, passed in checks.items() if not passed]
    return {
        "account_label": account_label,
        "strategy": strategy_name,
        "approved_git_commit_hash": approved_git_commit_hash,
        "approved_migration_revision": approved_migration_revision,
        "expected_git_commit": normalized_git_commit,
        "expected_migration_revision": normalized_migration_revision,
        "approval_precheck_checks": checks,
        "approval_precheck_errors": errors,
        "approval_precheck_ok": not errors,
    }


async def _prepare_live_risk_gates(
    *,
    database_url: str,
    account_label: str,
    strategy_name: str,
    lease_owner: str,
    code_generation: str,
    lease_ttl_seconds: int,
    max_order_notional: Decimal | None,
    max_gross_notional: Decimal | None,
    max_daily_loss: Decimal | None,
    max_open_positions: int | None,
    profile: LiveOrderFlowImpulseProfile,
    entry_positive_gainer_top_count: int | None,
    require_price_above_ema5: bool,
    require_price_above_ema10: bool,
    entry_policy_enforce: bool,
    entry_order_type: EntryType,
    entry_limit_ttl_seconds: int,
) -> dict[str, str]:
    now = datetime.now(tz=UTC)
    risk_config = RiskConfigSnapshot(
        environment="live",
        account_label=account_label,
        max_order_notional=max_order_notional,
        max_gross_notional=max_gross_notional,
        max_daily_loss=max_daily_loss,
        max_open_positions=max_open_positions,
        max_market_state_age_seconds=_LIVE_UNENFORCED_STATE_AGE_SECONDS,
        max_account_state_age_seconds=_LIVE_UNENFORCED_STATE_AGE_SECONDS,
        allow_reduce_only_while_draining=True,
        created_at=now,
    )
    lease = TradingLease(
        lease_id=f"lease-{uuid4()}",
        environment="live",
        account_label=account_label,
        strategy_name=strategy_name,
        owner=lease_owner,
        code_generation=code_generation,
        state=TradingLeaseState.ACTIVE,
        acquired_at=now,
        expires_at=now + timedelta(seconds=lease_ttl_seconds),
    )
    engine = create_execution_database_engine(database_url)
    try:
        repository = PostgresRiskRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        )
        await repository.save_risk_config(risk_config)
        await repository.acquire_lease(lease)
    finally:
        await engine.dispose()
    return {
        "lease_id": lease.lease_id,
        "lease_expires_at": lease.expires_at.isoformat(),
        "risk_config_hash": risk_config.config_hash,
        "strategy_config_hash": _live_strategy_config_hash(
            strategy_name,
            profile=profile,
            entry_positive_gainer_top_count=entry_positive_gainer_top_count,
            require_price_above_ema5=require_price_above_ema5,
            require_price_above_ema10=require_price_above_ema10,
            entry_policy_enforce=entry_policy_enforce,
            entry_order_type=entry_order_type,
            entry_limit_ttl_seconds=entry_limit_ttl_seconds,
        ),
    }


async def _renew_live_lease(
    *,
    database_url: str,
    account_label: str,
    strategy_name: str,
    lease_owner: str,
    lease_ttl_seconds: int,
    code_generation: str | None = None,
) -> dict[str, str]:
    if lease_ttl_seconds < 300:
        raise ValueError("lease_ttl_seconds must be at least 300")
    now = datetime.now(tz=UTC)
    engine = create_execution_database_engine(database_url)
    try:
        repository = PostgresRiskRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        )
        lease = await repository.load_active_lease("live", account_label, now)
        if lease is None:
            raise RuntimeError(
                f"active live lease is missing for account {account_label}"
            )
        if lease.owner != lease_owner:
            raise RuntimeError(f"live lease owner mismatch for account {account_label}")
        if lease.strategy_name != strategy_name:
            raise RuntimeError(
                f"live lease strategy mismatch for account {account_label}"
            )
        if code_generation is not None:
            renewed = await repository.renew_lease(
                lease_id=lease.lease_id,
                owner=lease_owner,
                expires_at=now + timedelta(seconds=lease_ttl_seconds),
                code_generation=code_generation,
            )
        else:
            renewed = await repository.renew_lease(
                lease_id=lease.lease_id,
                owner=lease_owner,
                expires_at=now + timedelta(seconds=lease_ttl_seconds),
            )
    finally:
        await engine.dispose()
    return {
        "account_label": renewed.account_label,
        "lease_id": renewed.lease_id,
        "lease_owner": renewed.owner,
        "lease_expires_at": renewed.expires_at.isoformat(),
    }


async def _save_approval(
    database_url: str,
    approval: LiveOperatorApproval,
) -> None:
    engine = create_execution_database_engine(database_url)
    try:
        repository = PostgresLiveRolloutRepository(
            async_sessionmaker(engine, expire_on_commit=False)
        )
        await repository.save_approval(approval)
    finally:
        await engine.dispose()


_UNLIMITED_VALUES = frozenset({"none", "unlimited"})


def _resolve_live_entry_positive_gainer_top_count(value: int | None) -> int:
    try:
        return _resolve_runtime_top_count(value)
    except LiveRuntimeOptionsError as error:
        raise typer.BadParameter(str(error)) from error


def _resolve_live_profile_options(
    *,
    impulse_window_buckets: int | None,
    confirmation_buckets: int | None,
    min_return_pct: str | None,
    min_imbalance: str | None,
    min_intensity: str | None,
    min_notional_5m_vs_30m: str | None,
    cooldown_buckets: int | None,
) -> LiveOrderFlowImpulseProfile:
    try:
        return _resolve_runtime_profile_options(
            impulse_window_buckets=impulse_window_buckets,
            confirmation_buckets=confirmation_buckets,
            min_return_pct=min_return_pct,
            min_imbalance=min_imbalance,
            min_intensity=min_intensity,
            min_notional_5m_vs_30m=min_notional_5m_vs_30m,
            cooldown_buckets=cooldown_buckets,
        )
    except LiveRuntimeOptionsError as error:
        raise typer.BadParameter(str(error)) from error


def _parse_exchange_operations(
    raw_value: str,
) -> frozenset[str] | None:
    try:
        return _parse_runtime_exchange_operations(raw_value)
    except LiveRuntimeOptionsError as error:
        raise typer.BadParameter(str(error)) from error


def _parse_optional_decimal_limit(
    raw_value: str,
    option_name: str,
) -> Decimal | None:
    normalized = raw_value.strip().lower()
    if normalized in _UNLIMITED_VALUES:
        return None
    try:
        value = Decimal(normalized)
    except InvalidOperation as error:
        raise typer.BadParameter(
            f"{option_name} must be positive or 'unlimited'"
        ) from error
    if not value.is_finite() or value <= 0:
        raise typer.BadParameter(f"{option_name} must be positive or 'unlimited'")
    return value


def _parse_optional_integer_limit(
    raw_value: str,
    option_name: str,
) -> int | None:
    normalized = raw_value.strip().lower()
    if normalized in _UNLIMITED_VALUES:
        return None
    try:
        value = int(normalized)
    except ValueError as error:
        raise typer.BadParameter(
            f"{option_name} must be a positive integer or 'unlimited'"
        ) from error
    if value <= 0:
        raise typer.BadParameter(
            f"{option_name} must be a positive integer or 'unlimited'"
        )
    return value


def _parse_approval_expiration(
    now: datetime,
    expires_in_minutes: str,
) -> datetime | None:
    normalized = expires_in_minutes.strip().lower()
    if normalized in {"never", *_UNLIMITED_VALUES}:
        return None
    try:
        minutes = int(normalized)
    except ValueError as error:
        raise typer.BadParameter(
            "--expires-in-minutes must be a positive integer or 'never'"
        ) from error
    if minutes <= 0:
        raise typer.BadParameter(
            "--expires-in-minutes must be a positive integer or 'never'"
        )
    return now + timedelta(minutes=minutes)


async def _preflight_summary(
    database_url: str,
    account_label: str,
    strategy_name: str,
    *,
    expected_git_commit: str | None = None,
    expected_migration_revision: str | None = None,
    expected_lease_owner: str | None = None,
    expected_strategy_config_hash: str | None = None,
) -> dict[str, object]:
    now = datetime.now(tz=UTC)
    engine = create_execution_database_engine(database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        approval = await PostgresLiveRolloutRepository(factory).load_active_approval(
            account_label=account_label,
            strategy_name=strategy_name,
            now=now,
        )
        lease = await PostgresRiskRepository(factory).load_active_lease(
            "live", account_label, now
        )
        unresolved = await PostgresOrderRepository(factory).load_unresolved_orders()
        risk_config = await _latest_risk_config(factory, account_label)
        runtime_config = _preflight_runtime_strategy_config()
        runtime_strategy_config_hash = _live_strategy_config_hash(
            strategy_name,
            profile=runtime_config.profile,
            entry_positive_gainer_top_count=(
                runtime_config.entry_positive_gainer_top_count
            ),
            require_price_above_ema5=runtime_config.require_price_above_ema5,
            require_price_above_ema10=runtime_config.require_price_above_ema10,
            entry_policy_enforce=runtime_config.entry_policy_enforce,
            entry_order_type=runtime_config.entry_order_type,
            entry_limit_ttl_seconds=runtime_config.entry_limit_ttl_seconds,
        )
        configured_strategy_config_hash = (
            os.environ.get("CML_LIVE_STRATEGY_CONFIG_HASH", "").strip().lower() or None
        )
        if configured_strategy_config_hash == "unset":
            configured_strategy_config_hash = None
        approved_strategy_config_hash = (
            None if approval is None else approval.strategy_config_hash
        )
        account_state = await _latest_account_state(factory, account_label)
        approval_risk_config_hash = (
            None if approval is None else approval.risk_config_hash
        )
        approval_git_commit_hash = (
            None if approval is None else approval.git_commit_hash
        )
        approval_migration_revision = (
            None if approval is None else approval.database_migration_revision
        )
        checks: dict[str, bool] = {
            "approval_present": approval is not None,
            "lease_present": lease is not None,
            "account_ready": account_state.value == "ready_readonly",
            "runtime_strategy_config_matches_approval": (
                approval is not None
                and runtime_strategy_config_hash == approved_strategy_config_hash
            ),
            "risk_config_matches_approval": (
                approval is not None
                and risk_config.config_hash == approval_risk_config_hash
            ),
        }
        if configured_strategy_config_hash is not None:
            checks["runtime_strategy_config_matches_configured"] = (
                runtime_strategy_config_hash == configured_strategy_config_hash
            )
        if expected_git_commit is not None:
            checks["approval_git_commit_matches_expected"] = (
                approval_git_commit_hash == expected_git_commit.strip().lower()
            )
        if expected_migration_revision is not None:
            checks["approval_migration_matches_expected"] = (
                approval_migration_revision == expected_migration_revision.strip()
            )
        if expected_lease_owner is not None:
            checks["lease_owner_matches_expected"] = (
                lease is not None and lease.owner == expected_lease_owner
            )
        if expected_strategy_config_hash is not None:
            checks["runtime_strategy_config_matches_manifest"] = (
                runtime_strategy_config_hash == expected_strategy_config_hash
            )
        preflight_errors = [name for name, passed in checks.items() if not passed]
        return {
            "approval_present": approval is not None,
            "lease_present": lease is not None,
            "account_state": account_state.value,
            "unresolved_order_count": len(unresolved),
            "risk_config_hash": risk_config.config_hash,
            "approved_risk_config_hash": approval_risk_config_hash,
            "runtime_strategy_config_hash": runtime_strategy_config_hash,
            "configured_strategy_config_hash": configured_strategy_config_hash,
            "approved_strategy_config_hash": approved_strategy_config_hash,
            "approved_git_commit_hash": approval_git_commit_hash,
            "approved_migration_revision": approval_migration_revision,
            "expected_lease_owner": expected_lease_owner,
            "expected_strategy_config_hash": expected_strategy_config_hash,
            "preflight_checks": checks,
            "preflight_errors": preflight_errors,
            "preflight_ok": not preflight_errors,
            "runtime_strategy_config_matches_configured": (
                None
                if configured_strategy_config_hash is None
                else runtime_strategy_config_hash == configured_strategy_config_hash
            ),
            "runtime_strategy_config_matches_approval": (
                None
                if approved_strategy_config_hash is None
                else runtime_strategy_config_hash == approved_strategy_config_hash
            ),
            "runtime_strategy_config_inputs": {
                **runtime_config.profile.as_dict(),
                "entry_positive_gainer_top_count": (
                    runtime_config.entry_positive_gainer_top_count
                ),
                "require_price_above_ema5": runtime_config.require_price_above_ema5,
                "require_price_above_ema10": runtime_config.require_price_above_ema10,
                "entry_policy_mode": runtime_config.entry_policy_mode,
                "entry_order_type": runtime_config.entry_order_type.value,
                "entry_limit_ttl_seconds": runtime_config.entry_limit_ttl_seconds,
            },
        }
    finally:
        await engine.dispose()


def _preflight_runtime_strategy_config() -> _PreflightRuntimeStrategyConfig:
    """Resolve the Live hash inputs exposed to one-off preflight containers.

    Compose passes the account-scoped profile, top-N, and policy-mode values
    into the long-running Live service environment. Keeping this resolver
    beside the diagnostic makes the reported hash explainable instead of
    silently using the library defaults (which may describe a different lane).
    """

    raw_top_count = os.environ.get(
        "CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT",
        str(_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT),
    ).strip()
    try:
        top_count = int(raw_top_count)
    except ValueError as error:
        raise ValueError(
            "CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT must be an integer"
        ) from error
    if top_count <= 0:
        raise ValueError("CML_LIVE_ENTRY_POSITIVE_GAINER_TOP_COUNT must be positive")

    policy_mode = (
        os.environ.get("CML_LIVE_ENTRY_POLICY_MODE", "enforce").strip().lower()
        or "enforce"
    )
    if policy_mode not in _LIVE_ENTRY_POLICY_MODES:
        raise ValueError(
            "CML_LIVE_ENTRY_POLICY_MODE must be one of: "
            + ", ".join(sorted(_LIVE_ENTRY_POLICY_MODES))
        )
    try:
        profile = LiveOrderFlowImpulseProfile.from_environment()
    except ValueError as error:
        raise ValueError(str(error)) from error
    return _PreflightRuntimeStrategyConfig(
        profile=profile,
        entry_positive_gainer_top_count=top_count,
        require_price_above_ema5=_LIVE_ENTRY_PRICE_ABOVE_EMA5,
        require_price_above_ema10=_LIVE_ENTRY_PRICE_ABOVE_EMA10,
        entry_policy_mode=policy_mode,
        entry_order_type=_LIVE_ENTRY_ORDER_TYPE,
        entry_limit_ttl_seconds=_LIVE_ENTRY_LIMIT_TTL_SECONDS,
    )


async def _approved_intent_notional(
    factory: async_sessionmaker[AsyncSession],
    intent_id: str,
) -> Decimal | None:
    async with factory() as session:
        details = await session.scalar(
            select(OrderIntentExecutionRow.details).where(
                OrderIntentExecutionRow.intent_id == intent_id
            )
        )
    if not isinstance(details, dict):
        return None
    value = details.get("desired_notional")
    return None if value is None else Decimal(str(value))


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
            async_sessionmaker(engine, expire_on_commit=False)
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
            database_url=_database_url(database_url),
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


def _resolve_live_cli_credentials(
    *,
    api_key_env: str | None,
    api_secret_env: str | None,
    allow_legacy_fallback: bool,
) -> ResolvedBinanceCredentials:
    try:
        return resolve_role_credentials(
            BinanceCredentialRole.TRADE,
            api_key_env=api_key_env,
            api_secret_env=api_secret_env,
            allow_legacy_fallback=allow_legacy_fallback,
        )
    except CredentialResolutionError as error:
        raise typer.BadParameter(str(error)) from error


def _database_url(value: str | None) -> str:
    """Resolve the execution plane for legacy live CLI commands."""

    return _execution_database_url(value)
