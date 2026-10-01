"""Account synchronization inputs and results independent of orchestration."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from crypto_momentum_lab.domain.account.models import (
    AccountFillEvent,
    AccountFillLoadScan,
    AccountFillReconciliationCursor,
    AccountFillSourceAnchor,
    ExecutionAccountStatus,
)
from crypto_momentum_lab.execution_account.baseline_checkpoint import (
    AccountBaselineCheckpoint,
)
from crypto_momentum_lab.execution_account.snapshot_models import (
    AccountSnapshot,
    AccountSnapshotDelta,
)

type FillKey = tuple[str, str]

_DEFAULT_HISTORICAL_FILL_RECONCILIATION_BATCH_SIZE = 10


@dataclass(frozen=True, slots=True)
class ExecutionAccountSyncConfig:
    environment: str
    account_label: str
    expected_multi_assets_mode: bool
    expected_hedge_mode: bool
    observed_at: datetime
    recent_fill_symbols: tuple[str, ...] = ()
    recent_fill_cursors: Mapping[str, AccountFillReconciliationCursor] = field(
        default_factory=dict
    )
    fill_source_anchors: Mapping[tuple[str, str], AccountFillSourceAnchor | None] = (
        field(default_factory=dict)
    )
    historical_fill_reconciliation_interval_seconds: float = 6 * 60 * 60
    # A historical sweep is deliberately incremental.  Active symbols are
    # always included; this only bounds the closed-symbol backlog so a
    # reconciliation cannot starve account heartbeats for minutes.
    historical_fill_reconciliation_batch_size: int = (
        _DEFAULT_HISTORICAL_FILL_RECONCILIATION_BATCH_SIZE
    )

    def __post_init__(self) -> None:
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if not self.account_label.strip():
            raise ValueError("account_label must not be empty")
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        if any(not symbol.strip() for symbol in self.recent_fill_symbols):
            raise ValueError("recent_fill_symbols must not contain empty values")
        if self.historical_fill_reconciliation_interval_seconds <= 0:
            raise ValueError(
                "historical_fill_reconciliation_interval_seconds must be positive"
            )
        if self.historical_fill_reconciliation_batch_size <= 0:
            raise ValueError(
                "historical_fill_reconciliation_batch_size must be positive"
            )
        for symbol, cursor in self.recent_fill_cursors.items():
            normalized_symbol = symbol.strip().upper()
            if not normalized_symbol:
                raise ValueError("recent_fill_cursors must not contain empty keys")
            if cursor.symbol.strip().upper() != normalized_symbol:
                raise ValueError("recent_fill_cursors keys must match cursor symbols")
            if (
                cursor.environment != self.environment
                or cursor.account_label != self.account_label
            ):
                raise ValueError(
                    "recent_fill_cursors must match the sync account scope"
                )
        for identity, anchor in self.fill_source_anchors.items():
            if len(identity) != 2:
                raise ValueError("fill_source_anchors keys must be (symbol, side)")
            symbol, side = (part.strip().upper() for part in identity)
            if not symbol or not side:
                raise ValueError("fill_source_anchors keys must not be empty")
            if anchor is None:
                continue
            if (symbol, side) != (
                anchor.symbol.strip().upper(),
                anchor.position_side.strip().upper(),
            ):
                raise ValueError("fill_source_anchors keys must match anchor identity")


@dataclass(frozen=True, slots=True)
class ExecutionAccountSyncResult:
    status: ExecutionAccountStatus
    reconciliation_id: str
    mismatch_count: int
    snapshot: AccountSnapshot | None = None
    delta: AccountSnapshotDelta | None = None
    fill_count: int = 0
    fills: tuple[AccountFillEvent, ...] = ()
    new_fills: tuple[AccountFillEvent, ...] = ()
    new_fill_keys: frozenset[FillKey] = frozenset()
    fill_count_by_symbol: tuple[tuple[str, int], ...] = ()
    fill_cursor_updates: tuple[AccountFillReconciliationCursor, ...] = ()
    fill_load_scans: tuple[AccountFillLoadScan, ...] = ()
    fills_catching_up: bool = False
    baseline_checkpoint: AccountBaselineCheckpoint | None = None
