"""SQLite experiment cataloging and multi-dimensional HTML/Markdown daily reports.

Saves immutable experiment artifacts:
- Local SQLite database indexing runs, protocols, and daily track records
- Rich, standalone, styled HTML daily reports
- Clean Markdown reports
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from local_optimization.optimizer import CandidateEvaluation
from local_optimization.protocol import OptimizationProtocol
from local_optimization.reconciliation import (
    LayerSummary,
    ReconciliationReport,
)
from local_optimization.snapshot import SnapshotManifest
from local_optimization.tracker import DailyTrackRecord


class ExperimentCatalog:
    """Local SQLite catalog for optimization runs, protocols, and track history."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _get_conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_schema(self) -> None:
        with self._get_conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS protocols (
                    protocol_id TEXT PRIMARY KEY,
                    scenario_family TEXT NOT NULL,
                    json_content TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    target_cutoff TEXT NOT NULL,
                    is_complete INTEGER NOT NULL,
                    json_content TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    protocol_id TEXT NOT NULL,
                    snapshot_id TEXT NOT NULL,
                    executed_at TEXT NOT NULL,
                    json_content TEXT NOT NULL,
                    FOREIGN KEY (protocol_id) REFERENCES protocols(protocol_id),
                    FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id)
                );

                CREATE TABLE IF NOT EXISTS daily_tracks (
                    date_str TEXT NOT NULL,
                    protocol_id TEXT NOT NULL,
                    daily_best_id TEXT,
                    recommended_id TEXT,
                    live_id TEXT,
                    stability_status TEXT NOT NULL,
                    json_content TEXT NOT NULL,
                    PRIMARY KEY (date_str, protocol_id)
                );
                """
            )

    def save_protocol(self, protocol: OptimizationProtocol) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO protocols
                (protocol_id, scenario_family, json_content, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    protocol.protocol_id,
                    protocol.scenario_family,
                    json.dumps(protocol.to_dict()),
                    datetime.now(tz=UTC).isoformat(),
                ),
            )

    def save_snapshot(self, manifest: SnapshotManifest) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO snapshots
                (snapshot_id, target_cutoff, is_complete, json_content)
                VALUES (?, ?, ?, ?)
                """,
                (
                    manifest.snapshot_id,
                    manifest.target_cutoff,
                    1 if manifest.is_complete else 0,
                    json.dumps(manifest.to_dict()),
                ),
            )

    def save_daily_track(self, record: DailyTrackRecord) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO daily_tracks
                (date_str, protocol_id, daily_best_id, recommended_id,
                 live_id, stability_status, json_content)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.date_str,
                    record.protocol_id,
                    record.daily_best.candidate.parameter_id
                    if record.daily_best
                    else None,
                    record.recommended.candidate.parameter_id
                    if record.recommended
                    else None,
                    record.live_actual.candidate.parameter_id
                    if record.live_actual
                    else None,
                    record.stability_status,
                    json.dumps(record.to_dict()),
                ),
            )

    def load_track_history(
        self, protocol_id: str, limit: int = 30
    ) -> list[DailyTrackRecord]:
        with self._get_conn() as conn:
            cursor = conn.execute(
                """
                SELECT json_content FROM daily_tracks
                WHERE protocol_id = ?
                ORDER BY date_str DESC
                LIMIT ?
                """,
                (protocol_id, limit),
            )
            rows = cursor.fetchall()

        history: list[DailyTrackRecord] = []
        for (raw_json,) in reversed(rows):
            data = json.loads(raw_json)
            history.append(DailyTrackRecord.from_dict(data))
        return history


def generate_markdown_report(
    date_str: str,
    protocol: OptimizationProtocol,
    snapshot: SnapshotManifest,
    daily_record: DailyTrackRecord,
    pareto_frontier: Sequence[CandidateEvaluation],
    reconciliation: ReconciliationReport | None = None,
) -> str:
    """Generate a clean, structured Markdown daily report."""
    tags_str = ", ".join(snapshot.capability_tags)
    is_complete_str = "✅ Complete" if snapshot.is_complete else "⚠️ Partial"
    md = [
        f"# 本地参数寻优与稳定性日报 ({date_str})",
        "",
        f"- **场景**: `{protocol.scenario_family}`",
        f"- **协议版本**: `{protocol.protocol_id}`",
        f"- **快照**: `{snapshot.snapshot_id}`",
        f"- **数据完整性**: {is_complete_str} | **能力标签**: `{tags_str}`",
        "",
        "## 1. 三轨参数当前状态",
        "",
        "| 观察轨道 | 参数 ID | 净收益 | UI | MDD | 峰值保证金 | 交易数 | 稳定评分 |",
        "|---|---|---|---|---|---|---|---|",
    ]

    for label, eval_item in [
        ("今日最高 (Daily Best)", daily_record.daily_best),
        ("稳定推荐 (Recommended)", daily_record.recommended),
        ("实盘配置 (Live Actual)", daily_record.live_actual),
    ]:
        if eval_item:
            p_id = eval_item.candidate.parameter_id
            pnl = eval_item.net_pnl
            ui = eval_item.ulcer_index
            mdd = eval_item.max_drawdown_pct
            margin = eval_item.peak_initial_margin_usdt
            cnt = eval_item.trade_count
            stab = eval_item.neighborhood_stability_score
            md.append(
                f"| **{label}** | `{p_id}` | ${pnl:.2f} | {ui:.4f} | "
                f"{mdd:.2%} | ${margin:.1f} | {cnt} | {stab:.2f} |"
            )
        else:
            md.append(f"| **{label}** | *N/A* | - | - | - | - | - | - |")

    md.extend(
        [
            "",
            f"**阶段稳定判定**: `{daily_record.stability_status.upper()}`",
            *[f"- {note}" for note in daily_record.stability_notes],
            "",
            "## 2. Pareto 前沿 (收益 vs Ulcer Index vs 保证金)",
            "",
            "| 候选 ID | 参数摘要 | 净收益 | UI | MDD | 峰值保证金 | 交易数 |",
            "|---|---|---|---|---|---|---|",
        ]
    )

    for p in pareto_frontier[:10]:
        p_summary = ", ".join(
            f"{k}={v}" for k, v in list(p.candidate.params.items())[:3]
        )
        md.append(
            f"| `{p.candidate.parameter_id}` | {p_summary} | "
            f"${p.net_pnl:.2f} | {p.ulcer_index:.4f} | "
            f"{p.max_drawdown_pct:.2%} | ${p.peak_initial_margin_usdt:.1f} | "
            f"{p.trade_count} |"
        )

    if reconciliation:
        audit_tag = "✅ PASS" if reconciliation.is_audit_passed else "❌ DISCREPANCY"
        sig_lyr = reconciliation.layers.get(
            "signals", LayerSummary("signals", 0, 0, 0, 1, 1, 0)
        )
        fill_lyr = reconciliation.layers.get(
            "fills", LayerSummary("fills", 0, 0, 0, 1, 1, 0)
        )
        md.extend(
            [
                "",
                "## 3. 实盘 6 层对账与首个因果分歧",
                "",
                f"**审计通过**: {audit_tag}",
                f"- 信号对账: 精确率 `{sig_lyr.precision:.1%}`, "
                f"召回率 `{sig_lyr.recall:.1%}`",
                f"- 成交对账: 精确率 `{fill_lyr.precision:.1%}`, "
                f"召回率 `{fill_lyr.recall:.1%}`",
            ]
        )
        if reconciliation.first_divergence:
            fd = reconciliation.first_divergence
            md.append(
                f"- **首个因果分歧点**: `{fd.timestamp}` | `{fd.symbol}` | "
                f"**[{fd.layer.upper()}]** {fd.divergence_type}"
            )
        for note in reconciliation.audit_notes:
            md.append(f"  - {note}")

    return "\n".join(md)
