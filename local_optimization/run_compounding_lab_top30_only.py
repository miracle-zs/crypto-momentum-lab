#!/usr/bin/env python3
"""Run Compounding Lab comparison on Top 30 opportunities only.

Preserves the existing 3-view 6-scenario grid optimization and reconciliation data,
recalculating only the Compounding Lab comparison using Top 30 opportunities.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
if str(ROOT_DIR / "src") not in sys.path:
    sys.path.insert(0, str(ROOT_DIR / "src"))

from local_optimization.generate_six_scenarios_dashboard import (  # noqa: E402
    BEIJING_TZ,
    CACHE_PRICE_FILE,
    DEFAULT_DATA_DIR,
    DEFAULT_GOLD_PROFILE,
    DEFAULT_PARQUET_DIR,
    DEFAULT_TOP30_CACHE,
    INITIAL_EQUITY,
    LEVERAGE,
    OUTPUT_HTML_REPORT,
    TEMPLATE_FILE,
    evaluate_compounding_comparison,
    filter_opportunities_by_top10,
    load_all_replay_events,
    load_cached_price_series,
    load_top10_lookup,
)


def run_compounding_top30_update() -> None:
    html_path = OUTPUT_HTML_REPORT
    if not html_path.exists():
        raise FileNotFoundError(f"Dashboard HTML not found at {html_path}")

    print(f"📖 Reading existing dashboard: {html_path}")
    existing_html = html_path.read_text(encoding="utf-8")

    # Extract RAW_DATA from existing html
    pattern = r"const RAW_DATA = ({.*?});\n\s*if \(RAW_DATA\.version\)"
    match = re.search(pattern, existing_html, re.DOTALL)
    if not match:
        raise ValueError("Could not extract RAW_DATA JSON from existing HTML")

    raw_data = json.loads(match.group(1))
    views_keys = list(raw_data.get("views", {}).keys())
    print(f"✅ Successfully extracted existing views: {views_keys}")

    # Load 15s price streams & replay events
    print("⏳ Loading cached price series...")
    prices_by_symbol = load_cached_price_series(CACHE_PRICE_FILE)
    print(f"✅ Loaded price streams for {len(prices_by_symbol)} symbols.")

    print("⏳ Loading replay events...")
    events, manifest = load_all_replay_events(DEFAULT_DATA_DIR)
    print(f"✅ Loaded {len(events):,} deduplicated replay events.")

    # Load Top 30 events
    print("⏳ Filtering events to Top 30 universe...")
    top30_lookup = load_top10_lookup(
        DEFAULT_PARQUET_DIR, cache_path=DEFAULT_TOP30_CACHE, max_rank=30
    )
    events_top30 = filter_opportunities_by_top10(events, top30_lookup)
    print(
        f"⚡ Top 30 机会池: {len(events_top30):,} / {len(events):,} 机会 "
        f"({len(events_top30) / max(1, len(events)) * 100:.1f}%)"
    )

    scheduled_risk_window = None
    if raw_data.get("scheduled_risk_window", {}).get("enabled"):
        from crypto_momentum_lab.live_rollout.scheduled_risk_window import (
            ScheduledRiskWindowConfig,
        )

        srw = raw_data["scheduled_risk_window"]
        scheduled_risk_window = ScheduledRiskWindowConfig(
            enabled=True,
            timezone=srw.get("timezone", "Asia/Shanghai"),
            flatten_time=srw.get("flatten_time", "07:45"),
            reopen_time=srw.get("reopen_time", "09:00"),
        )

    # Evaluate Compounding Lab on Top 30
    print("\n=== [Compounding Lab] 正在解算 Top 30 下的 5 种资金管理与截断模式对比 ===")
    compounding_lab_data = evaluate_compounding_comparison(
        events=events_top30,
        prices_by_symbol=prices_by_symbol,
        params=DEFAULT_GOLD_PROFILE,
        manifest=manifest,
        scheduled_risk_window=scheduled_risk_window,
        initial_equity=INITIAL_EQUITY,
        f=0.10,
        leverage=LEVERAGE,
        universe="top30",
        universe_label="涨幅榜 Top 30 (全域扩展池)",
    )

    # Update RAW_DATA
    raw_data["compounding_lab"] = compounding_lab_data

    # Version tag
    version_tag = datetime.now(tz=BEIJING_TZ).strftime("%Y%m%d_%H%M%S")
    raw_data["version"] = f"v{datetime.now(tz=BEIJING_TZ).strftime('%Y.%m.%d')}"
    raw_data["generated_at"] = datetime.now(tz=BEIJING_TZ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # Render template
    template_content = TEMPLATE_FILE.read_text(encoding="utf-8")
    new_payload = json.dumps(raw_data, ensure_ascii=False)
    rendered_html = template_content.replace("__DATA_JSON__", new_payload)

    # Write output
    html_path.write_text(rendered_html, encoding="utf-8")
    print(f"✅ 更新完成，报告已写回: {html_path}")

    # History
    history_dir = html_path.parent / "history"
    history_dir.mkdir(parents=True, exist_ok=True)
    v_path = history_dir / f"six_scenarios_equity_comparison_{version_tag}.html"
    v_path.write_text(rendered_html, encoding="utf-8")
    print(f"✅ 历史版本归档至: {v_path}")

    # Artifact sync
    artifact_base = Path("/Users/zhangshuai/.gemini/antigravity/brain")
    artifact_dir = artifact_base / "926dc2ee-4344-4489-a275-387bac0c367a"
    if artifact_dir.exists():
        art_path = artifact_dir / "six_scenarios_equity_comparison.html"
        art_path.write_text(rendered_html, encoding="utf-8")
        art_hist_dir = artifact_dir / "history"
        art_hist_dir.mkdir(parents=True, exist_ok=True)
        art_v_path = (
            art_hist_dir / f"six_scenarios_equity_comparison_{version_tag}.html"
        )
        art_v_path.write_text(rendered_html, encoding="utf-8")
        print(f"✅ 报告已同步至 Artifact 目录: {art_path}")


if __name__ == "__main__":
    run_compounding_top30_update()
