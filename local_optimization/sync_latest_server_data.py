#!/usr/bin/env python3
"""Sync latest market parquet data and 4-account postgres events.

Pulls continuous parquet partitions and Postgres event streams from production server.
Supports automatic remote date discovery, incremental sync, and health checking.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

SERVER_HOST = os.environ.get("SERVER_HOST", "43.167.191.253")
SERVER_PORT = os.environ.get("SERVER_PORT", "22")
SERVER_USER = os.environ.get("SERVER_USER", "root")
SERVER_PASSWORD = os.environ.get("CML_SSH_PASSWORD", "123456@abcdef")

LOCAL_DATA_DIR = SCRIPT_DIR / "data"
ALL_PARQUET_DIR = LOCAL_DATA_DIR / "all_data_parquet/environment=research"
DAILY_PARQUET_DIR = LOCAL_DATA_DIR / "parquet/environment=research"
LIVE_LATEST_DIR = LOCAL_DATA_DIR / "live_latest"

REMOTE_PARQUET_BASE = (
    "/var/lib/docker/volumes/crypto-momentum-lab_research-data"
    "/_data/parquet/environment=research"
)
REMOTE_PG_CONTAINER = "crypto-momentum-lab-postgres-1"

ACCOUNT_MAP = {
    "primary": "primary",
    "acc01": "account-2",
    "acc02": "account-3",
    "acc03": "account-4",
}


def run_ssh_command(cmd: str, timeout: int = 120) -> str:
    """Run remote SSH command and return stdout."""
    full_cmd = [
        "sshpass",
        "-p",
        SERVER_PASSWORD,
        "ssh",
        "-p",
        str(SERVER_PORT),
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "ConnectTimeout=10",
        f"{SERVER_USER}@{SERVER_HOST}",
        cmd,
    ]
    proc = subprocess.run(
        full_cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=True,
    )
    return proc.stdout


def check_server_health() -> bool:
    """Perform preflight connectivity check for SSH, Docker, and PostgreSQL."""
    print("🩺 正在检查服务器连接与生产容器就绪状态...")
    try:
        cmd = (
            "uname -s && docker ps --format '{{.Names}}' "
            "| grep -E 'postgres|live-strategy|execution' | wc -l"
        )
        out = run_ssh_command(cmd)
        lines = out.strip().splitlines()
        os_name = lines[0] if lines else "Unknown"
        container_count = int(lines[1]) if len(lines) > 1 else 0

        pg_check = run_ssh_command(
            f"docker exec {REMOTE_PG_CONTAINER} psql -U cml -d cml -c 'SELECT 1;' -t"
        ).strip()
        if "1" not in pg_check:
            raise RuntimeError(f"Unexpected postgres ping output: {pg_check}")

        print(f"  ✅ SSH 连接成功: 目标系统 {os_name}")
        print(f"  ✅ 核心实盘容器在线数: {container_count} 个")
        print(f"  ✅ PostgreSQL 数据库联通正常 (cml @ {REMOTE_PG_CONTAINER})")
        return True
    except Exception as e:
        print(f"  ❌ 服务器健康检查失败: {e}", file=sys.stderr)
        return False


def discover_remote_dates() -> dict[str, int]:
    """Query remote server for available parquet dates and their hour counts."""
    remote_script = (
        f"for d in {REMOTE_PARQUET_BASE}/date=*; do "
        "echo $(basename $d): $(ls -d $d/hour=* 2>/dev/null | wc -l); "
        "done"
    )
    try:
        out = run_ssh_command(remote_script, timeout=30)
    except Exception as e:
        print(f"  ⚠️ 查询远程日期失败: {e}", file=sys.stderr)
        return {}

    remote_dates: dict[str, int] = {}
    for line in out.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        prefix, count_str = line.split(":", 1)
        prefix = prefix.strip()
        if prefix.startswith("date="):
            dt_str = prefix[len("date=") :]
            try:
                remote_dates[dt_str] = int(count_str.strip())
            except ValueError:
                pass
    return remote_dates


def get_local_dates_summary() -> dict[str, int]:
    """Get local parquet dates and their hour counts."""
    local_dates: dict[str, int] = {}
    if not ALL_PARQUET_DIR.exists():
        return local_dates

    for d in ALL_PARQUET_DIR.glob("date=*"):
        if d.is_dir():
            dt_str = d.name[len("date=") :]
            hour_dirs = list(d.glob("hour=*"))
            local_dates[dt_str] = len(hour_dirs)
    return local_dates


def sync_parquet_date(date_str: str) -> bool:
    """Stream remote parquet partition via tar over SSH (deadlock-free)."""
    print(f"📦 同步行情切片: date={date_str}...")
    t0 = time.perf_counter()
    target_all = ALL_PARQUET_DIR / f"date={date_str}"
    target_daily = DAILY_PARQUET_DIR / f"date={date_str}"
    target_all.mkdir(parents=True, exist_ok=True)
    target_daily.mkdir(parents=True, exist_ok=True)

    remote_cmd = (
        f"if [ -d '{REMOTE_PARQUET_BASE}/date={date_str}' ]; then "
        f"tar -czf - -C '{REMOTE_PARQUET_BASE}/date={date_str}' . ; "
        f"else echo 'NOT_FOUND' >&2; exit 1; fi"
    )

    ssh_proc = subprocess.Popen(
        [
            "sshpass",
            "-p",
            SERVER_PASSWORD,
            "ssh",
            "-p",
            str(SERVER_PORT),
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            f"{SERVER_USER}@{SERVER_HOST}",
            remote_cmd,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    tar_proc = subprocess.Popen(
        ["tar", "-xzf", "-", "-C", str(target_all)],
        stdin=ssh_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Threaded drain of ssh stderr to prevent pipe buffer deadlock
    stderr_chunks: list[bytes] = []

    def _drain_stderr() -> None:
        if ssh_proc.stderr:
            stderr_chunks.append(ssh_proc.stderr.read())

    drain_thread = threading.Thread(target=_drain_stderr, daemon=True)
    drain_thread.start()

    if ssh_proc.stdout is not None:
        ssh_proc.stdout.close()
    _, tar_err = tar_proc.communicate()
    drain_thread.join(timeout=30)
    ssh_proc.wait()

    if tar_proc.returncode != 0:
        ssh_err_msg = b"".join(stderr_chunks).decode("utf-8", errors="replace").strip()
        print(
            f"  ⚠️ 同步 date={date_str} 出现警告/错误: "
            f"tar={tar_err.decode()} ssh={ssh_err_msg}"
        )
        return False
    else:
        # Mirror to daily parquet
        for item in target_all.iterdir():
            dest = target_daily / item.name
            if item.is_dir():
                if dest.exists():
                    shutil.rmtree(dest)
                shutil.copytree(item, dest)
            elif item.is_file():
                shutil.copy2(item, dest)
        n_files = len(list(target_all.rglob("*.parquet")))
        print(
            f"  ✅ date={date_str} 同步完成: {n_files} 个 Parquet 文件 "
            f"({time.perf_counter() - t0:.2f}s)"
        )
        return True


def partition_table_by_accounts(tbl: str, gz_path: Path) -> None:
    """Streamingly partition a downloaded table across 4 accounts."""
    if not gz_path.exists() or gz_path.stat().st_size == 0:
        return

    writers: dict[str, tuple[Any, Any]] = {}
    handles: list[Any] = []

    try:
        with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f_in:
            reader = csv.DictReader(f_in)
            fieldnames = reader.fieldnames
            if not fieldnames:
                return

            for acc_id, remote_label in ACCOUNT_MAP.items():
                acc_dir = LIVE_LATEST_DIR / acc_id
                acc_dir.mkdir(parents=True, exist_ok=True)

                out_name = (
                    "account_balance_usdt"
                    if tbl == "account_balance_snapshots"
                    else tbl
                )

                f_csv = open(
                    acc_dir / f"{out_name}.csv", "w", newline="", encoding="utf-8"
                )
                f_gz = gzip.open(
                    acc_dir / f"{out_name}.csv.gz", "wt", newline="", encoding="utf-8"
                )
                handles.extend([f_csv, f_gz])

                w_csv = csv.DictWriter(f_csv, fieldnames=fieldnames)
                w_gz = csv.DictWriter(f_gz, fieldnames=fieldnames)
                w_csv.writeheader()
                w_gz.writeheader()
                writers[remote_label] = (w_csv, w_gz)

            row_counts: dict[str, int] = {acc_id: 0 for acc_id in ACCOUNT_MAP}
            has_acc_col = "account_label" in fieldnames

            for row in reader:
                if tbl == "account_balance_snapshots" and row.get("asset") != "USDT":
                    continue
                acc_label = row.get("account_label") if has_acc_col else None
                if acc_label and acc_label in writers:
                    w_csv, w_gz = writers[acc_label]
                    w_csv.writerow(row)
                    w_gz.writerow(row)
                    # map remote_label back to acc_id
                    for aid, r_lbl in ACCOUNT_MAP.items():
                        if r_lbl == acc_label:
                            row_counts[aid] += 1
                elif not has_acc_col:
                    # Table without account_label (e.g. universe_snapshots)
                    for w_csv, w_gz in writers.values():
                        w_csv.writerow(row)
                        w_gz.writerow(row)

            # Print brief stats
            if tbl == "account_balance_snapshots":
                summary_parts = [f"{aid}: {row_counts[aid]:,}条" for aid in ACCOUNT_MAP]
                print(f"     分账完成: {', '.join(summary_parts)}")
    finally:
        for h in handles:
            try:
                h.close()
            except Exception:
                pass


def sync_postgres_tables(tables: list[str] | None = None) -> None:
    """Dump latest live Postgres tables via gzip stream from production container."""
    print("\n📊 导出实盘 Postgres 核心数据表 (流式 Gzip 压缩)...")
    if tables is None:
        tables = [
            "account_balance_snapshots",
            "account_position_snapshots",
            "account_fill_events",
            "order_intents",
            "exchange_orders",
            "live_strategy_signals",
            "account_config_snapshots",
            "universe_snapshots",
        ]

    LIVE_LATEST_DIR.mkdir(parents=True, exist_ok=True)

    for tbl in tables:
        t0 = time.perf_counter()
        gz_path = LIVE_LATEST_DIR / f"{tbl}.csv.gz"

        remote_cmd = (
            f"docker exec {REMOTE_PG_CONTAINER} psql -U cml -d cml "
            f'-c "COPY {tbl} TO STDOUT WITH CSV HEADER;" | gzip -c'
        )

        full_ssh = [
            "sshpass",
            "-p",
            SERVER_PASSWORD,
            "ssh",
            "-p",
            str(SERVER_PORT),
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            f"{SERVER_USER}@{SERVER_HOST}",
            remote_cmd,
        ]

        with open(gz_path, "wb") as f_out:
            proc = subprocess.run(full_ssh, stdout=f_out, stderr=subprocess.PIPE)

        if proc.returncode != 0 or gz_path.stat().st_size == 0:
            err_msg = proc.stderr.decode("utf-8", errors="replace").strip()
            print(f"  ⚠️ 表 {tbl} 导出警告/失败: {err_msg}")
            continue

        gz_size_kb = gz_path.stat().st_size / 1024
        print(
            f"  ✅ 表 {tbl:28s}: {gz_size_kb:7.1f} KB ({time.perf_counter() - t0:.2f}s)"
        )

        # Streamingly partition for 4 accounts
        partition_table_by_accounts(tbl, gz_path)


def trigger_cache_rebuild() -> None:
    """Trigger price cache rebuild using build_price_cache.py."""
    print("\n⚡ 重新构建 15s 价格序列缓存 (cache_15s_price_series.pkl)...")
    cache_script = SCRIPT_DIR / "build_price_cache.py"
    if not cache_script.exists():
        print("  ⚠️ 未找到 build_price_cache.py，跳过缓存构建")
        return
    cmd = [sys.executable, str(cache_script), "--force"]
    env = {**os.environ, "PYTHONPATH": str(ROOT_DIR)}
    res = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if res.returncode == 0:
        print("  ✅ 价格序列缓存构建完成!")
    else:
        print(f"  ⚠️ 缓存构建失败: {res.stderr}")


def trigger_opportunity_and_replay_update() -> None:
    """Trigger opportunity pool, top-N lookups, and replay events update."""
    import pickle

    from local_optimization.opportunity import load_top10_lookup

    env = {**os.environ, "PYTHONPATH": str(ROOT_DIR)}

    print("\n🎯 重新构建原始突破机会池 (build_raw_opportunity_pool.py)...")
    opp_script = SCRIPT_DIR / "build_raw_opportunity_pool.py"
    if opp_script.exists():
        res = subprocess.run(
            [sys.executable, str(opp_script)],
            capture_output=True,
            text=True,
            env=env,
        )
        if res.returncode == 0:
            print("  ✅ 原始机会池更新完成!")
        else:
            print(f"  ⚠️ 机会池构建失败: {res.stderr}")

    print("\n⚡ 重新构建 Top-N 门禁标的池索引缓存...")
    for rank in [10, 20, 30]:
        c_p = SCRIPT_DIR / f"data/cache_top{rank}_lookup.pkl"
        s = load_top10_lookup(
            ALL_PARQUET_DIR, cache_path=None, max_rank=rank, force_rebuild=True
        )
        with c_p.open("wb") as f:
            pickle.dump(s, f)
        print(f"  ✅ Top {rank} 索引就绪: {len(s):,} 条记录")

    print("\n🔄 重新生成 4 账户离线回放事件流 (update_account_replay_events.py)...")
    rep_script = SCRIPT_DIR / "update_account_replay_events.py"
    if rep_script.exists():
        res = subprocess.run(
            [sys.executable, str(rep_script)],
            capture_output=True,
            text=True,
            env=env,
        )
        if res.returncode == 0:
            print("  ✅ 4 账户离线回放流更新完毕!")
        else:
            print(f"  ⚠️ 回放流更新失败: {res.stderr}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Sync parquet price feeds and Postgres trade events from production server."
        )
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        default=True,
        help=(
            "Automatically detect and sync missing or incomplete remote dates "
            "(default: True)"
        ),
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        help="Explicit list of dates to sync (e.g. --dates 2026-09-20 2026-09-21)",
    )
    parser.add_argument(
        "--skip-parquet",
        action="store_true",
        help="Skip downloading parquet high frequency price slices",
    )
    parser.add_argument(
        "--skip-postgres",
        action="store_true",
        help="Skip dumping Postgres database live tables",
    )
    parser.add_argument(
        "--rebuild-cache",
        action="store_true",
        help="Rebuild 15s price cache pickle after synchronization",
    )
    parser.add_argument(
        "--skip-replays",
        action="store_true",
        help="Skip rebuilding opportunity pool and account replay event streams",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only run preflight connectivity check and exit",
    )
    args = parser.parse_args()

    print("======================================================================")
    print(f"🚀 连接生产服务器 [{SERVER_HOST}] 抓取最新数据与实盘事件流")
    print("======================================================================")

    if not check_server_health():
        sys.exit(1)

    if args.check:
        print("✅ 连通性检查完成。")
        return

    # 1. Parquet high frequency slices
    dates_to_sync: list[str] = []
    if not args.skip_parquet:
        if args.dates:
            dates_to_sync = args.dates
        else:
            # Auto-discovery
            print("\n🔍 正在检索远程与本地 Parquet 分区完整性...")
            remote_dates = discover_remote_dates()
            local_dates = get_local_dates_summary()

            for dt_str, r_hours in sorted(remote_dates.items()):
                l_hours = local_dates.get(dt_str, 0)
                if l_hours < r_hours:
                    print(
                        f"  📌 发现增量切片: date={dt_str} "
                        f"(本地 {l_hours}h / 远程 {r_hours}h)"
                    )
                    dates_to_sync.append(dt_str)
                else:
                    print(
                        f"  👌 切片已完整: date={dt_str} ({l_hours}/{r_hours}h 已就绪)"
                    )

        if dates_to_sync:
            for d in dates_to_sync:
                sync_parquet_date(d)
        else:
            print("  ✨ 所有 Parquet 行情切片已为最新状态，无需重新同步。")

    # 2. Postgres live account tables
    if not args.skip_postgres:
        sync_postgres_tables()

    # 3. Optional price cache rebuild
    if args.rebuild_cache:
        trigger_cache_rebuild()

    # 4. Rebuild opportunity pool and replay event streams
    if not args.skip_replays and (dates_to_sync or args.rebuild_cache):
        trigger_opportunity_and_replay_update()

    print("\n======================================================================")
    print("✅ 远程生产数据同步全部完成!")
    print("======================================================================")


if __name__ == "__main__":
    main()
