#!/usr/bin/env python3
"""
Emergency Flatten Script for Crypto Momentum Lab.

Discovers and market-closes all active positions and cancels open orders across
the 4 Binance trading accounts:
  1. primary
  2. account-2
  3. account-3
  4. account-4

Usage:
  python3 close_all_positions.py --dry-run
  python3 close_all_positions.py --account account-3 --dry-run
  python3 close_all_positions.py --yes
  python3 close_all_positions.py
"""

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal

DEFAULT_ENV_PATHS = [
    "/opt/crypto-momentum-lab/.env.server",
    "/root/crypto-momentum-lab/.env.server",
    ".env.server",
    ".env",
]


def load_env(custom_path=None):
    paths = [custom_path] if custom_path else DEFAULT_ENV_PATHS
    env = {}
    target_path = None
    for p in paths:
        if p and os.path.exists(p):
            target_path = p
            break

    if not target_path:
        print("Warning: No .env or .env.server file found in standard locations.")
        return env, None

    with open(target_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip("'\"")

    return env, target_path


def get_signature(secret: str, query_string: str) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def binance_request(api_key: str, api_secret: str, method: str, path: str, params: dict = None):
    base_url = "https://fapi.binance.com"
    if params is None:
        params = {}
    params = {k: v for k, v in params.items() if v is not None}
    params["timestamp"] = int(time.time() * 1000)
    query_string = urllib.parse.urlencode(params)
    signature = get_signature(api_secret, query_string)
    full_query = f"{query_string}&signature={signature}"
    headers = {
        "X-MBX-APIKEY": api_key,
        "User-Agent": "CML-Emergency-Flatten/1.0",
    }

    if method in ("GET", "DELETE"):
        url = f"{base_url}{path}?{full_query}"
        req = urllib.request.Request(url, headers=headers, method=method)
    else:
        url = f"{base_url}{path}"
        data = full_query.encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8")
        try:
            parsed = json.loads(err_body)
            return {"error": e.code, "code": parsed.get("code"), "msg": parsed.get("msg"), "raw": err_body}
        except Exception:
            return {"error": e.code, "raw": err_body}
    except Exception as e:
        return {"error": str(e)}


def format_qty(amt: float) -> str:
    qty = abs(Decimal(str(amt)))
    if qty == int(qty):
        return str(int(qty))
    return f"{qty:f}".rstrip("0").rstrip(".")


def get_accounts(env: dict, selected_account: str = "all"):
    account_defs = [
        ("primary", env.get("BINANCE_TRADE_API_KEY"), env.get("BINANCE_TRADE_API_SECRET")),
        ("account-2", env.get("BINANCE_TRADE_API_KEY_ACCOUNT_2"), env.get("BINANCE_TRADE_API_SECRET_ACCOUNT_2")),
        ("account-3", env.get("BINANCE_TRADE_API_KEY_ACCOUNT_3"), env.get("BINANCE_TRADE_API_SECRET_ACCOUNT_3")),
        ("account-4", env.get("BINANCE_TRADE_API_KEY_ACCOUNT_4"), env.get("BINANCE_TRADE_API_SECRET_ACCOUNT_4")),
    ]
    if selected_account != "all":
        account_defs = [a for a in account_defs if a[0] == selected_account]
    return account_defs


def scan_account(label: str, key: str, secret: str):
    pos_res = binance_request(key, secret, "GET", "/fapi/v2/positionRisk")
    orders_res = binance_request(key, secret, "GET", "/fapi/v1/openOrders")

    active_positions = []
    if isinstance(pos_res, list):
        active_positions = [p for p in pos_res if float(p.get("positionAmt", 0)) != 0]
    else:
        print(f"[{label}] Warning: failed to fetch positionRisk: {pos_res}")

    open_orders = []
    if isinstance(orders_res, list):
        open_orders = orders_res
    else:
        print(f"[{label}] Warning: failed to fetch openOrders: {orders_res}")

    return active_positions, open_orders


def flatten_account(label: str, key: str, secret: str, active_positions: list, open_orders: list, dry_run: bool = False):
    print(f"\n==================================================")
    print(f"[{label}] Executing Flatten Actions")
    print(f"==================================================")

    # 1. Cancel open orders
    if open_orders:
        affected_symbols = sorted(set(o.get("symbol") for o in open_orders if o.get("symbol")))
        print(f"[{label}] Found {len(open_orders)} open order(s) across {len(affected_symbols)} symbol(s).")
        for sym in affected_symbols:
            if dry_run:
                print(f"[{label}] (DRY RUN) Would cancel all open orders for {sym}")
            else:
                cancel_res = binance_request(key, secret, "DELETE", "/fapi/v1/allOpenOrders", {"symbol": sym})
                print(f"[{label}] Cancel all open orders for {sym}: {cancel_res}")
    else:
        print(f"[{label}] No open orders to cancel.")

    # 2. Market close positions
    if not active_positions:
        print(f"[{label}] No active positions to close.")
        return 0

    closed_count = 0
    for p in active_positions:
        sym = p.get("symbol")
        pos_side = p.get("positionSide")
        amt = float(p.get("positionAmt", 0))
        if amt == 0:
            continue

        side = "SELL" if amt > 0 else "BUY"
        qty_str = format_qty(amt)

        order_params = {
            "symbol": sym,
            "side": side,
            "positionSide": pos_side,
            "type": "MARKET",
            "quantity": qty_str,
        }
        if pos_side == "BOTH":
            order_params["reduceOnly"] = "true"

        if dry_run:
            print(f"[{label}] (DRY RUN) Would place close order: {order_params}")
            closed_count += 1
        else:
            # Cancel orders for this symbol first just in case
            binance_request(key, secret, "DELETE", "/fapi/v1/allOpenOrders", {"symbol": sym})
            print(f"[{label}] Submitting MARKET close order: {order_params}")
            order_res = binance_request(key, secret, "POST", "/fapi/v1/order", order_params)
            status = order_res.get("status")
            order_id = order_res.get("orderId")
            avg_price = order_res.get("avgPrice")
            executed_qty = order_res.get("executedQty")
            if status in ("FILLED", "NEW"):
                print(f"[{label}] Closed {sym}: orderId={order_id}, status={status}, avgPrice={avg_price}, executedQty={executed_qty}")
                closed_count += 1
            else:
                print(f"[{label}] ERROR closing {sym}: {order_res}")

    return closed_count


def verify_account(label: str, key: str, secret: str):
    pos_res = binance_request(key, secret, "GET", "/fapi/v2/positionRisk")
    orders_res = binance_request(key, secret, "GET", "/fapi/v1/openOrders")

    active = []
    if isinstance(pos_res, list):
        active = [p for p in pos_res if float(p.get("positionAmt", 0)) != 0]

    orders = []
    if isinstance(orders_res, list):
        orders = orders_res

    status_str = "FLAT & CLEAN (0 positions, 0 orders)" if not active and not orders else f"STILL HAS {len(active)} positions, {len(orders)} orders"
    print(f"[{label}] Status: {status_str}")
    if active:
        for p in active:
            print(f"    Remaining pos: {p.get('symbol')} {p.get('positionSide')} amt={p.get('positionAmt')}")
    if orders:
        for o in orders:
            print(f"    Remaining order: {o.get('symbol')} {o.get('side')} {o.get('type')} qty={o.get('origQty')}")

    return len(active), len(orders)


def main():
    parser = argparse.ArgumentParser(description="Emergency close all positions and orders across accounts.")
    parser.add_argument("--env-file", help="Path to .env / .env.server file")
    parser.add_argument("--account", default="all", choices=["all", "primary", "account-2", "account-3", "account-4"], help="Target account (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without sending real orders")
    parser.add_argument("-y", "--yes", action="store_true", help="Execute without asking for interactive confirmation")
    args = parser.parse_args()

    env, env_path = load_env(args.env_file)
    print(f">>> CML Emergency Flatten Script <<<")
    print(f"Environment source: {env_path or 'NOT FOUND'}")
    print(f"Target account(s): {args.account}")
    print(f"Mode: {'DRY RUN (Simulated)' if args.dry_run else 'LIVE EXECUTION'}")

    accounts = get_accounts(env, args.account)
    if not accounts:
        print(f"Error: Selected account '{args.account}' not found.")
        sys.exit(1)

    print("\n--- Scanning Current Portfolio State ---")
    plan = []
    total_positions = 0
    total_orders = 0

    for label, key, secret in accounts:
        if not key or not secret:
            print(f"[{label}] Missing API Key / Secret, skipping.")
            continue

        active_positions, open_orders = scan_account(label, key, secret)
        plan.append((label, key, secret, active_positions, open_orders))
        total_positions += len(active_positions)
        total_orders += len(open_orders)

        print(f"[{label}] Positions: {len(active_positions)}, Open Orders: {len(open_orders)}")
        for p in active_positions:
            print(f"  * POS: {p.get('symbol'):12s} | side: {p.get('positionSide'):5s} | amt: {p.get('positionAmt'):>10s} | entry: {p.get('entryPrice'):>10s} | uPnL: {p.get('unRealizedProfit'):>8s}")
        for o in open_orders:
            print(f"  * ORD: {o.get('symbol'):12s} | side: {o.get('side'):5s} | type: {o.get('type'):8s} | qty: {o.get('origQty'):>10s}")

    print(f"\nSummary: Total active positions to close: {total_positions}, Total open orders to cancel: {total_orders}")

    if total_positions == 0 and total_orders == 0:
        print("All accounts are already flat with 0 open orders. Nothing to do.")
        return

    if not args.dry_run and not args.yes:
        print("\n" + "!" * 60)
        print("WARNING: This will MARKET CLOSE all active positions and CANCEL all orders above!")
        print("!" * 60)
        try:
            confirm = input("Are you sure you want to proceed? Type 'yes' to confirm: ").strip().lower()
        except EOFError:
            confirm = "no"
        if confirm != "yes":
            print("Aborted by user.")
            sys.exit(0)

    for label, key, secret, active_positions, open_orders in plan:
        flatten_account(label, key, secret, active_positions, open_orders, dry_run=args.dry_run)

    if not args.dry_run:
        print("\n--- Verifying Post-Execution State ---")
        time.sleep(1.5)
        remaining_pos = 0
        remaining_orders = 0
        for label, key, secret, _, _ in plan:
            p_cnt, o_cnt = verify_account(label, key, secret)
            remaining_pos += p_cnt
            remaining_orders += o_cnt

        print(f"\nFinal Result: Remaining positions: {remaining_pos}, Remaining orders: {remaining_orders}")
        if remaining_pos == 0 and remaining_orders == 0:
            print(">>> SUCCESS: All target accounts are 100% flat! <<<")
        else:
            print(">>> WARNING: Some positions or orders could not be cleared. Please check logs above. <<<")


if __name__ == "__main__":
    main()
