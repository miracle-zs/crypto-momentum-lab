#!/usr/bin/env python3
"""Emergency script to flatten all open positions across all Binance Futures accounts."""

import os
import sys
import time
import hmac
import hashlib
import json
import urllib.parse
import urllib.request
import urllib.error

BASE_URL = "https://fapi.binance.com"

def parse_env_file(env_path):
    config = {}
    if not os.path.exists(env_path):
        return config
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                config[k.strip()] = v.strip().strip("'\"")
    return config

def binance_request(method, path, api_key, api_secret, params=None):
    if params is None:
        params = {}
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 60000

    query_str = urllib.parse.urlencode(params)
    sig = hmac.new(api_secret.encode("utf-8"), query_str.encode("utf-8"), hashlib.sha256).hexdigest()
    full_query = f"{query_str}&signature={sig}"

    url = f"{BASE_URL}{path}?{full_query}" if method in ["GET", "DELETE"] else f"{BASE_URL}{path}"
    data = None
    if method == "POST":
        data = full_query.encode("utf-8")

    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("X-MBX-APIKEY", api_key)
    if method == "POST":
        req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8")
        raise RuntimeError(f"HTTPError {e.code}: {err_msg}")
    except Exception as e:
        raise RuntimeError(f"Request failed: {e}")

def get_positions(api_key, api_secret):
    res = binance_request("GET", "/fapi/v2/positionRisk", api_key, api_secret)
    active = []
    for p in res:
        amt = float(p.get("positionAmt", 0))
        if abs(amt) > 1e-8:
            active.append(p)
    return active

def get_balances(api_key, api_secret):
    res = binance_request("GET", "/fapi/v2/balance", api_key, api_secret)
    usdt = [b for b in res if b.get("asset") == "USDT"]
    return usdt[0] if usdt else {}

def cancel_open_orders(symbol, api_key, api_secret):
    try:
        return binance_request("DELETE", "/fapi/v1/allOpenOrders", api_key, api_secret, {"symbol": symbol})
    except Exception as e:
        print(f"  Warning: failed to cancel open orders for {symbol}: {e}")
        return None

def flatten_position(p, api_key, api_secret):
    symbol = p["symbol"]
    amt = float(p["positionAmt"])
    entry_price = float(p.get("entryPrice", 0))
    mark_price = float(p.get("markPrice", 0))
    unrealized_pnl = float(p.get("unRealizedProfit", 0))
    position_side = p.get("positionSide", "BOTH")

    print(f"  Target: {symbol} | Amt: {amt} | Entry: {entry_price} | Mark: {mark_price} | uPnL: {unrealized_pnl}")

    # 1. Cancel open orders on symbol
    print(f"  -> Cancelling existing orders on {symbol}...")
    cancel_open_orders(symbol, api_key, api_secret)

    # 2. Market close order
    side = "SELL" if amt > 0 else "BUY"
    qty_str = str(abs(amt))
    # If quantity has too many trailing decimal zeros or formatting, keep standard representation
    if qty_str.endswith(".0"):
        qty_str = qty_str[:-2]

    order_params = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty_str,
    }
    if position_side in ["LONG", "SHORT"]:
        order_params["positionSide"] = position_side
    else:
        order_params["reduceOnly"] = "true"

    print(f"  -> Submitting MARKET {side} {qty_str} (positionSide={position_side})...")
    res = binance_request("POST", "/fapi/v1/order", api_key, api_secret, order_params)
    order_id = res.get("orderId")
    status = res.get("status")
    avg_price = res.get("avgPrice", "0")
    print(f"  -> Order placed: ID={order_id}, Status={status}, ExecPrice={avg_price}")
    return res

def process_account(account_name, api_key, api_secret):
    print("=" * 60)
    print(f"ACCOUNT: {account_name}")
    print("=" * 60)

    try:
        bal = get_balances(api_key, api_secret)
        print(f"USDT Balance: {bal.get('balance')} | Available: {bal.get('availableBalance')} | Cross UnPnl: {bal.get('crossUnPnl')}")
    except Exception as e:
        print(f"Error fetching balance: {e}")

    try:
        active_positions = get_positions(api_key, api_secret)
        if not active_positions:
            print("No open positions found.")
            return

        print(f"Found {len(active_positions)} open position(s):")
        for p in active_positions:
            flatten_position(p, api_key, api_secret)

        time.sleep(1)
        remaining = get_positions(api_key, api_secret)
        if not remaining:
            print(f"SUCCESS: All positions in {account_name} have been completely flattened!")
        else:
            print(f"WARNING: Remaining positions in {account_name}: {remaining}")

        bal_after = get_balances(api_key, api_secret)
        print(f"Updated USDT Balance: {bal_after.get('balance')} | Available: {bal_after.get('availableBalance')}")

    except Exception as e:
        print(f"Error processing {account_name}: {e}")

def main():
    env_file = sys.argv[1] if len(sys.argv) > 1 else "/opt/crypto-momentum-lab/.env.server"
    config = parse_env_file(env_file)
    if not config:
        print(f"No environment file found at {env_file}, attempting system environment...")
        config = os.environ

    accounts = [
        ("primary (Account 1)", config.get("BINANCE_TRADE_API_KEY"), config.get("BINANCE_TRADE_API_SECRET")),
        ("Account 2", config.get("BINANCE_TRADE_API_KEY_ACCOUNT_2"), config.get("BINANCE_TRADE_API_SECRET_ACCOUNT_2")),
        ("Account 3", config.get("BINANCE_TRADE_API_KEY_ACCOUNT_3"), config.get("BINANCE_TRADE_API_SECRET_ACCOUNT_3")),
        ("Account 4", config.get("BINANCE_TRADE_API_KEY_ACCOUNT_4"), config.get("BINANCE_TRADE_API_SECRET_ACCOUNT_4")),
    ]

    for name, key, secret in accounts:
        if not key or not secret:
            print(f"Skipping {name}: API key/secret not set.")
            continue
        process_account(name, key, secret)

    print("\nAll accounts processed.")

if __name__ == "__main__":
    main()
