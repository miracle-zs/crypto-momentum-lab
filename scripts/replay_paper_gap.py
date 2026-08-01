#!/usr/bin/env python3
"""Replay all paper-account pairs against a selected recovery database."""

from __future__ import annotations

import argparse
import os
import subprocess

from sqlalchemy.engine import make_url

PAIR_ARGUMENTS = (
    (
        "compression_breakout",
        "paper-account-01-compression-v1",
        "paper-account-04-compression-candle15m-v1",
        "0.03",
        "0.015",
        "480",
    ),
    (
        "orderflow_impulse",
        "paper-account-02-orderflow-v1",
        "paper-account-05-orderflow-candle15m-v1",
        "0.02",
        "0.01",
        "80",
    ),
    (
        "liquidation_cascade",
        "paper-account-03-liquidation-v1",
        "paper-account-06-liquidation-candle15m-v1",
        "0.025",
        "0.0125",
        "60",
    ),
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-name", required=True)
    parser.add_argument("--max-states", default="100000000")
    args = parser.parse_args()

    raw_database_url = os.environ.get("CML_DATABASE_URL")
    if not raw_database_url:
        raise SystemExit("CML_DATABASE_URL is required")
    environment = dict(os.environ)
    environment["CML_DATABASE_URL"] = make_url(raw_database_url).set(
        database=args.database_name
    ).render_as_string(hide_password=False)

    for strategy, fixed_run, candle_run, take_profit, stop_loss, max_hold in (
        PAIR_ARGUMENTS
    ):
        print(f"replaying {strategy}", flush=True)
        subprocess.run(
            [
                "cml-strategy-runner",
                "paper-live-pair",
                "--strategy",
                strategy,
                "--environment",
                "research",
                "--fixed-run-id",
                fixed_run,
                "--candle-run-id",
                candle_run,
                "--signal-interval-seconds",
                "300",
                "--compression-window-buckets",
                "20",
                "--max-range-width-pct",
                "0.025",
                "--min-breakout-pct",
                "0.003",
                "--acceptance-buckets",
                "1",
                "--cooldown-buckets",
                "12",
                "--candidate-notional",
                "25",
                "--paper-initial-balance",
                "1000",
                "--fixed-take-profit-pct",
                take_profit,
                "--fixed-stop-loss-pct",
                stop_loss,
                "--fixed-max-holding-buckets",
                max_hold,
                "--candle-max-holding-buckets",
                "5760",
                "--max-states",
                args.max_states,
                "--idle-timeout-seconds",
                "0",
                "--poll-interval-seconds",
                "0",
                "--batch-size",
                "2000",
                "--checkpoint-every-states",
                "5000",
                "--checkpoint-every-seconds",
                "3600",
                "--max-market-state-age-seconds",
                "120",
                "--continue-while-halted",
                "--replay-stale-states",
                "--require-market-quote",
            ],
            check=True,
            env=environment,
        )


if __name__ == "__main__":
    main()
