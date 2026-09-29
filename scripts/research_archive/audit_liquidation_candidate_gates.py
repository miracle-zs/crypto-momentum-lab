from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

FAMILIES = ("C0", "C1", "C2")
COMPONENTS = (
    "sample_size",
    "positive_net_and_pf",
    "tail_robustness",
    "concentration",
    "stress_2bp",
    "all_statistical_gates",
)


def number(row: dict[str, str], name: str) -> float:
    value = row[name]
    if not value:
        return float("nan")
    return float(value)


def finite_rank(value: float) -> float:
    if math.isnan(value):
        return 0.0
    if math.isinf(value):
        return 1_000_000.0
    return value


def failures(row: dict[str, str]) -> tuple[str, ...]:
    value = row["statistical_gate_failures"]
    return tuple(item for item in value.split("|") if item)


def component_passes(row: dict[str, str]) -> dict[str, bool]:
    split_values = {
        split: {
            "closed": number(row, f"{split}_closed_trades"),
            "net": number(row, f"{split}_net_pnl"),
            "pf": number(row, f"{split}_profit_factor"),
            "tail": number(row, f"{split}_net_without_top_5_trades"),
            "symbol": number(row, f"{split}_top_positive_symbol_share"),
            "day": number(row, f"{split}_top_positive_day_share"),
            "stress": number(row, f"{split}_stress_2bp_pnl"),
        }
        for split in ("train", "validation")
    }
    return {
        "sample_size": all(
            values["closed"] >= 50 for values in split_values.values()
        ),
        "positive_net_and_pf": all(
            values["net"] > 0 and values["pf"] > 1
            for values in split_values.values()
        ),
        "tail_robustness": all(
            values["tail"] > 0 for values in split_values.values()
        ),
        "concentration": all(
            values["symbol"] <= 0.35 and values["day"] <= 0.50
            for values in split_values.values()
        ),
        "stress_2bp": all(
            values["stress"] > 0 for values in split_values.values()
        ),
        "all_statistical_gates": not failures(row),
    }


def row_summary(row: dict[str, str]) -> dict[str, object]:
    train_pf = number(row, "train_profit_factor")
    validation_pf = number(row, "validation_profit_factor")
    train_net = number(row, "train_net_pnl")
    validation_net = number(row, "validation_net_pnl")
    train_tail = number(row, "train_net_without_top_5_trades")
    validation_tail = number(row, "validation_net_without_top_5_trades")
    train_stress = number(row, "train_stress_2bp_pnl")
    validation_stress = number(row, "validation_stress_2bp_pnl")
    return {
        "candidate_id": row["candidate_id"],
        "family": row["family"],
        "gate_failure_count": len(failures(row)),
        "gate_failures": list(failures(row)),
        "train_closed_trades": int(number(row, "train_closed_trades")),
        "validation_closed_trades": int(
            number(row, "validation_closed_trades")
        ),
        "train_net_pnl": train_net,
        "validation_net_pnl": validation_net,
        "train_profit_factor": train_pf,
        "validation_profit_factor": validation_pf,
        "train_win_rate": number(row, "train_win_rate"),
        "validation_win_rate": number(row, "validation_win_rate"),
        "train_net_without_top_5": train_tail,
        "validation_net_without_top_5": validation_tail,
        "train_stress_2bp_pnl": train_stress,
        "validation_stress_2bp_pnl": validation_stress,
        "train_symbol_concentration": number(
            row,
            "train_top_positive_symbol_share",
        ),
        "validation_symbol_concentration": number(
            row,
            "validation_top_positive_symbol_share",
        ),
        "train_day_concentration": number(
            row,
            "train_top_positive_day_share",
        ),
        "validation_day_concentration": number(
            row,
            "validation_top_positive_day_share",
        ),
        "minimum_profit_factor": min(
            finite_rank(train_pf),
            finite_rank(validation_pf),
        ),
        "minimum_net_pnl": min(train_net, validation_net),
        "minimum_tail_pnl": min(train_tail, validation_tail),
        "minimum_stress_2bp_pnl": min(train_stress, validation_stress),
        "components": component_passes(row),
    }


def near_miss_rank(row: dict[str, object]) -> tuple[object, ...]:
    return (
        int(row["gate_failure_count"]),
        -float(row["minimum_profit_factor"]),
        -float(row["minimum_tail_pnl"]),
        -float(row["minimum_stress_2bp_pnl"]),
        str(row["candidate_id"]),
    )


def train_rank(row: dict[str, object]) -> tuple[object, ...]:
    train_failures = [
        failure
        for failure in row["gate_failures"]
        if str(failure).startswith("train:")
    ]
    return (
        len(train_failures),
        -finite_rank(float(row["train_profit_factor"])),
        -float(row["train_net_without_top_5"]),
        -float(row["train_stress_2bp_pnl"]),
        str(row["candidate_id"]),
    )


def audit(metrics_path: Path, selection_path: Path) -> dict[str, object]:
    with metrics_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    summaries = [row_summary(row) for row in rows]
    by_family: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in summaries:
        by_family[str(row["family"])].append(row)

    reason_counts = Counter(
        failure for row in summaries for failure in row["gate_failures"]
    )
    reason_counts_by_family = {
        family: dict(
            Counter(
                failure
                for row in by_family[family]
                for failure in row["gate_failures"]
            ).most_common()
        )
        for family in FAMILIES
    }
    component_counts = {
        "all": {
            component: sum(bool(row["components"][component]) for row in summaries)
            for component in COMPONENTS
        },
        **{
            family: {
                component: sum(
                    bool(row["components"][component])
                    for row in by_family[family]
                )
                for component in COMPONENTS
            }
            for family in FAMILIES
        },
    }
    diagnostic_combinations = {}
    for scope, scoped_rows in (
        ("all", summaries),
        *((family, by_family[family]) for family in FAMILIES),
    ):
        diagnostic_combinations[scope] = {
            "core_sample_net_pf_stress": sum(
                bool(row["components"]["sample_size"])
                and bool(row["components"]["positive_net_and_pf"])
                and bool(row["components"]["stress_2bp"])
                for row in scoped_rows
            ),
            "all_except_tail_robustness": sum(
                bool(row["components"]["sample_size"])
                and bool(row["components"]["positive_net_and_pf"])
                and bool(row["components"]["concentration"])
                and bool(row["components"]["stress_2bp"])
                for row in scoped_rows
            ),
            "all_except_concentration": sum(
                bool(row["components"]["sample_size"])
                and bool(row["components"]["positive_net_and_pf"])
                and bool(row["components"]["tail_robustness"])
                and bool(row["components"]["stress_2bp"])
                for row in scoped_rows
            ),
            "all_except_tail_and_concentration": sum(
                bool(row["components"]["sample_size"])
                and bool(row["components"]["positive_net_and_pf"])
                and bool(row["components"]["stress_2bp"])
                for row in scoped_rows
            ),
            "train_gate_pass": sum(
                not any(
                    str(failure).startswith("train:")
                    for failure in row["gate_failures"]
                )
                for row in scoped_rows
            ),
            "validation_gate_pass": sum(
                not any(
                    str(failure).startswith("validation:")
                    for failure in row["gate_failures"]
                )
                for row in scoped_rows
            ),
        }
    quadrants = {
        family: dict(
            Counter(
                "|".join(
                    (
                        "train_positive"
                        if float(row["train_net_pnl"]) > 0
                        else "train_nonpositive",
                        "validation_positive"
                        if float(row["validation_net_pnl"]) > 0
                        else "validation_nonpositive",
                    )
                )
                for row in by_family[family]
            )
        )
        for family in FAMILIES
    }
    closest = sorted(summaries, key=near_miss_rank)
    closest_by_family = {
        family: sorted(by_family[family], key=near_miss_rank)[:5]
        for family in FAMILIES
    }
    train_selected = {
        family: sorted(by_family[family], key=train_rank)[0]
        for family in FAMILIES
    }
    selection = json.loads(selection_path.read_text(encoding="utf-8"))
    baseline_id = str(selection["baseline_candidate_id"])
    baseline = next(
        row for row in summaries if row["candidate_id"] == baseline_id
    )
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "candidate_count": len(summaries),
        "candidate_count_by_family": {
            family: len(by_family[family]) for family in FAMILIES
        },
        "selection": selection,
        "component_pass_counts": component_counts,
        "diagnostic_combination_counts": diagnostic_combinations,
        "failure_reason_counts": dict(reason_counts.most_common()),
        "failure_reason_counts_by_family": reason_counts_by_family,
        "net_pnl_sign_quadrants_by_family": quadrants,
        "baseline": baseline,
        "closest_near_misses": closest[:15],
        "closest_near_misses_by_family": closest_by_family,
        "train_only_selected_by_family": train_selected,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    payload = audit(args.metrics, args.selection)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    compact = {
        "candidate_count_by_family": payload["candidate_count_by_family"],
        "component_pass_counts": payload["component_pass_counts"],
        "failure_reason_counts": payload["failure_reason_counts"],
        "train_only_selected_by_family": payload["train_only_selected_by_family"],
    }
    print(json.dumps(compact, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
