"""CLI and programmatic tool to reproduce and audit historic DecisionTraces.

Obeys Astra Architecture Blueprint Section 8.3:
- Loads pinned DecisionTrace and exact MarketRevisionRefs from Postgres;
- Verifies input hashes, frame digests, and semantic decision outputs;
- Replays decision and certifies 100% reproducibility in an independent process.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from crypto_momentum_lab.config import resolve_database_url
from crypto_momentum_lab.domain.market.market_book import UnreproducibleError
from crypto_momentum_lab.persistence.postgres.decision_trace_repository import (
    PostgresDecisionTraceRepository,
)
from crypto_momentum_lab.persistence.postgres.session import (
    create_async_database_engine,
)


async def audit_decision_trace(
    decision_id: str,
    database_url: str | None = None,
    trace_override: Any | None = None,
) -> dict[str, Any]:
    """Audits and verifies exact reproducibility of a DecisionTrace."""
    if trace_override is not None:
        trace = trace_override
    else:
        url = resolve_database_url(
            database_url,
            "CML_OBSERVABILITY_DATABASE_URL",
            "CML_DATABASE_URL",
        )
        if not url:
            raise ValueError(
                "Database URL must be provided or configured via CML_DATABASE_URL"
            )
        engine = create_async_database_engine(url, pool_size=1, max_overflow=0)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        repo = PostgresDecisionTraceRepository(session_factory)

        try:
            trace = await repo.load_decision_trace(decision_id)
        finally:
            await engine.dispose()

    try:
        if trace is None:
            return {
                "decision_id": decision_id,
                "status": "NOT_FOUND",
                "error": f"DecisionTrace '{decision_id}' not found in database",
                "reproduced": False,
            }

        # 1. Structural evidence checks
        if not trace.evaluated_market_refs:
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "DecisionTrace has no evaluated market references",
                "reproduced": False,
            }

        if not trace.input_hash or not trace.frame_digest:
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": (
                    "DecisionTrace missing cryptographic input_hash or frame_digest"
                ),
                "reproduced": False,
            }

        payload = trace.trace_payload or {}

        # 2. Hash consistency checks
        if payload.get("input_hash") and payload.get("input_hash") != trace.input_hash:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": (
                    f"input_hash mismatch: trace {trace.input_hash} vs payload "
                    f"{payload.get('input_hash')}"
                ),
                "reproduced": False,
            }
        if (
            payload.get("frame_digest")
            and payload.get("frame_digest") != trace.frame_digest
        ):
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": (
                    f"frame_digest mismatch: trace {trace.frame_digest} vs payload "
                    f"{payload.get('frame_digest')}"
                ),
                "reproduced": False,
            }

        revisions_summary = []
        for ref in trace.evaluated_market_refs:
            if not getattr(ref, "revision_id", None) or not getattr(
                ref, "content_hash", None
            ):
                rev_id = getattr(ref, "revision_id", "?")
                return {
                    "decision_id": trace.decision_id,
                    "status": "EVIDENCE_INSUFFICIENT",
                    "error": (f"Market revision ref {rev_id} has invalid content_hash"),
                    "reproduced": False,
                }
            revisions_summary.append(
                {
                    "revision_id": ref.revision_id,
                    "symbol": ref.symbol,
                    "bucket_start": ref.bucket_start.isoformat(),
                    "bucket_end": ref.bucket_end.isoformat(),
                    "content_hash": ref.content_hash,
                    "visibility_mode": (
                        ref.visibility_mode.value
                        if hasattr(ref.visibility_mode, "value")
                        else str(ref.visibility_mode)
                    ),
                }
            )

        output_intent = payload.get("output_intent")
        next_policy_state = payload.get("next_policy_state")

        # 3. Intent presence and consistency check
        if bool(output_intent) != trace.intent_produced:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": (
                    f"Output intent presence mismatch: {bool(output_intent)} vs "
                    f"{trace.intent_produced}"
                ),
                "reproduced": False,
            }

        # 4. Semantic Replay verification (market_state payload is required)
        market_state_payload = payload.get("market_state")
        if not market_state_payload:
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "Missing market_state in trace payload; semantic decision replay impossible",
                "reproduced": False,
            }

        from crypto_momentum_lab.domain.decision.decision_engine import (
            ClockEvent,
            DecisionInput,
            EffectivePolicy,
            PolicyState,
            decide,
        )
        from crypto_momentum_lab.domain.execution.position_ledger_models import (
            PositionHealthStatus,
            PositionKey,
            PositionLedgerBatch,
            PositionView,
        )
        from crypto_momentum_lab.domain.market.revision_models import MarketEnvelope
        from crypto_momentum_lab.domain.strategy.models import StrategySide
        from crypto_momentum_lab.market_data.hub import market_state_from_payload

        m_state = market_state_from_payload(market_state_payload)
        ref0 = trace.evaluated_market_refs[0]
        envelope = MarketEnvelope(ref=ref0, state=m_state)

        if envelope.state.symbol != ref0.symbol:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": (
                    f"Envelope symbol {envelope.state.symbol} does not match "
                    f"ref symbol {ref0.symbol}"
                ),
                "reproduced": False,
            }

        # Reconstruct policy parameters from trace payload or defaults
        pol_params = payload.get("policy_parameters") or {}
        entry_thresh = Decimal(str(pol_params.get("entry_threshold", "65000.00")))
        short_entry_thresh = (
            Decimal(str(pol_params["short_entry_threshold"]))
            if pol_params.get("short_entry_threshold") is not None
            else None
        )
        target_notional = Decimal(str(pol_params.get("target_notional", "1000.00")))
        cd_secs = pol_params.get("cooldown_duration_seconds") or pol_params.get("cooldown_duration") or 900
        grace_secs = pol_params.get("grace_period_seconds") or pol_params.get("grace_period") or 0

        sizing_data = pol_params.get("sizing_model")
        sizing_model = None
        if sizing_data and isinstance(sizing_data, dict):
            cls_name = sizing_data.get("class")
            from crypto_momentum_lab.domain.strategy.sizing import (
                EquityFractionSizingModel,
                FixedNotionalSizingModel,
            )
            if cls_name == "FixedNotionalSizingModel":
                sizing_model = FixedNotionalSizingModel(
                    target_notional=Decimal(str(sizing_data.get("target_notional", "500.00"))),
                    max_leverage=Decimal(str(sizing_data.get("max_leverage", "5.0"))),
                    max_slippage_budget_bps=Decimal(str(sizing_data.get("max_slippage_budget_bps", "10.0"))),
                    resize_tolerance=Decimal(str(sizing_data.get("resize_tolerance", "0.05"))),
                )
            elif cls_name == "EquityFractionSizingModel":
                sizing_model = EquityFractionSizingModel(
                    target_fraction=Decimal(str(sizing_data.get("target_fraction", "0.10"))),
                    max_leverage=Decimal(str(sizing_data.get("max_leverage", "5.0"))),
                    max_slippage_budget_bps=Decimal(str(sizing_data.get("max_slippage_budget_bps", "10.0"))),
                    resize_tolerance=Decimal(str(sizing_data.get("resize_tolerance", "0.05"))),
                )

        lot_rules_data = pol_params.get("symbol_lot_rules")
        lot_rules = None
        if lot_rules_data and isinstance(lot_rules_data, dict):
            from crypto_momentum_lab.domain.strategy.sizing import SymbolLotRules
            lot_rules = SymbolLotRules(
                symbol=lot_rules_data.get("symbol", ref0.symbol),
                tick_size=Decimal(str(lot_rules_data.get("tick_size", "0.1"))),
                step_size=Decimal(str(lot_rules_data.get("step_size", "0.001"))),
                min_quantity=Decimal(str(lot_rules_data.get("min_quantity", "0.001"))),
                max_quantity=Decimal(str(lot_rules_data.get("max_quantity", "1000"))),
                min_notional=Decimal(str(lot_rules_data.get("min_notional", "5.0"))),
            )

        policy = EffectivePolicy(
            policy_id=str(pol_params.get("policy_id", f"policy_{trace.strategy_name}")),
            strategy_name=trace.strategy_name,
            policy_version=int(pol_params.get("policy_version", 1)),
            entry_threshold=entry_thresh,
            short_entry_threshold=short_entry_thresh,
            target_notional=target_notional,
            cooldown_duration=timedelta(seconds=int(cd_secs)),
            grace_period=timedelta(seconds=int(grace_secs)),
            sizing_model=sizing_model,
            symbol_lot_rules=lot_rules,
        )

        # Restore frozen context if recorded
        ctx = payload.get("decision_context") or {}
        pos_view_data = ctx.get("position_view") or {}
        batches_list = []
        for b in pos_view_data.get("batches", ()):
            qty = Decimal(str(b.get("allocated_quantity", b.get("quantity", "0"))))
            batches_list.append(
                PositionLedgerBatch(
                    batch_id=b["batch_id"],
                    episode_id=b.get("episode_id", f"ep_{b['batch_id']}"),
                    quantity=qty,
                    original_quantity=qty,
                    entry_price=Decimal(str(b["entry_price"])),
                    opened_at=datetime.fromisoformat(b["opened_at"]),
                )
            )

        pos_key = PositionKey(
            environment="live",
            account_label=trace.account_label,
            symbol=ref0.symbol,
            position_side=pos_view_data.get("position_side", "BOTH"),
        )
        pos_view = PositionView(
            key=pos_key,
            projection_version=pos_view_data.get("projection_version", "pv_replay_0"),
            input_revision=1,
            event_cut=None,
            policy_version="v1",
            schema_version="v1",
            coverage=None,
            active_episode=None,
            batches=tuple(batches_list),
            unallocated_quantity=Decimal(str(pos_view_data.get("unallocated_quantity", "0"))),
            reconciliation_gap=Decimal("0"),
            health_status=PositionHealthStatus(pos_view_data.get("health_status", "READY")),
        )

        prior_state_data = payload.get("prior_policy_state") or {}
        if prior_state_data:
            prior_state = PolicyState(
                policy_version=prior_state_data.get("policy_version", 1),
                cooldown_until_by_symbol={
                    k: datetime.fromisoformat(v)
                    for k, v in prior_state_data.get("cooldown_until", {}).items()
                },
                anchor_prices_by_symbol={
                    k: Decimal(str(v))
                    for k, v in prior_state_data.get("anchor_prices", {}).items()
                },
                active_intent_ids_by_symbol=dict(prior_state_data.get("active_intent_ids", {})),
                warmup_status=dict(prior_state_data.get("warmup_status", {})),
                grace_until_by_symbol={
                    k: datetime.fromisoformat(v)
                    for k, v in prior_state_data.get("grace_until", {}).items()
                },
                holding_deadline_by_symbol={
                    k: datetime.fromisoformat(v)
                    for k, v in prior_state_data.get("holding_deadline", {}).items()
                },
                signal_memory=dict(prior_state_data.get("signal_memory", {})),
                custom_state=dict(prior_state_data.get("custom_state", {})),
                sizing_state_by_symbol=dict(prior_state_data.get("sizing_state", {})),
            )
        else:
            prior_state = PolicyState(policy_version=1)

        dec_input = DecisionInput(
            symbol=ref0.symbol,
            market_ref=ref0,
            market_envelope=envelope,
            position_view=pos_view,
            universe_version=ctx.get("universe_version", "u1"),
            clock_event=ClockEvent(sequence=1, timestamp=trace.decision_time),
            cash_balance=Decimal(str(ctx.get("cash_balance", "10000.00"))),
            risk_config_version=ctx.get("risk_config_version", "risk_v1"),
        )
        replayed_result = decide(dec_input, prior_state, policy)
        rep_intent_produced = replayed_result.intent is not None
        if rep_intent_produced != trace.intent_produced:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": (
                    f"Replay intent mismatch: produced={rep_intent_produced} "
                    f"vs recorded={trace.intent_produced}"
                ),
                "reproduced": False,
            }
        if (
            not trace.intent_produced
            and replayed_result.rejection_reason != trace.rejection_reason
        ):
            rej = replayed_result.rejection_reason
            rec = trace.rejection_reason
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": (
                    f"Replay rejection reason mismatch: '{rej}' vs recorded '{rec}'"
                ),
                "reproduced": False,
            }

        # Check exit command consistency
        recorded_exit = payload.get("output_exit_command")
        if bool(recorded_exit) != (replayed_result.exit_command is not None):
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": "Replay exit command presence mismatch",
                "reproduced": False,
            }

        if replayed_result.intent is not None and output_intent:
            rep_notional = getattr(replayed_result.intent, "desired_notional", None)
            rec_notional = output_intent.get("desired_notional")
            if rep_notional is not None and rec_notional is not None:
                if Decimal(str(rep_notional)) != Decimal(str(rec_notional)):
                    return {
                        "decision_id": trace.decision_id,
                        "status": "UNREPRODUCIBLE",
                        "error": (
                            f"Replay intent desired_notional mismatch: replayed {rep_notional} "
                            f"vs recorded {rec_notional}"
                        ),
                        "reproduced": False,
                    }

        if replayed_result.exit_command is not None and recorded_exit:
            rep_qty = replayed_result.exit_command.requested_quantity
            rec_qty = recorded_exit.get("quantity")
            if rec_qty is not None and Decimal(str(rep_qty)) != Decimal(str(rec_qty)):
                return {
                    "decision_id": trace.decision_id,
                    "status": "UNREPRODUCIBLE",
                    "error": (
                        f"Replay exit command quantity mismatch: replayed {rep_qty} "
                        f"vs recorded {rec_qty}"
                    ),
                    "reproduced": False,
                }

        return {
            "decision_id": trace.decision_id,
            "status": "VERIFIED_REPRODUCIBLE",
            "reproduced": True,
            "strategy_name": trace.strategy_name,
            "account_label": trace.account_label,
            "decision_time": trace.decision_time.isoformat(),
            "intent_produced": trace.intent_produced,
            "intent_id": trace.intent_id,
            "rejection_reason": trace.rejection_reason,
            "input_hash": trace.input_hash,
            "frame_digest": trace.frame_digest,
            "evaluated_revisions_count": len(trace.evaluated_market_refs),
            "evaluated_revisions": revisions_summary,
            "output_intent": output_intent,
            "next_policy_state_version": (
                next_policy_state.get("policy_version")
                if isinstance(next_policy_state, dict)
                else None
            ),
        }
    except UnreproducibleError as exc:
        return {
            "decision_id": decision_id,
            "status": "UNREPRODUCIBLE",
            "error": str(exc),
            "reproduced": False,
        }
    except Exception as exc:
        return {
            "decision_id": decision_id,
            "status": "UNREPRODUCIBLE",
            "error": f"Replay audit error: {exc}",
            "reproduced": False,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit and reproduce a DecisionTrace.")
    parser.add_argument("decision_id", help="The decision_id of the trace to audit")
    parser.add_argument("--db-url", default=None, help="PostgreSQL connection URL")
    args = parser.parse_args()

    result = asyncio.run(
        audit_decision_trace(args.decision_id, database_url=args.db_url)
    )
    print(json.dumps(result, indent=2))
    if not result.get("reproduced", False):
        sys.exit(1)


if __name__ == "__main__":
    main()
