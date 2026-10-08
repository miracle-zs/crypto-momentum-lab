"""Pure replay verification for persisted decision-trace values."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from crypto_momentum_lab.domain.market.market_book import UnreproducibleError
from crypto_momentum_lab.domain.market.revision_models import DecisionTrace


def verify_decision_trace(
    trace: DecisionTrace | None, decision_id: str
) -> dict[str, Any]:
    """Verify a trace already loaded by an outer adapter."""
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

        payload = trace.trace_payload

        if payload.get("evidence_level") == "summary":
            return {
                "decision_id": trace.decision_id,
                "status": "SUMMARY_ONLY",
                "error": (
                    "Normal hold decisions retain only hot operational evidence; "
                    "exact replay is unavailable until cold evidence archival "
                    "is enabled"
                ),
                "reproduced": False,
            }

        if payload.get("trace_schema_version") != 1:
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "DecisionTrace lacks the supported frozen-input schema",
                "reproduced": False,
            }
        policy_evidence = payload.get("policy_parameters")
        prior_state_evidence = payload.get("prior_policy_state")
        frame_evidence = payload.get("decision_frame")
        context_evidence = payload.get("decision_context")
        if (
            not isinstance(policy_evidence, dict)
            or policy_evidence.get("serialization_version") != 1
        ):
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "DecisionTrace lacks versioned policy parameters",
                "reproduced": False,
            }
        if (
            not isinstance(prior_state_evidence, dict)
            or prior_state_evidence.get("serialization_version") != 1
        ):
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "DecisionTrace lacks versioned prior policy state",
                "reproduced": False,
            }
        if not isinstance(frame_evidence, dict) or not isinstance(
            context_evidence, dict
        ):
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "DecisionTrace lacks its decision frame or frozen context",
                "reproduced": False,
            }
        next_state_evidence = payload.get("next_policy_state")
        if (
            not isinstance(next_state_evidence, dict)
            or next_state_evidence.get("serialization_version") != 1
        ):
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "DecisionTrace lacks versioned next policy state",
                "reproduced": False,
            }

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
            if not ref.revision_id or not ref.content_hash:
                rev_id = ref.revision_id
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
                    "visibility_mode": ref.visibility_mode.value,
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

        # 4. Semantic replay requires every input needed to rebuild the same frame.
        market_state_payload = payload.get("market_state")
        if not market_state_payload:
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": (
                    "Missing market_state in trace payload; "
                    "semantic decision replay impossible"
                ),
                "reproduced": False,
            }

        from crypto_momentum_lab.domain.decision.decision_engine import (
            ClockEvent,
            DecisionFrame,
            DecisionInput,
            EffectivePolicy,
            PolicyState,
            _serialize_intent_candidate,
            compute_policy_parameters_digest,
            compute_policy_state_digest,
            decide,
            serialize_policy_state,
        )
        from crypto_momentum_lab.domain.decision.policy_transition import (
            canonicalize_policy_value,
        )
        from crypto_momentum_lab.domain.execution.order_state import (
            FuturesPositionSide,
        )
        from crypto_momentum_lab.domain.execution.position_ledger_models import (
            PositionEpisode,
            PositionHealthStatus,
            PositionKey,
            PositionLedgerBatch,
            PositionView,
        )
        from crypto_momentum_lab.domain.market.closed_candle import ClosedCandle15m
        from crypto_momentum_lab.domain.market.revision_models import MarketEnvelope
        from crypto_momentum_lab.domain.market.state_codec import (
            market_state_from_payload,
        )
        from crypto_momentum_lab.domain.strategy.models import (
            EntryType,
            OrderIntentCandidate,
            StrategySide,
        )
        from crypto_momentum_lab.domain.strategy.position_exit import (
            PositionExitMode,
            PositionExitPolicy,
        )
        from crypto_momentum_lab.domain.strategy.sizing import (
            EquityFractionSizingModel,
            FixedNotionalSizingModel,
            SymbolLotRules,
        )

        m_state = market_state_from_payload(market_state_payload)
        ref_by_id = {ref.revision_id: ref for ref in trace.evaluated_market_refs}
        frame_ref_ids = frame_evidence.get("market_revision_ids")
        if not isinstance(frame_ref_ids, list) or not frame_ref_ids:
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "DecisionFrame does not identify its market revisions",
                "reproduced": False,
            }
        if not all(isinstance(ref_id, str) and ref_id for ref_id in frame_ref_ids):
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "DecisionFrame contains an invalid market revision identity",
                "reproduced": False,
            }
        trace_ref_ids = tuple(ref.revision_id for ref in trace.evaluated_market_refs)
        if tuple(frame_ref_ids) != trace_ref_ids:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": (
                    "DecisionFrame market revisions differ from the trace revisions"
                ),
                "reproduced": False,
            }
        if len(ref_by_id) != len(trace.evaluated_market_refs):
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "DecisionTrace has duplicate market revision identities",
                "reproduced": False,
            }
        frame_refs = tuple(ref_by_id[ref_id] for ref_id in frame_ref_ids)
        ref0 = frame_refs[0]
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

        pol_params = policy_evidence
        supported_policy_fields = {
            "serialization_version",
            "policy_id",
            "strategy_name",
            "policy_version",
            "entry_threshold",
            "short_entry_threshold",
            "order_type",
            "target_notional",
            "max_open_positions",
            "max_concurrency_per_symbol",
            "exit_policy",
            "cooldown_duration",
            "cooldown_duration_seconds",
            "position_mode",
            "grace_period",
            "grace_period_seconds",
            "sizing_model",
            "symbol_lot_rules",
        }
        if set(pol_params) - supported_policy_fields:
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "Trace contains policy fields this auditor cannot rebuild",
                "reproduced": False,
            }
        required_policy_fields = {
            "policy_id",
            "strategy_name",
            "policy_version",
            "entry_threshold",
            "order_type",
            "target_notional",
            "max_open_positions",
            "max_concurrency_per_symbol",
            "exit_policy",
            "cooldown_duration_seconds",
            "position_mode",
            "grace_period_seconds",
        }
        if not required_policy_fields.issubset(pol_params):
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "Trace policy parameters are incomplete",
                "reproduced": False,
            }

        def _duration(seconds: Any) -> timedelta:
            micros = Decimal(str(seconds)) * Decimal(1_000_000)
            if micros != micros.to_integral_value():
                raise ValueError("duration precision exceeds microseconds")
            return timedelta(microseconds=int(micros))

        def _model_class(data: Any, name: str) -> bool:
            return (
                isinstance(data, dict)
                and data.get("class") == name
                and data.get("module") == "crypto_momentum_lab.domain.strategy.sizing"
            )

        exit_data = pol_params["exit_policy"]
        if (
            not isinstance(exit_data, dict)
            or exit_data.get("class") != "PositionExitPolicy"
            or exit_data.get("module")
            != "crypto_momentum_lab.domain.strategy.position_exit"
        ):
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "Trace uses an unsupported exit policy type",
                "reproduced": False,
            }
        sizing_data = pol_params.get("sizing_model")
        sizing_model = None
        if sizing_data is not None:
            if _model_class(sizing_data, "FixedNotionalSizingModel"):
                sizing_model = FixedNotionalSizingModel(
                    target_notional=Decimal(str(sizing_data["target_notional"])),
                    max_leverage=Decimal(str(sizing_data["max_leverage"])),
                    max_slippage_budget_bps=Decimal(
                        str(sizing_data["max_slippage_budget_bps"])
                    ),
                    resize_tolerance=Decimal(str(sizing_data["resize_tolerance"])),
                )
            elif _model_class(sizing_data, "EquityFractionSizingModel"):
                sizing_model = EquityFractionSizingModel(
                    fraction_of_equity=Decimal(str(sizing_data["fraction_of_equity"])),
                    min_notional_floor=Decimal(str(sizing_data["min_notional_floor"])),
                    max_notional_cap=Decimal(str(sizing_data["max_notional_cap"])),
                    max_leverage=Decimal(str(sizing_data["max_leverage"])),
                    max_slippage_budget_bps=Decimal(
                        str(sizing_data["max_slippage_budget_bps"])
                    ),
                    resize_tolerance=Decimal(str(sizing_data["resize_tolerance"])),
                )
            else:
                return {
                    "decision_id": trace.decision_id,
                    "status": "EVIDENCE_INSUFFICIENT",
                    "error": "Trace uses an unsupported sizing model type",
                    "reproduced": False,
                }

        lot_rules_data = pol_params.get("symbol_lot_rules")
        lot_rules = None
        if lot_rules_data is not None:
            if not _model_class(lot_rules_data, "SymbolLotRules"):
                return {
                    "decision_id": trace.decision_id,
                    "status": "EVIDENCE_INSUFFICIENT",
                    "error": "Trace uses an unsupported lot-rules type",
                    "reproduced": False,
                }
            lot_rules = SymbolLotRules(
                symbol=lot_rules_data["symbol"],
                tick_size=Decimal(str(lot_rules_data["tick_size"])),
                step_size=Decimal(str(lot_rules_data["step_size"])),
                min_quantity=Decimal(str(lot_rules_data["min_quantity"])),
                max_quantity=Decimal(str(lot_rules_data["max_quantity"])),
                min_notional=Decimal(str(lot_rules_data["min_notional"])),
            )

        from crypto_momentum_lab.domain.decision.policy_transition import (
            StrategyPositionMode,
        )

        exit_policy = PositionExitPolicy(
            max_holding_seconds=exit_data["max_holding_seconds"],
            mode=PositionExitMode(exit_data["mode"]),
            minimum_holding_seconds=int(exit_data["minimum_holding_seconds"]),
            candle_confirmation_count=int(exit_data["candle_confirmation_count"]),
        )
        position_mode = StrategyPositionMode(pol_params["position_mode"])
        order_type = EntryType(pol_params["order_type"])

        candidate_generation_mode = payload.get("candidate_generation_mode")
        input_candidate_data = payload.get("input_candidate")
        input_candidate = None
        if candidate_generation_mode == "injected_candidate":
            if not isinstance(input_candidate_data, dict):
                return {
                    "decision_id": trace.decision_id,
                    "status": "EVIDENCE_INSUFFICIENT",
                    "error": (
                        "Trace lacks the injected candidate used as decision input"
                    ),
                    "reproduced": False,
                }
            input_candidate = OrderIntentCandidate(
                candidate_id=input_candidate_data["candidate_id"],
                signal_id=input_candidate_data["signal_id"],
                run_id=input_candidate_data["run_id"],
                strategy_name=input_candidate_data["strategy_name"],
                strategy_version=input_candidate_data["strategy_version"],
                config_hash=input_candidate_data["config_hash"],
                symbol=input_candidate_data["symbol"],
                side=StrategySide(input_candidate_data["side"]),
                entry_type=EntryType(input_candidate_data["entry_type"]),
                limit_price=(
                    Decimal(str(input_candidate_data["limit_price"]))
                    if input_candidate_data.get("limit_price") is not None
                    else None
                ),
                desired_notional=(
                    Decimal(str(input_candidate_data["desired_notional"]))
                    if input_candidate_data.get("desired_notional") is not None
                    else None
                ),
                reduce_only=bool(input_candidate_data["reduce_only"]),
                expires_at=datetime.fromisoformat(input_candidate_data["expires_at"]),
                created_at=datetime.fromisoformat(input_candidate_data["created_at"]),
                reason=input_candidate_data["reason"],
                features=dict(input_candidate_data["features"]),
            )
        elif candidate_generation_mode != "policy":
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "Trace does not identify how its entry candidate was supplied",
                "reproduced": False,
            }

        policy = EffectivePolicy(
            policy_id=str(pol_params["policy_id"]),
            strategy_name=str(pol_params["strategy_name"]),
            policy_version=int(pol_params["policy_version"]),
            entry_threshold=(
                Decimal(str(pol_params["entry_threshold"]))
                if pol_params.get("entry_threshold") is not None
                else None
            ),
            short_entry_threshold=(
                Decimal(str(pol_params["short_entry_threshold"]))
                if pol_params.get("short_entry_threshold") is not None
                else None
            ),
            order_type=order_type,
            target_notional=Decimal(str(pol_params["target_notional"])),
            max_open_positions=(
                int(pol_params["max_open_positions"])
                if pol_params["max_open_positions"] is not None
                else None
            ),
            max_concurrency_per_symbol=pol_params["max_concurrency_per_symbol"],
            exit_policy=exit_policy,
            cooldown_duration=_duration(pol_params["cooldown_duration_seconds"]),
            position_mode=position_mode,
            grace_period=_duration(pol_params["grace_period_seconds"]),
            sizing_model=sizing_model,
            symbol_lot_rules=lot_rules,
            candidate_generator=(
                (lambda _input, _state: input_candidate)
                if input_candidate is not None
                else None
            ),
        )

        # Rebuild the original account view, including its complete identity and
        # open-position episode. Missing identity is not replaced with defaults.
        ctx = context_evidence
        pos_view_data = ctx.get("position_view")
        if not isinstance(pos_view_data, dict):
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "Trace lacks the frozen position view",
                "reproduced": False,
            }
        position_key_data = pos_view_data.get("position_key")
        if not isinstance(position_key_data, dict):
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "Trace lacks complete position scope identity",
                "reproduced": False,
            }
        pos_key = PositionKey(
            environment=position_key_data["environment"],
            account_label=position_key_data["account_label"],
            symbol=position_key_data["symbol"],
            position_side=FuturesPositionSide(position_key_data["position_side"]),
        )
        batches_list = []
        for batch_data in pos_view_data.get("batches", ()):
            quantity = Decimal(str(batch_data["quantity"]))
            batches_list.append(
                PositionLedgerBatch(
                    batch_id=batch_data["batch_id"],
                    episode_id=batch_data["episode_id"],
                    quantity=quantity,
                    original_quantity=Decimal(str(batch_data["original_quantity"])),
                    entry_price=Decimal(str(batch_data["entry_price"])),
                    opened_at=datetime.fromisoformat(batch_data["opened_at"]),
                    entry_order_ids=tuple(batch_data["entry_order_ids"]),
                    entry_client_order_ids=tuple(batch_data["entry_client_order_ids"]),
                    exit_order_submitted_at=(
                        datetime.fromisoformat(batch_data["exit_order_submitted_at"])
                        if batch_data.get("exit_order_submitted_at")
                        else None
                    ),
                )
            )
        episode_data = pos_view_data.get("active_episode")
        active_episode = None
        if episode_data is not None:
            active_episode = PositionEpisode(
                episode_id=episode_data["episode_id"],
                position_key=pos_key,
                side=StrategySide(episode_data["side"]),
                opened_at=datetime.fromisoformat(episode_data["opened_at"]),
                closed_at=(
                    datetime.fromisoformat(episode_data["closed_at"])
                    if episode_data.get("closed_at")
                    else None
                ),
                is_active=bool(episode_data["is_active"]),
                cumulative_bought=Decimal(str(episode_data["cumulative_bought"])),
                cumulative_sold=Decimal(str(episode_data["cumulative_sold"])),
                peak_quantity=Decimal(str(episode_data["peak_quantity"])),
                batches=tuple(batches_list),
            )
        pos_view = PositionView(
            key=pos_key,
            projection_version=pos_view_data["projection_version"],
            input_revision=int(pos_view_data["input_revision"]),
            event_cut=(
                datetime.fromisoformat(pos_view_data["event_cut"])
                if pos_view_data.get("event_cut")
                else None
            ),
            policy_version=pos_view_data["policy_version"],
            schema_version=pos_view_data["schema_version"],
            coverage=None,
            active_episode=active_episode,
            batches=tuple(batches_list),
            unallocated_quantity=Decimal(str(pos_view_data["unallocated_quantity"])),
            reconciliation_gap=(
                Decimal(str(pos_view_data["reconciliation_gap"]))
                if pos_view_data.get("reconciliation_gap") is not None
                else None
            ),
            health_status=PositionHealthStatus(pos_view_data["health_status"]),
            is_comparable=bool(pos_view_data["is_comparable"]),
        )

        prior_state_data = prior_state_evidence
        prior_state = PolicyState(
            policy_version=int(prior_state_data["policy_version"]),
            cooldown_until_by_symbol={
                key: datetime.fromisoformat(value)
                for key, value in prior_state_data.get("cooldown_until", {}).items()
            },
            anchor_prices_by_symbol={
                key: Decimal(str(value))
                for key, value in prior_state_data.get("anchor_prices", {}).items()
            },
            active_intent_ids_by_symbol=dict(
                prior_state_data.get("active_intent_ids", {})
            ),
            warmup_status=dict(prior_state_data.get("warmup_status", {})),
            grace_until_by_symbol={
                key: datetime.fromisoformat(value)
                for key, value in prior_state_data.get("grace_until", {}).items()
            },
            holding_deadline_by_symbol={
                key: datetime.fromisoformat(value)
                for key, value in prior_state_data.get("holding_deadline", {}).items()
            },
            signal_memory=dict(prior_state_data.get("signal_memory", {})),
            custom_state=dict(prior_state_data.get("custom_state", {})),
            sizing_state_by_symbol=dict(prior_state_data.get("sizing_state", {})),
        )

        policy_digest = frame_evidence.get("policy_parameters_digest")
        state_digest = frame_evidence.get("policy_state_digest")
        if not isinstance(policy_digest, str) or not policy_digest:
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "DecisionFrame lacks its policy parameters digest",
                "reproduced": False,
            }
        if not isinstance(state_digest, str) or not state_digest:
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "DecisionFrame lacks its prior policy state digest",
                "reproduced": False,
            }
        if compute_policy_parameters_digest(policy) != policy_digest:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": (
                    "Reconstructed policy parameters differ from "
                    "the DecisionFrame digest"
                ),
                "reproduced": False,
            }
        if compute_policy_state_digest(prior_state) != state_digest:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": (
                    "Reconstructed prior policy state differs from "
                    "the DecisionFrame digest"
                ),
                "reproduced": False,
            }

        frame_clock_data = frame_evidence.get("clock_event")
        input_clock_data = payload.get("clock_event")
        if not isinstance(frame_clock_data, dict) or not isinstance(
            input_clock_data, dict
        ):
            return {
                "decision_id": trace.decision_id,
                "status": "EVIDENCE_INSUFFICIENT",
                "error": "Trace lacks its decision clock event",
                "reproduced": False,
            }
        frame_clock = ClockEvent(
            timestamp=datetime.fromisoformat(frame_clock_data["timestamp"]),
            sequence=int(frame_clock_data["sequence"]),
            event_type=frame_clock_data["event_type"],
        )
        input_clock = ClockEvent(
            timestamp=datetime.fromisoformat(input_clock_data["timestamp"]),
            sequence=int(input_clock_data["sequence"]),
            event_type=input_clock_data["event_type"],
        )
        if frame_clock != input_clock:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": (
                    "DecisionInput clock differs from the frozen DecisionFrame clock"
                ),
                "reproduced": False,
            }
        frame = DecisionFrame(
            scope=frame_evidence["scope"],
            symbol=frame_evidence["symbol"],
            market_refs=frame_refs,
            position_view_token=frame_evidence["position_view_token"],
            clock_event=frame_clock,
            universe_version=frame_evidence["universe_version"],
            risk_config_version=frame_evidence["risk_config_version"],
            policy_code_digest=frame_evidence["policy_code_digest"],
            policy_parameters_digest=frame_evidence["policy_parameters_digest"],
            policy_state_digest=frame_evidence["policy_state_digest"],
            risk_plan_digest=frame_evidence["risk_plan_digest"],
            cash_balance=Decimal(str(frame_evidence["cash_balance"])),
            max_clock_skew=_duration(frame_evidence["max_clock_skew_seconds"]),
        )
        if frame.frame_digest != trace.frame_digest:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": "Rebuilt DecisionFrame digest differs from recorded digest",
                "reproduced": False,
            }
        if frame.position_view_token != pos_view.projection_version:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": (
                    "DecisionFrame position token differs from frozen position view"
                ),
                "reproduced": False,
            }

        closed_candles = tuple(
            ClosedCandle15m(
                symbol=item["symbol"],
                candle_start=datetime.fromisoformat(item["candle_start"]),
                candle_end=datetime.fromisoformat(item["candle_end"]),
                open_price=Decimal(str(item["open_price"])),
                close_price=Decimal(str(item["close_price"])),
            )
            for item in payload.get("closed_candles", ())
        )
        dec_input = DecisionInput(
            symbol=ref0.symbol,
            market_ref=ref0,
            market_envelope=envelope,
            position_view=pos_view,
            universe_version=ctx["universe_version"],
            clock_event=input_clock,
            cash_balance=Decimal(str(ctx["cash_balance"])),
            risk_config_version=ctx["risk_config_version"],
            frame=frame,
            closed_candles=closed_candles,
        )
        replayed_result = decide(dec_input, prior_state, policy)

        if replayed_result.input_hash != trace.input_hash:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": "Replayed input hash differs from recorded input hash",
                "reproduced": False,
            }
        if replayed_result.frame_digest != trace.frame_digest:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": "Replayed frame digest differs from recorded frame digest",
                "reproduced": False,
            }
        if replayed_result.decision_id != trace.decision_id:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": "Replayed decision identity differs from recorded identity",
                "reproduced": False,
            }
        if replayed_result.rejection_reason != trace.rejection_reason:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": "Replayed rejection reason differs from recorded reason",
                "reproduced": False,
            }
        if replayed_result.intent is not None:
            replayed_intent = _serialize_intent_candidate(replayed_result.intent)
        else:
            replayed_intent = None
        if replayed_intent != output_intent:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": "Replayed intent differs from the complete recorded intent",
                "reproduced": False,
            }
        replayed_exit = (
            canonicalize_policy_value(replayed_result.exit_command)
            if replayed_result.exit_command is not None
            else None
        )
        if replayed_exit != payload.get("output_exit_command"):
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": (
                    "Replayed trade command differs from the complete recorded command"
                ),
                "reproduced": False,
            }
        replayed_next_state = serialize_policy_state(replayed_result.next_policy_state)
        if replayed_next_state != next_policy_state:
            return {
                "decision_id": trace.decision_id,
                "status": "UNREPRODUCIBLE",
                "error": "Replayed next policy state differs from recorded state",
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
