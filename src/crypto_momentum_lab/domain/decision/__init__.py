"""DecisionEngine and SimulationExecution contracts (R3)."""

from crypto_momentum_lab.domain.decision.decision_engine import (
    DecisionInput,
    DecisionResult,
    EffectivePolicy,
    FrozenDecisionInputs,
    PolicyState,
    build_decision_input,
    compute_decision_input_hash,
    decide,
    decision_trace_from_result,
)
from crypto_momentum_lab.domain.decision.decision_frame import (
    ClockEvent,
    DecisionFrame,
)
from crypto_momentum_lab.domain.decision.policy_transition import (
    PolicyTransition,
    StrategyPositionMode,
    TimerRequest,
    compute_transition_input_hash,
    execute_policy_transition,
)
from crypto_momentum_lab.domain.decision.simulation_execution import (
    FillModel,
    SimulatedFillResult,
    SimulationExecutionAdapter,
)
from crypto_momentum_lab.domain.strategy.sizing import (
    EquityFractionSizingModel,
    FixedNotionalSizingModel,
    SizingModel,
    SizingPlan,
    SizingRejection,
    SymbolLotRules,
    quantize_lot_quantity,
)

__all__ = [
    "ClockEvent",
    "DecisionFrame",
    "DecisionInput",
    "DecisionResult",
    "EffectivePolicy",
    "EquityFractionSizingModel",
    "FillModel",
    "FixedNotionalSizingModel",
    "FrozenDecisionInputs",
    "PolicyState",
    "PolicyTransition",
    "SimulatedFillResult",
    "SimulationExecutionAdapter",
    "SizingModel",
    "SizingPlan",
    "SizingRejection",
    "StrategyPositionMode",
    "SymbolLotRules",
    "TimerRequest",
    "build_decision_input",
    "compute_decision_input_hash",
    "compute_transition_input_hash",
    "decide",
    "decision_trace_from_result",
    "execute_policy_transition",
    "quantize_lot_quantity",
]
