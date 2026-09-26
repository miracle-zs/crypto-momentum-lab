"""DecisionEngine and SimulationExecution contracts (R3)."""

from crypto_momentum_lab.domain.decision.decision_engine import (
    DecisionEngine,
    DecisionInput,
    DecisionResult,
    EffectivePolicy,
    FrozenDecisionInputs,
    PolicyState,
    build_decision_input,
    compute_decision_input_hash,
    decide,
    map_decision_rejection_reason,
)
from crypto_momentum_lab.domain.decision.decision_frame import (
    ClockEvent,
    DecisionFrame,
)
from crypto_momentum_lab.domain.decision.policy_transition import (
    PolicyTransition,
    StrategyPolicy,
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

__all__ = [
    "ClockEvent",
    "DecisionEngine",
    "DecisionFrame",
    "DecisionInput",
    "DecisionResult",
    "EffectivePolicy",
    "FillModel",
    "FrozenDecisionInputs",
    "PolicyState",
    "PolicyTransition",
    "SimulatedFillResult",
    "SimulationExecutionAdapter",
    "StrategyPolicy",
    "StrategyPositionMode",
    "TimerRequest",
    "build_decision_input",
    "compute_decision_input_hash",
    "compute_transition_input_hash",
    "decide",
    "execute_policy_transition",
    "map_decision_rejection_reason",
]
