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
    "SimulatedFillResult",
    "SimulationExecutionAdapter",
    "build_decision_input",
    "compute_decision_input_hash",
    "decide",
    "map_decision_rejection_reason",
]
