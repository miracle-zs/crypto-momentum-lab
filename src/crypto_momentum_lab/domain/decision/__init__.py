"""DecisionEngine and SimulationExecution contracts (R3)."""

from crypto_momentum_lab.domain.decision.decision_engine import (
    ClockEvent,
    DecisionInput,
    DecisionResult,
    EffectivePolicy,
    PolicyState,
    compute_decision_input_hash,
    decide,
)
from crypto_momentum_lab.domain.decision.simulation_execution import (
    FillModel,
    SimulatedFillResult,
    SimulationExecutionAdapter,
)

__all__ = [
    "ClockEvent",
    "DecisionInput",
    "DecisionResult",
    "EffectivePolicy",
    "FillModel",
    "PolicyState",
    "SimulatedFillResult",
    "SimulationExecutionAdapter",
    "compute_decision_input_hash",
    "decide",
]
