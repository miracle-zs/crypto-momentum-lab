"""Compatibility exports for the shared entry-policy comparison contract."""

from crypto_momentum_lab.domain.strategy.entry_policy_compare import (
    EntryPolicyComparison,
    EntryPolicyComparisonRequest,
    EntryPolicyComparisonSummary,
    compare_entry_candidate,
    compare_entry_policy_request,
    summarize_entry_policy_comparisons,
    universe_snapshot_for_symbols,
)

__all__ = [
    "EntryPolicyComparison",
    "EntryPolicyComparisonRequest",
    "EntryPolicyComparisonSummary",
    "compare_entry_candidate",
    "compare_entry_policy_request",
    "summarize_entry_policy_comparisons",
    "universe_snapshot_for_symbols",
]
