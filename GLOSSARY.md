# Market Data

This context records versioned market states so decisions and datasets can identify which market data they use.

## Language

**Canonical market revision**:
The selected authoritative market-data revision for one symbol, interval, and time bucket. A sequence of these selected revisions across buckets is the canonical market history.
_Avoid_: every raw feed update; the exact revision seen by a particular decision (that is decision-visible data).

**Decision replay**:
An audit that reconstructs or validates a past strategy decision from its recorded inputs and outcome. It is diagnostic, not part of live order execution; a canonical replay is a counterfactual using the selected market history rather than necessarily the exact input originally seen.

**Dataset manifest**:
An immutable description of a time-bounded market-data set that identifies the exact market revisions included, so research or backtesting can use a repeatable input set.
