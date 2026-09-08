# SOLV stale exit repair — 2026-09-08

## Confirmed causes

The original 21,654 exit carried the correct old `batch_id`, but position
reconstruction attached exits to the latest accumulator. An intervening
21,819 entry and exit left the old accumulator incorrectly open. Snapshot
reconciliation then assigned the 707 residual to that old accumulator.

`prepare_submission` ignored a conflicting order insert and still returned a
submission journal. Concurrent callers and later retries could therefore send
the same client ID again, including after its original exchange order filled.

The deployed `d779f06` did not fix these paths. Its predecessor only removed the
fallback that invented a batch when no surviving accumulator existed.

## Changes

- A successful unique order insert is now the durable submission grant.
  Existing IDs return no grant, without appending another SUBMITTING event or
  resetting intent state. This survives process restarts and arbitrates across
  exit lanes. Uncertain outcomes continue through the existing reconciliation
  and distinct recovery-order path.
- Late events cannot move a filled order back to an active state. Conflicting
  exchange IDs remain in the event journal but cannot overwrite the order row.
- Both position-loading paths read the batch binding from persisted intents.
  Named exits consume that accumulator, not whichever accumulator was newest
  when the exit was submitted. Older records lacking bindings retain the
  previous chronological interpretation.
- Expired reduce-only candidates are rejected before submission. A fresh exit
  decision can be evaluated on the next observation.
- Exit-only checks reject an unmanaged target symbol; another unmanaged symbol
  no longer prevents known positions from exiting. Entry checks remain global.

## Validation

The PostgreSQL concurrency test failed before the fix: two requests both
received permission to submit (`2 != 1`). The restart case also verifies that
a filled ID cannot be submitted eight hours later.

The interleaved-batch test reproduces 707 being attributed to the old entry.
After the fix it retains the latest entry time. A read-only export of actual
account-3 SOLV orders independently produced these anchors:

- Before: 2026-09-07 20:36:01 Asia/Shanghai.
- After: 2026-09-08 07:38:14 Asia/Shanghai (order-update timestamps in this replay;
  production additionally loads exchange fill timestamps).

214 related tests pass, including PostgreSQL integration tests, order execution,
position reconstruction, expiry, and scheduled flatten/verify/reopen behavior.
Ruff passes and mypy reports no issues in the four changed source files.

## Operational boundaries

This change does not rewrite historical orders, reconstruct each reused exchange
attempt, or submit a one-off liquidation. The historical residual can retain a
merged batch identity where prior repeated exchange attempts were absent from
the order summary, although its new entry time is restored. The original event
and account-fill journals remain the source for historical audit.

Starting a daemon after 08:02 intentionally does not replay that morning's
scheduled liquidation. Existing 707 positions therefore must not be described
as already cleared by this code change. Deployment and live position verification
are separate from the local regression results.

Earlier `unmanaged_live_positions` logs named other symbols; they were not proof
that SOLV itself was unmanaged. The actual SOLV replay shows a managed position
with an incorrect old batch anchor before this repair.

The quote and candle queues remain separate so slow candle loading cannot delay
quotes. Exchange execution already serializes by account/symbol/side; the durable
submission grant adds the missing duplicate protection without coupling feeds.
