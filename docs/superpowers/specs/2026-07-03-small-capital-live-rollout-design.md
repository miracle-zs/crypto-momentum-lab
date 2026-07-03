# Small-Capital Live Rollout Design

Date: 2026-07-03

## 1. Status And Scope

This document defines the first real-money rollout after shadow mode. It
enables one selected strategy to submit small Binance USD-M Futures orders
under strict limits, audited controls, and manual rollback.

This phase includes:

- live submit enablement for one strategy;
- fixed small notional limits;
- conservative risk and halt settings;
- deployment checklist;
- live session runbook;
- post-session reconciliation and review;
- rollback and emergency procedures.

This phase excludes:

- scaling capital dynamically;
- running multiple live strategies;
- automatic strategy switching;
- multi-account operation;
- advanced portfolio optimization.

## 2. Preconditions

Small-capital live cannot start until:

1. live paper daemon has run stably;
2. all selected strategy runtime tests pass;
3. account read-only sync is stable;
4. risk gateway is active;
5. order execution state machine passes fake-exchange and integration tests;
6. shadow mode has completed the required session and drills;
7. operator has reviewed recent shadow decisions;
8. emergency halt and cancel controls are available.

## 3. Live Enablement

Live submission requires all of:

- config flag `live_submit_enabled=true`;
- selected account label;
- selected strategy lease;
- submit policy `live_submit`;
- risk config with explicit numeric limits;
- operator approval record;
- current account reconciliation `READY`;
- no global halt.

No implicit defaults may enable live trading.

## 4. Initial Limits

Initial live limits are intentionally small:

- fixed candidate notional;
- maximum one open position unless explicitly changed;
- maximum one new entry per symbol per cooldown window;
- low daily loss limit;
- low gross exposure limit;
- strict spread and min-notional checks;
- strict account freshness and market freshness limits;
- immediate halt on unresolved order uncertainty.

The rollout should prefer one strategy and a narrow symbol universe until
operational evidence supports expansion.

## 5. Session Runbook

Before session:

1. verify git commit and config hash;
2. verify database migrations at head;
3. verify Binance account mode, margin, leverage, balances, and no unexpected
   positions;
4. run read-only reconciliation;
5. run shadow for a short preflight window;
6. acquire live lease;
7. enable live submit;
8. monitor first decision manually.

During session:

- monitor account state, market data, risk state, open orders, fills, and
  process health;
- halt on any unresolved mismatch;
- avoid config changes mid-run.

After session:

- disable live submit;
- reconcile account;
- persist final strategy and account checkpoints;
- export report;
- review signals, rejects, fills, fees, slippage, and halts.

## 6. Rollback And Emergency

Rollback states:

- disable new entries;
- mark strategy `DRAINING`;
- cancel non-reduce-only open orders;
- flatten only with explicit emergency command;
- reconcile until Binance and local state agree;
- release lease after flat-account confirmation.

Emergency flatten must be audited and should require an explicit operator
command unless the risk config defines an automatic threshold.

## 7. Metrics

Live rollout reports:

- signal count;
- approved and rejected intents;
- submitted orders;
- filled, partially filled, canceled, rejected orders;
- realized fees;
- estimated slippage;
- latency from state close to exchange acknowledgement;
- position holding time;
- risk halt events;
- reconciliation mismatches;
- realized PnL and drawdown.

## 8. Testing Strategy

Automated tests cover:

- live submit cannot enable without all gates;
- risk limits block oversized orders;
- unresolved orders halt new exposure;
- session runbook state transitions;
- rollback state transitions;
- final reconciliation report generation.

Manual-gated tests cover:

- operator preflight;
- one tiny live order on the selected account;
- cancel and reconciliation;
- post-session report review.

## 9. Acceptance Criteria

This phase is complete when:

1. one selected strategy can submit small real orders only after explicit live
   enablement;
2. all orders are risk approved, quantized, submitted idempotently, and
   reconciled;
3. halt, rollback, cancel, and reconciliation procedures are tested;
4. final live report includes account and strategy evidence;
5. the account can return to flat and release the lease cleanly;
6. no second strategy can trade concurrently.
