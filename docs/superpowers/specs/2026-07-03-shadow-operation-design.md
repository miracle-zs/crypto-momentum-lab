# Shadow Operation Design

Date: 2026-07-03

## 1. Status And Scope

This document defines shadow operation: the system runs the production live
path against real market data, real account state, real exchange metadata, and
real risk checks, but suppresses exchange order submission.

This phase includes:

- live strategy lease in shadow mode;
- real account synchronization;
- real risk gateway evaluation;
- order intent planning and quantization;
- simulated execution decision records;
- comparison between planned orders and paper fill assumptions;
- operational halt and restart drills.

This phase excludes:

- placing, canceling, or amending real orders;
- emergency flattening through Binance;
- dynamic position sizing;
- multiple simultaneous live strategies.

## 2. Design Position

Shadow mode is the final proof step before capital is at risk:

```text
live market states -> selected strategy -> order intents
-> risk approval -> execution plan -> shadow suppression
```

It must use the same code path as live trading up to the submission boundary.
The only difference is that the final exchange submit call is replaced by a
durable `SHADOW_SUPPRESSED` event.

## 3. Mode Semantics

Add run mode:

```text
SHADOW
```

In `SHADOW`:

- strategy lease is required;
- account sync must be `READY`;
- market data must be fresh;
- risk checks are enforced;
- order plans are quantized using real exchange metadata;
- no write endpoint is called;
- order plans expire and resolve locally as shadow records.

The execution state machine should support a submit policy:

```text
submit_policy = shadow_suppress | live_submit
```

Only `live_submit` can call Binance write endpoints.

## 4. Shadow Records

Persist:

- strategy signals;
- order intents;
- risk evaluations;
- quantized order plans;
- suppression events;
- paper/simulated fill comparison;
- missed-opportunity and reject summaries;
- account and market readiness status at decision time.

The shadow order plan should include the exact order that would have been sent:

- symbol;
- side;
- type;
- quantity;
- price;
- reduce-only;
- client order ID;
- expiration;
- reason for suppression.

## 5. Comparison Metrics

Shadow reports include:

- number of signals;
- approved intents;
- rejected intents by reason;
- would-submit orders;
- paper fill outcome;
- estimated live fill feasibility from bid/ask and spread;
- latency from state close to order plan;
- account/risk blocks;
- stale data blocks;
- mismatch between paper assumptions and live account constraints.

These metrics decide whether small-capital live can start.

## 6. Drills

Before small-capital live, shadow mode must pass drills:

- market-data reconnect;
- account user-stream reconnect;
- database restart or temporary failure;
- process restart with active lease;
- strategy halt;
- stale market data;
- risk daily-loss halt using fixture data;
- order submission ambiguity simulation with fake client.

## 7. Error Handling

Shadow mode halts when:

- account reconciliation is lost;
- market data is stale;
- risk gateway is halted;
- strategy lease expires;
- order plan cannot be quantized;
- the code path attempts a Binance write call.

The last condition is a test failure and production safety violation.

## 8. Testing Strategy

Unit tests cover:

- shadow submit policy never calls write endpoints;
- risk-approved intents produce shadow suppression records;
- rejected intents do not produce order plans;
- client order IDs are still deterministic in shadow;
- shadow reports include latency and reject metrics.

Integration tests cover:

- full PostgreSQL path from state to shadow order plan;
- restart with active shadow lease;
- account stale state halting shadow mode.

Manual-gated tests cover:

- real account read-only connectivity;
- one full trading session in shadow mode;
- operator review of shadow decisions.

## 9. Acceptance Criteria

This phase is complete when:

1. the selected strategy can run in shadow mode for a configured session;
2. no Binance write endpoint is called;
3. every would-be order is risk checked, quantized, and durably suppressed;
4. shadow metrics expose latency, rejects, and paper/live assumption gaps;
5. restart and halt drills pass;
6. small-capital live remains disabled until operator approval.
