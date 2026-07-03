# Real Binance Live Trading Roadmap Design

Date: 2026-07-03

## 1. Status And Scope

This document orders the remaining design phases required to move
`crypto-momentum-lab` from live-data paper trading to a real Binance USD-M
Futures account.

The target is:

```text
market-data -> selected strategy -> risk-approved order intents
-> execution-account -> Binance USD-M account
```

The current system has completed the research, replay, paper, paper
persistence, and live closed-state paper source layers. It has not yet added
authenticated account connectivity, risk approval, exchange order submission,
shadow mode, small-capital live rollout, or an operator dashboard.

## 2. Delivery Order

The remaining backend and operator phases are:

1. live paper daemon hardening;
2. runtime promotion for all three independent strategies;
3. read-only `execution-account` account synchronization;
4. trading lease and risk gateway;
5. order intent execution state machine;
6. shadow operation;
7. small-capital live rollout;
8. operator dashboard.

This order is intentional. The system should not submit real orders until it
can run live-data paper sessions continuously, prove account reconciliation,
evaluate risk deterministically, and survive restart/recovery tests.

## 3. Phase Dependencies

### 3.1 Live Paper Daemon Hardening

Required before any account work. It proves the selected strategy can consume
closed live market states continuously with restart recovery and stale-data
halts.

### 3.2 Runtime Strategy Promotion

Required before the user can choose any of the three strategy families. Each
strategy must expose the same runtime contract and paper validation path.

### 3.3 Read-Only Execution Account

Required before risk or order execution. This phase proves private Binance
credentials, account stream handling, REST reconciliation, and local account
state persistence without placing orders.

### 3.4 Trading Lease And Risk Gateway

Required before the strategy can affect the account. It ensures exactly one
strategy owns the account lease and every order intent passes explicit risk and
eligibility checks.

### 3.5 Order Execution State Machine

Required before live orders. It turns approved intents into idempotent
exchange orders and reconciles ambiguous submissions, rejects, partial fills,
and cancels.

### 3.6 Shadow Operation

Required before small-capital trading. It runs the full production path with
real account state and real exchange metadata but suppresses order submission.

### 3.7 Small-Capital Live Rollout

Final production gate. It enables one selected strategy with fixed small
limits, strict halts, manual rollback, and post-run reconciliation.

### 3.8 Operator Dashboard

Can be built after read-only account sync starts. It should be read-only first,
then add tightly controlled actions such as halt, resume, cancel-all, and
flatten only after the backend commands are audited.

## 4. Non-Negotiable Invariants

The remaining phases must preserve these rules:

- a strategy never calls Binance trading APIs directly;
- only `execution-account` owns authenticated Binance connectivity;
- exactly one live strategy can own the trading lease;
- Binance is the authority for balances, positions, orders, and fills;
- PostgreSQL is the durable local record and recovery index;
- all order submissions use deterministic client order IDs;
- ambiguous order submission is resolved by query before retry;
- account uncertainty, stale data, lease loss, or risk halt prevents new
  exposure;
- reduce-only and emergency actions remain possible during normal-entry halt;
- the new strategy never inherits the old strategy's position.

## 5. Documentation Set

The ordered design documents for the remaining work are:

1. `2026-07-03-live-paper-daemon-hardening-design.md`
2. `2026-07-03-runtime-strategy-promotion-design.md`
3. `2026-07-03-execution-account-readonly-sync-design.md`
4. `2026-07-03-trading-lease-risk-gateway-design.md`
5. `2026-07-03-order-execution-state-machine-design.md`
6. `2026-07-03-shadow-operation-design.md`
7. `2026-07-03-small-capital-live-rollout-design.md`
8. `2026-07-03-operator-dashboard-design.md`

Each document is independently implementable and should receive its own
implementation plan before code changes.

## 6. Acceptance Criteria

The roadmap is complete when:

1. the ordered spec set exists and has no overlapping ownership ambiguity;
2. each phase states its scope, exclusions, data flow, failure handling, tests,
   and acceptance criteria;
3. no phase introduces real order submission before account synchronization,
   risk approval, and shadow operation are stable;
4. implementation can proceed one phase at a time without redesigning earlier
   contracts.
