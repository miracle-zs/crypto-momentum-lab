# Trading Lease And Risk Gateway Design

Date: 2026-07-03

## 1. Status And Scope

This document defines the phase that decides whether a selected strategy is
allowed to affect the Binance account. It introduces the account trading lease,
strategy live state, order-intent eligibility, and layered risk approval.

This phase includes:

- single-strategy account trading lease;
- strategy live lifecycle states;
- risk configuration snapshots;
- symbol, strategy, and account risk checks;
- order intent rejection records;
- halt, resume, drain, and lease-expiry behavior;
- read-only integration with the account synchronization state.

This phase excludes:

- submitting real orders;
- canceling or flattening exchange positions;
- user interface controls;
- running multiple live strategies at the same time;
- dynamic portfolio allocation across strategies.

## 2. Design Position

Before an order can be submitted, the system must prove:

```text
selected strategy owns lease
+ market state is fresh
+ symbol is eligible
+ account state is reconciled
+ risk limits allow exposure
= approved order intent
```

The risk gateway is not an exchange client. It accepts standardized strategy
order intents and returns approval or rejection. The execution state machine
uses only approved intents.

## 3. Trading Lease

Add a `trading_leases` table:

- `lease_id`;
- `environment`;
- `account_label`;
- `strategy_name`;
- `strategy_version`;
- `config_hash`;
- `run_id`;
- `state`: `PENDING`, `ACTIVE`, `DRAINING`, `EXPIRED`, `RELEASED`, `HALTED`;
- `acquired_at`;
- `renewed_at`;
- `expires_at`;
- `released_at`;
- `release_reason`;
- `fencing_token`.

Only one lease may be `ACTIVE` for an `(environment, account_label)` pair.
Lease renewal is periodic. Loss of renewal prevents new exposure.

## 4. Strategy Lifecycle

Live strategy states:

```text
STARTING -> SYNCING -> READY -> TRADING -> DRAINING
READY/TRADING -> HALTED
DRAINING -> RELEASED
```

`TRADING` requires:

- active lease;
- fresh market data;
- account reconciliation `READY`;
- risk gateway `READY`;
- strategy checkpoint loaded;
- selected strategy config hash matches lease.

`DRAINING` rejects new entry intents but allows reduce-only or controlled exit
intents.

## 5. Risk Layers

### 5.1 Symbol Layer

Checks:

- symbol is in current target universe for new entries;
- held symbols remain monitored;
- market state is fresh;
- spread and midpoint are available;
- turnover/depth checks pass when data exists;
- contract status is trading;
- tick size, step size, min quantity, and min notional are known;
- symbol cooldown is not active.

### 5.2 Strategy Layer

Checks:

- strategy owns lease;
- intent belongs to current run/config;
- intent has not expired;
- max concurrent positions;
- max notional per position;
- max entries per time window;
- strategy session loss;
- consecutive rejection or loss pause;
- maximum holding duration.

### 5.3 Account Layer

Checks:

- account reconciliation is fresh;
- available balance reserve;
- gross and net exposure;
- margin utilization;
- daily loss;
- maximum drawdown;
- order rate;
- global halt.

## 6. Risk Records

Add tables:

- `risk_config_snapshots`;
- `risk_evaluations`;
- `risk_rejections`;
- `risk_halts`;
- `strategy_live_states`.

Every order intent receives one `risk_evaluation` row. Rejected intents include
machine-readable reason codes and measured values.

Risk configs are immutable per run. A live run may not silently change numeric
limits. Changing limits requires a new config hash or an explicit audited
operator action.

## 7. Halt Semantics

Global halt:

- rejects new and exposure-increasing intents;
- preserves account synchronization;
- allows cancel, reduce-only, and emergency flatten commands once those phases
  exist;
- requires reconciliation and explicit operator or rule-based clearance before
  returning to `READY`.

Strategy halt:

- applies to one strategy lease;
- may allow another strategy only after flat-account handover succeeds.

Symbol halt:

- blocks new entries for the symbol;
- held positions remain monitored.

## 8. Error Handling

An intent is rejected, not retried, when:

- the lease is missing, expired, or mismatched;
- account state is stale;
- market state is stale;
- symbol eligibility fails;
- risk limit would be breached;
- precision metadata is missing;
- a prior order for the symbol is unresolved.

The gateway enters `HALTED` when:

- account reconciliation is lost;
- database cannot persist evaluations;
- risk config cannot be decoded;
- repeated rejected intents indicate strategy malfunction;
- clock drift exceeds tolerance.

## 9. Testing Strategy

Unit tests cover:

- only one active lease can exist;
- lease expiry rejects new entries;
- risk checks produce deterministic approvals and rejections;
- halt states block exposure-increasing intents;
- `DRAINING` allows only reduce-only intents;
- risk config hash is stable.

Integration tests cover:

- PostgreSQL lease acquisition with concurrent attempts;
- risk evaluation persistence;
- restart with active lease and fencing token;
- account stale state causing risk halt.

## 10. Acceptance Criteria

This phase is complete when:

1. exactly one selected strategy can hold the account trading lease;
2. every order intent is either approved or rejected with durable evidence;
3. stale market/account state prevents new exposure;
4. halt and draining states behave deterministically;
5. no real exchange orders are submitted;
6. unit, integration, concurrency, ruff, and mypy checks pass.
