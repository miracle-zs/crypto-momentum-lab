# Execution Account Read-Only Sync Design

Date: 2026-07-03

## 1. Status And Scope

This document defines the first `execution-account` phase. The goal is to
connect to a Binance USD-M Futures account in read-only operational mode and
persist reconciled account state without submitting, canceling, or amending
orders.

This phase includes:

- `execution-account` application boundary;
- authenticated Binance client wrapper for account reads;
- user-data stream consumer for account/order/fill updates;
- REST reconciliation loop;
- local PostgreSQL tables for account snapshots, positions, orders, fills,
  commissions, funding, and reconciliation events;
- account configuration validation for position mode, margin mode, leverage,
  and permissions;
- structured health, heartbeat, and account-sync status.

This phase excludes:

- order submission, cancellation, amendment, and flattening;
- risk approval;
- trading leases;
- strategy order intent claiming;
- dashboard write actions;
- multi-account operation.

## 2. Design Position

Real order execution should not be added until the system can prove what the
account currently contains. Binance remains the authority for balances,
positions, open orders, fills, and account settings. PostgreSQL stores the
local recovery record and audit trail.

The process flow is:

```text
Binance private REST + user-data stream
-> normalized account events
-> PostgreSQL account state
-> reconciliation status
```

Implementation must verify current Binance USD-M API details against official
documentation at implementation time. The design intentionally names data
categories rather than hard-coding endpoint versions.

## 3. Application Boundary

Add:

```text
src/crypto_momentum_lab/apps/execution_account/
src/crypto_momentum_lab/execution_account/
src/crypto_momentum_lab/domain/account/
src/crypto_momentum_lab/domain/execution/
```

The `execution-account` process owns authenticated Binance connectivity. No
strategy package, runner command, or market-data process may import the private
client directly.

## 4. Credential And Permission Model

Credentials are supplied by environment variables or secret files:

- API key;
- API secret;
- account environment name;
- optional read/write permission expectation.

This read-only phase requires the implementation to refuse startup if:

- credentials are missing;
- server time synchronization is outside configured tolerance;
- account type is not USD-M Futures;
- position mode is not one-way mode;
- required account reads fail;
- API key permission does not match configured mode.

Write/trade permission may exist on the key for later phases, but this phase
does not invoke write endpoints.

## 5. PostgreSQL State

Add tables for latest state and audit history:

- `account_balance_snapshots`;
- `account_position_snapshots`;
- `account_open_orders`;
- `account_order_events`;
- `account_fill_events`;
- `account_funding_events`;
- `account_config_snapshots`;
- `account_reconciliation_runs`;
- `execution_account_process_states`.

Rows include:

- `environment`;
- `account_id_hash` or configured account label;
- exchange timestamps when available;
- local received timestamps;
- source type: `rest_snapshot`, `user_stream`, or `reconciliation`;
- raw payload reference or compact JSON payload;
- schema version.

Sensitive secrets are never persisted. Account identifiers should be hashed or
configured labels unless a raw exchange account ID is required for support.

## 6. Synchronization Model

Startup sequence:

1. validate credentials and clock;
2. fetch account configuration;
3. fetch balances, positions, open orders, and recent fills;
4. persist a REST snapshot;
5. start user-data stream;
6. reconcile stream updates against local state;
7. mark process state `READY_READONLY`.

Steady state:

- user-data stream updates are persisted as events;
- periodic REST reconciliation runs compare Binance state to local state;
- mismatches create reconciliation records;
- severe mismatches move process state to `DEGRADED` or `HALTED_READONLY`.

## 7. Reconciliation Rules

The process records a mismatch when:

- a Binance open order is missing locally;
- a local open order is absent from Binance without a terminal event;
- a non-zero Binance position is missing locally;
- local position size differs from Binance;
- recent fill IDs are missing or duplicated;
- account configuration differs from expected live config.

Read-only mismatch handling records evidence and halts live eligibility. It
does not cancel or place orders.

## 8. Error Handling

Recoverable:

- temporary REST failure;
- user-data stream reconnect;
- delayed event arrival;
- rate-limit backoff within configured budget.

Unrecoverable for read-only readiness:

- credential validation failure;
- account mode mismatch;
- persistent REST/user-stream disagreement;
- clock drift outside tolerance;
- database write failure;
- unknown payload schema for a critical account event.

## 9. Testing Strategy

Unit tests cover:

- credential validation paths without real secrets;
- normalization of account snapshots, positions, orders, and fills;
- reconciliation mismatch classification;
- process state transitions;
- no write endpoint is called in read-only mode.

Integration tests cover:

- PostgreSQL table creation;
- idempotent event persistence;
- reconciliation run persistence;
- process restart from existing latest state.

Live tests are manual-gated and require explicit credentials. They verify
read-only account snapshots and user-data stream connectivity without trading.

## 10. Acceptance Criteria

This phase is complete when:

1. `execution-account` can start with Binance credentials and persist account
   state without placing orders;
2. balances, positions, open orders, fills, funding, and account config are
   represented locally;
3. REST reconciliation detects mismatches and records audited evidence;
4. account uncertainty prevents future live eligibility;
5. no strategy or runner can call private Binance clients directly;
6. unit, integration, ruff, mypy, and manual-gated read-only smoke tests pass.
