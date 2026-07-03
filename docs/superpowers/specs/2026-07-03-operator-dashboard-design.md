# Operator Dashboard Design

Date: 2026-07-03

## 1. Status And Scope

This document defines the operator dashboard needed for real Binance account
operation. The dashboard is not required for the backend to compile, but it is
strongly recommended before small-capital live trading because terminal logs
alone are not enough for safe operation.

This phase includes:

- read-only dashboard MVP;
- service status views;
- selected strategy and lease view;
- market-data freshness and universe view;
- account balances, positions, open orders, and fills;
- signals, order intents, risk decisions, and halts;
- paper, shadow, and live run reports;
- later controlled actions with explicit confirmation.

This phase excludes:

- public user accounts;
- multi-tenant access control;
- mobile app;
- strategy optimization UI;
- unaudited live trading controls.

## 2. Design Position

The dashboard should start read-only. Write actions such as halt, resume,
cancel-all, and flatten should be added only after backend command records and
audit trails exist.

The dashboard reads PostgreSQL and backend health endpoints. It must not call
Binance directly.

## 3. MVP Screens

### 3.1 System Overview

Shows:

- service states: `market-data`, `strategy-runner`, `execution-account`;
- database connectivity;
- latest market-data heartbeat;
- latest account reconciliation;
- active global halt;
- active strategy lease;
- current run mode: paper, daemon, shadow, or live.

### 3.2 Universe And Market Data

Shows:

- current top 20 gainers and top 20 losers;
- monitoring universe;
- readiness by symbol;
- latest closed state timestamp;
- stream gaps and stale symbols.

### 3.3 Strategy Run

Shows:

- selected strategy;
- config hash;
- run ID;
- checkpoint age;
- latest signals;
- candidates/order intents;
- paper/shadow/live status;
- rejection summary.

### 3.4 Account

Shows:

- balances;
- margin utilization;
- positions;
- open orders;
- recent fills;
- reconciliation status;
- account config mismatch warnings.

### 3.5 Risk And Execution

Shows:

- risk evaluations;
- reject reasons;
- active halts;
- planned orders;
- submitted orders;
- ambiguous orders requiring reconciliation;
- emergency command status.

## 4. Controlled Actions

Later write actions require:

- authenticated local operator session;
- explicit confirmation text;
- backend command record before action;
- idempotent command handler;
- audit event after completion;
- no direct browser-to-Binance calls.

Initial actions:

- global halt;
- strategy drain;
- release lease when flat;
- cancel all open orders;
- emergency flatten.

These actions should remain disabled until the corresponding backend command
specs are implemented and tested.

## 5. Technical Shape

Preferred V0:

- lightweight FastAPI or existing Python app endpoint layer for dashboard API;
- static frontend or small React/Vite app if richer state is needed;
- server-side reads from PostgreSQL repositories;
- WebSocket or polling for status updates;
- read-only by default.

The dashboard code should live under a separate `apps/operator_dashboard`
boundary or a top-level `frontend/` only after choosing the UI stack.

## 6. Error Handling

The dashboard must clearly show:

- stale data age;
- unknown account state;
- disconnected service;
- database read failure;
- active halt reason;
- unresolved order;
- mismatch between local and Binance state.

If dashboard data is stale, it should display `UNKNOWN` rather than implying
the system is safe.

## 7. Testing Strategy

Unit tests cover:

- API response shaping;
- stale status classification;
- halt and risk summary aggregation;
- permission gating for future write actions.

Frontend tests cover:

- overview renders degraded and halted states clearly;
- strategy and account pages show empty states safely;
- dangerous buttons require explicit confirmation when enabled.

Integration tests cover:

- dashboard API reads PostgreSQL fixture state;
- service health endpoints;
- no Binance credentials are exposed.

## 8. Acceptance Criteria

This phase is complete when:

1. an operator can see whether the system is safe, halted, stale, or trading;
2. market-data, strategy, account, risk, and execution states are visible in
   one place;
3. no dashboard path can bypass backend risk or execution command records;
4. read-only MVP can run locally against PostgreSQL;
5. later write actions have explicit audit and confirmation requirements.
