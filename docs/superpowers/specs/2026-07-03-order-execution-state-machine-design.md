# Order Execution State Machine Design

Date: 2026-07-03

## 1. Status And Scope

This document defines the first phase that may submit real orders to Binance
USD-M Futures. It depends on read-only account synchronization and risk gateway
approval.

This phase includes:

- durable order-intent claim and execution workflow;
- exchange order records;
- deterministic Binance client order IDs;
- idempotent submit and reconciliation behavior;
- timeout and ambiguous-result handling;
- partial fill, reject, cancel, expire, and terminal state handling;
- reduce-only and controlled exit order support;
- emergency cancel-all and flatten command plumbing, behind explicit gates.

This phase excludes:

- strategy signal research;
- risk policy definition beyond consuming risk approvals;
- frontend controls beyond backend command records;
- multi-account operation;
- unsupported position modes.

## 2. Design Position

The strategy produces an order intent. The risk gateway approves or rejects it.
Only then can `execution-account` claim and execute it:

```text
order_intent -> risk_evaluation APPROVED
-> execution claim -> quantized exchange order
-> Binance submit/query -> local order state
-> fills/reconciliation -> execution update
```

The core invariant is no duplicate order from retry. Every exchange order uses
a deterministic client order ID. If a submit call times out or returns an
ambiguous error, the state machine queries Binance by client order ID before
any retry.

## 3. Durable Tables

Add or extend tables:

- `order_intents`;
- `order_intent_claims`;
- `exchange_orders`;
- `exchange_order_events`;
- `exchange_fills`;
- `execution_commands`;
- `execution_reconciliation_events`.

`order_intents` store strategy output before risk approval. `exchange_orders`
store the order after quantization and submission planning.

`exchange_orders` fields include:

- `exchange_order_id`;
- `client_order_id`;
- `intent_id`;
- `run_id`;
- `strategy_name`;
- `symbol`;
- `side`;
- `order_type`;
- `quantity`;
- `price`;
- `reduce_only`;
- `time_in_force`;
- `status`;
- `submitted_at`;
- `last_exchange_update_at`;
- `terminal_at`;
- `raw_payload`.

## 4. State Machine

Order execution states:

```text
INTENT_APPROVED
-> CLAIMED
-> PLANNED
-> SUBMITTING
-> SUBMITTED
-> ACKNOWLEDGED
-> PARTIALLY_FILLED
-> FILLED
-> CANCELED
-> REJECTED
-> EXPIRED
-> UNKNOWN_PENDING_RECONCILIATION
```

Terminal states:

- `FILLED`;
- `CANCELED`;
- `REJECTED`;
- `EXPIRED`.

`UNKNOWN_PENDING_RECONCILIATION` is not terminal. It blocks new exposure for
that symbol until resolved.

## 5. Claiming And Fencing

Execution claims use PostgreSQL transactions and row locks:

1. select approved unclaimed intents;
2. verify active lease fencing token;
3. create claim row;
4. create planned order with deterministic client order ID;
5. commit before network submission.

If the process crashes after planning but before submission, restart inspects
the planned order and either submits it or marks it abandoned according to
state and exchange query evidence.

## 6. Quantization

Before submission, the state machine applies current exchange metadata:

- tick size;
- step size;
- min quantity;
- min notional;
- order type support;
- reduce-only support;
- price protection rules where applicable.

If quantization changes the intended notional beyond configured tolerance, the
order is rejected with a durable reason rather than silently resized.

## 7. Submission And Ambiguity

Submission outcomes:

- clear success: persist exchange acknowledgement;
- clear reject: persist reject with reason;
- timeout/network error/unknown response: query by client order ID;
- rate limit: backoff within risk-approved expiration window;
- account mode or permission error: global halt.

Retry is allowed only after query proves no order exists for the deterministic
client order ID and the intent is still valid.

## 8. Fills And Execution Updates

Fills arrive from user-data stream and REST reconciliation. The state machine:

- deduplicates fills by exchange fill/trade ID;
- updates position and order state;
- calculates fee and realized execution cost;
- emits execution updates for the strategy checkpoint;
- persists terminal state when filled, canceled, rejected, or expired.

Partial fills remain open until Binance reports terminal state or the order is
canceled/resolved.

## 9. Reduce-Only And Emergency Commands

Risk-reducing actions are separate command records, not strategy entry
intents. Commands include:

- cancel open order;
- cancel all for symbol;
- cancel all for account;
- reduce-only close symbol position;
- emergency flatten account.

These commands require explicit backend authorization and audited records.
They remain possible when normal entries are halted.

## 10. Error Handling

The process enters halt or symbol block when:

- an order remains ambiguous after configured reconciliation attempts;
- Binance reports an unknown order state;
- user-data stream is lost and REST reconciliation fails;
- repeated order rejects exceed threshold;
- local and Binance positions disagree;
- database persistence fails after exchange acknowledgement.

The last case is critical. The process must halt and reconcile from Binance
before accepting new intents.

## 11. Testing Strategy

Unit tests cover:

- deterministic client order ID generation;
- state transitions;
- duplicate intent claim prevention;
- quantization and min-notional rejection;
- ambiguous timeout query-before-retry;
- partial fill to terminal fill;
- reduce-only command planning.

Integration tests cover:

- PostgreSQL claim locking with concurrent workers;
- idempotent order event persistence;
- restart from planned, submitted, and unknown states;
- reconciliation resolving local/exchange mismatch.

Live sandbox or manual-gated tests cover:

- submit disabled by default;
- one tiny reduce-only-safe test order only when explicitly enabled;
- timeout simulation with fake Binance client before any real test order.

## 12. Acceptance Criteria

This phase is complete when:

1. approved intents can be claimed and converted into deterministic exchange
   order plans;
2. real submission is behind explicit live enablement;
3. ambiguous submission never creates duplicate orders;
4. fills and terminal states reconcile from Binance evidence;
5. unresolved order uncertainty blocks new exposure;
6. reduce-only and emergency commands are represented as audited backend
   commands;
7. unit, integration, fake-exchange e2e, ruff, mypy, and manual-gated live
   checks pass.
