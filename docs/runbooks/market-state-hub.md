# Market-state Hub

## Decision

The market-data process owns the Binance market WebSocket connections. It
publishes closed `MarketState15s` batches through an internal WebSocket Hub.
The live strategy subscribes to the Hub instead of polling
`runtime_market_states_15s`.

PostgreSQL remains the durable adapter for audit, checkpoint, startup warmup,
and explicit recovery mode. It is no longer on the normal live decision path.

```text
Binance market WS
        |
        v
market-data -- normalize/aggregate --> MarketStateHub
        |                                  |
        |                                  +--> live strategy (latest batches)
        +--> PostgreSQL (durable sink)     +--> future paper subscribers

Binance account WS --> execution-account --> AccountEventHub --> live exit/reconcile lane
                                      |
                                      +--> PostgreSQL account/order snapshots
REST               --> startup/periodic reconciliation
```

## Interface invariants

- The Hub is one internal fan-out point; strategy processes do not open their
  own Binance market connections.
- Each subscriber has a bounded queue. If a live subscriber falls behind, its
  old queued batch is replaced by the newest batch and the sequence gap is
  detected by the client.
- The live market-state client drains the WebSocket in a dedicated reader
  task. Its small bounded receive queue never applies strategy or database
  backpressure to the socket: on overflow it records
  `market_state_hub_client_queue_overflow`, advances to the newest sequence,
  reconnects without replaying the stale backlog, and keeps entries disabled
  until the first new batch has been processed.
- Account events and realtime quotes use the same reader/processor split.
  Account-event overflow is logged and forces a REST reconciliation; quote
  delivery keeps the latest pending value per symbol. Both are acceleration
  paths with PostgreSQL reconciliation as the recovery adapter.
- The Hub keeps a bounded per-environment replay window. Reconnects carry the
  last stream epoch and sequence, and receive contiguous missing batches when
  they are still buffered.
- A stream epoch change or a sequence gap disables live entries until the
  stream is caught up. If the requested sequence is outside the replay window,
  the client fails closed rather than silently continuing with incomplete
  strategy state.
- A market-state consumer-lag reset clears each strategy symbol's rolling
  indicators on its first post-gap state. This prevents a bounded latest-state
  recovery from continuing with a partially missing warmup window.
- PostgreSQL remains the durable recovery adapter. The current increment makes
  the realtime replay boundary explicit; durable gap replay is a separate
  recovery step and must not be replaced with blind latest-state processing.
- Live startup warms strategy state from a bounded PostgreSQL history window,
  then starts consuming only newly published Hub batches. This avoids applying
  the warmup state twice.
- PostgreSQL persistence is best effort relative to the realtime fan-out: a
  slow database write cannot delay publishing a closed batch to live.
- The account WebSocket reader never waits for PostgreSQL. The ordered account
  processor applies each event to the in-memory snapshot, while a separate
  ordered worker persists the immutable snapshot. The live Hub notification is
  emitted after that durable snapshot is visible, so the live account lane can
  invalidate its context, reconcile the matching order, and evaluate
  reduce-only exits without reading a stale account projection.
- The account persistence queue is bounded. A queue overflow, processor
  failure, or persistence failure disables account-event acceptance, requests a
  stream reconnect, drains/skips the uncommitted backlog, and performs an
  authoritative REST reconciliation before accepting events again. PostgreSQL
  is not in the WebSocket receive path, but remains the durable boundary for
  live account-event fan-out and the recovery sink.
- The account lane never calls the entry strategy. It can only reconcile order
  state and submit reduce-only exit candidates.

## Deployment

The server compose file starts the market Hub inside `market-data` on the
internal `market-data:8766` address and the account-event Hub inside
`execution-account-live` on `execution-account-live:8767`. `live-strategy`
uses `--market-state-source hub`, `--market-state-hub-url
ws://market-data:8766`, and `--account-event-hub-url
ws://execution-account-live:8767`.

The old database consumer remains available only through the explicit
`--market-state-source postgres` option for recovery and diagnostics.

## Remaining hardening

The next optional seam is a typed risk-event stream for operator halts and
emergency reduce-only controls. It can reuse the account-event transport
pattern without putting PostgreSQL back on the normal decision path.

## Live execution latency path

The live strategy keeps the Hub topology but routes order commands through an
in-process execution coordinator. The coordinator serializes only one
`account + symbol + position_side` key at a time. Reduce-only submits and
cancels have priority over entry submits; reconciliation is lowest priority
and cannot hold an unrelated symbol's order command.

Before a live exchange request, the order journal writes the approved intent,
planned order, and `SUBMITTING` event in one PostgreSQL transaction. The
exchange call happens only after that transaction commits. Deterministic client
order IDs and the existing reconciliation path remain the recovery mechanism
for a crash between the journal commit and the Binance response.

The market consumer still evaluates strategy states in order. It uses a
bounded context prefetch queue to overlap the next state's PostgreSQL context
read with current strategy and exchange work. A context fetched before an
account/order invalidation is discarded by a generation check and reloaded
before it can authorize an order.

Latency must be measured with the runtime phases
`candidate_accepted -> intent_saved -> submitting ->
exchange_request_started -> exchange_response_received -> account_fill`.
The bucket start is a strategy timestamp, not the wall-clock time at which the
candidate object was created.
