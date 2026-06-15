# WebSocket Capture and Raw Archive Design

Date: 2026-06-15

## 1. Status and Scope

This document defines the second backend phase for `crypto-momentum-lab`.
It extends the existing `market-data` process with Binance USD-M public
WebSocket capture, immutable raw-event archival, connection quality tracking,
and recovery behavior.

The phase subscribes only to the active monitoring universe, normally around
40 symbols. It does not build 15-second market states, strategy features,
signals, execution, account synchronization, or trading APIs.

The phase captures these five public streams:

1. aggregate trades;
2. best bid and ask updates;
3. liquidation-order snapshots;
4. one-second mark-price updates;
5. one-minute kline updates.

Raw data is retained permanently. Parquet datasets and future market states
are rebuildable derivatives and do not replace the raw archive.

## 2. Architecture Decision

The implementation uses an event-pipeline architecture inside the existing
`market-data` process:

```text
UniverseRefreshService
        |
        | monitoring-universe update
        v
SubscriptionManager
        |
        | desired subscription set
        v
BinanceWebSocketConnectionPool
        |
        | RawEnvelope
        v
BoundedEventQueue
        |
        v
CaptureCoordinator
        |
        +--> RawJsonlArchive --> DurableArchiveAcknowledgement
        |
        +--> StreamQualityTracker
```

The WebSocket receiver performs no file-system or database work. It converts
each received message into a uniform envelope and attempts to place it on a
bounded queue. The capture coordinator is the queue's sole consumer. It sends
the envelope to the archive writer and quality tracker without competing
consumers racing to remove the same event. The archive writer owns group
commit, file rotation, durability, checksums, and manifests. The quality
tracker observes captured events and connection lifecycle events.

Future aggregation may consume only archive-acknowledged events. This ensures
that a strategy decision can always be traced back to durable raw inputs.

The alternatives were rejected:

- direct callback-to-file writes couple networking, disk latency, and quality
  logic;
- PostgreSQL-first raw storage puts high-volume events in the wrong storage
  system and conflicts with the approved architecture.

## 3. Process Organization

Universe refresh, WebSocket connection management, and archive writing run in
one `market-data` process.

The components have separate failure domains:

- a universe refresh failure retains the last successful monitoring universe;
- a failed WebSocket connection degrades only its assigned streams and
  symbols;
- an archive failure halts the whole market-data pipeline;
- a PostgreSQL manifest outage may be tolerated temporarily while file
  durability remains healthy.

The process exposes two operational modes:

- `refresh-universe`, the existing one-shot command;
- `run-market-data`, the long-running command that runs the hourly universe
  scheduler and WebSocket capture together.

The existing scheduler remains the source of monitoring-universe changes.
After a successful activated universe refresh, the resulting memberships are
published directly to the subscription manager. A failed refresh does not
empty or replace the current subscription set.

## 4. Stream Scope and Binance Routes

Stream names and public WebSocket routes follow the Binance USD-M Futures
documentation effective in 2026:

| Domain stream | Binance stream | Route |
| --- | --- | --- |
| aggregate trade | `<symbol>@aggTrade` | `/market` |
| best bid/ask | `<symbol>@bookTicker` | `/public` |
| liquidation snapshot | `<symbol>@forceOrder` | `/market` |
| mark price | `<symbol>@markPrice@1s` | `/market` |
| one-minute kline | `<symbol>@kline_1m` | `/market` |

Symbols are lowercased in subscription names and normalized to uppercase in
domain metadata.

The implementation must not use the legacy route-less WebSocket endpoint that
Binance retired on April 23, 2026.

Liquidation streams are interpreted as snapshots, not complete liquidation
feeds. Binance publishes only one liquidation-order snapshot per symbol within
its documented one-second window. Research metadata and downstream schemas
must preserve this limitation.

## 5. Subscription and Connection Management

### 5.1 Grouping

Connections are grouped by route and stream type. Each connection carries at
most 100 symbol-stream subscriptions. The limit is configurable downward for
testing and operations.

The pool maintains:

- a desired subscription set;
- an acknowledged active set;
- a monotonically increasing subscription generation;
- route and stream ownership for every subscription;
- connection session IDs and lifecycle state.

### 5.2 Dynamic Changes

For each monitoring-universe update, the subscription manager computes:

```text
additions = desired - active
removals  = active - desired
```

Additions are subscribed first. Removals are sent only after the additions
have been acknowledged, reducing monitoring gaps during universe changes.

If a symbol remains monitored because of retention, an open order, or a
position, all five streams remain subscribed.

### 5.3 Control Rate

Outbound subscribe, unsubscribe, ping, and pong control messages are processed
by a rate-limited command queue. The internal operational limit is five control
messages per second, below Binance's documented maximum of ten incoming
messages per second.

Subscription requests use unique request IDs. Unknown, duplicate, or timed-out
acknowledgements generate quality events and trigger reconciliation against
the connection's desired set.

### 5.4 Lifecycle

Each connection follows:

```text
CONNECTING -> SYNCING -> LIVE -> DEGRADED -> RECONNECTING
```

A connection receives a new UUID session ID on every connect. Local event
sequence numbers restart at one within a new session.

Binance connections are proactively replaced before the documented 24-hour
maximum lifetime. Rolling replacement establishes and synchronizes the new
connection before closing the old connection.

Disconnects use bounded exponential backoff with jitter. After reconnect, the
connection sends its complete desired subscription set rather than relying on
previous exchange state.

Ping/pong and message-silence timeouts are configurable. A connection that is
technically open but silent beyond its stream-specific threshold is degraded
and reconnected.

## 6. Raw Event Envelope

Every inbound WebSocket payload is stored in a uniform envelope:

```text
schema_version
exchange
environment
route
stream
symbol
exchange_event_at
received_at
received_monotonic_ns
connection_session_id
local_sequence
exchange_sequence
subscription_generation
raw_payload
```

Field rules:

- `schema_version` identifies the envelope schema, not the exchange payload
  version;
- `exchange` is `binance-usdm`;
- `route` is `market` or `public`;
- `stream` is the normalized domain stream name;
- `symbol` is uppercase when present;
- `exchange_event_at` is nullable when Binance supplies no event timestamp;
- `received_at` is an aware UTC wall-clock timestamp;
- `received_monotonic_ns` comes from the local monotonic clock;
- `local_sequence` is strictly increasing within a connection session;
- `exchange_sequence` stores update or trade identifiers when supplied;
- `subscription_generation` identifies the desired monitoring set;
- `raw_payload` contains the unmodified decoded Binance JSON value.

The raw exchange payload is never rewritten to hide duplicates, out-of-order
events, or schema anomalies.

## 7. Queue and Backpressure

The inbound queue is bounded by event count and an approximate byte budget.
Configuration defines both limits and high-watermark alert thresholds.

If the receiver cannot enqueue an event immediately:

1. it emits an in-process queue-overflow fault;
2. the process enters `HALTED`;
3. no future derived or tradable state may be produced;
4. active connections remain open where possible;
5. recovery checks continue.

The system does not silently discard WebSocket events and does not use an
unbounded memory buffer.

Archive acknowledgement is produced only after the event's archive batch has
completed a Zstandard block flush, file-buffer flush, and `fsync`. Group commit
uses a configurable maximum event count and maximum elapsed time so it does
not perform one `fsync` per event. File identity and checksums are finalized
during rotation.

An envelope waiting in memory for its group commit is not considered durable
and may not be consumed by future aggregation. Abrupt process or host failure
may leave a bounded unacknowledged tail. Startup records the resulting session
gap; the system does not claim zero loss where the exchange offers no replay.

## 8. Archive Layout and Rotation

Raw files use compressed JSON Lines with Zstandard:

```text
data/raw/
  exchange=binance-usdm/
    date=2026-06-15/
      stream=aggTrade/
        symbol=BTCUSDT/
          hour=02/
            <session-id>-<sequence>.jsonl.zst
```

Files are partitioned by:

- exchange;
- UTC date;
- normalized stream;
- symbol;
- UTC hour.

One archive writer may keep multiple partition writers open. A configurable
upper bound prevents unbounded file descriptors. Least-recently-used
partitions are safely rotated when the bound is reached.

A file rotates when any condition is met:

- its UTC hour changes;
- its uncompressed byte estimate reaches the configured limit;
- its connection session ID changes;
- the process shuts down;
- its writer is evicted by the open-file limit.

The active filename has a temporary suffix. Finalization performs:

1. commit and acknowledge the final event batch;
2. finish the Zstandard frame;
3. flush application buffers;
4. `fsync` the file;
5. close the file;
6. calculate SHA-256 over the finalized compressed bytes;
7. atomically rename the temporary file;
8. `fsync` the containing directory;
9. persist or queue its manifest.

Raw files are append-only and are never reopened after finalization.

## 9. Archive Manifests

PostgreSQL stores one immutable manifest per finalized raw file:

```text
manifest_id
schema_version
exchange
environment
route
stream
symbol
utc_date
utc_hour
relative_path
connection_session_id
subscription_generation_min
subscription_generation_max
row_count
compressed_bytes
first_exchange_event_at
last_exchange_event_at
first_received_at
last_received_at
sha256
capture_version
recovery_status
known_gap_count
created_at
```

The relative path is resolved under a configured archive root. Absolute paths
are not stored.

Manifest insertion is idempotent by final relative path and checksum. A
conflicting checksum for the same path is a critical fault.

When PostgreSQL is unavailable, finalized manifests are written to a durable
local pending-manifest journal. The process retries them in order. If the
oldest pending manifest exceeds the configured maximum age, the service enters
`HALTED`, while raw-file recovery attempts continue.

## 10. Crash Recovery

Startup recovery runs before opening Binance connections. Initial startup
requires PostgreSQL so the process can load the active monitoring universe and
establish its operational identity. After the process is live, a temporary
PostgreSQL outage may use the bounded local journals described below.

For every temporary archive file:

1. identify its expected partition and compression format;
2. read complete recoverable JSONL records from valid Zstandard frames;
3. discard only an incomplete trailing record or incomplete frame;
4. write a new recovered finalized file;
5. calculate its checksum;
6. persist a manifest with recovery status and discarded-byte count;
7. retain the original corrupt temporary file under a recovery quarantine
   path until operator review.

Recovery never silently deletes a temporary file.

Pending manifest journals are replayed after archive recovery. A finalized
file without a PostgreSQL manifest remains discoverable and is reported as a
degraded operational condition.

## 11. Data Quality

The quality tracker consumes connection lifecycle events, subscription
acknowledgements, and raw envelopes. It writes low-volume quality records to
PostgreSQL.

Quality event categories include:

- connection opened, closed, or replaced;
- reconnect interval;
- subscribe or unsubscribe acknowledgement failure;
- unexpected stream or symbol;
- duplicate exchange identifier;
- exchange identifier gap;
- event-time regression;
- receive-time silence;
- malformed payload;
- unknown schema variant;
- queue high watermark or overflow;
- archive write, rotation, or checksum failure;
- pending-manifest backlog;
- disk-space threshold breach.

Duplicates remain in raw files. The quality event references the affected
session, local sequence, stream, symbol, and exchange identifier.

Streams without reliable exchange sequence identifiers use time regression,
silence, connection intervals, and subscription acknowledgements as gap
evidence. The system does not claim exact loss counts when Binance does not
provide the information required to prove them.

For one-minute klines, all raw updates are archived. The future derived layer
will consume only events with Binance's closed-kline flag.

## 12. Runtime State

The market-data process uses:

```text
STARTING -> SYNCING -> READY
                    |
                    v
                 DEGRADED
                    |
                    v
                  HALTED
```

Definitions:

- `STARTING`: load configuration, inspect disk, recover files and manifests,
  and connect to PostgreSQL;
- `SYNCING`: load the active monitoring universe, connect, and acknowledge
  subscriptions;
- `READY`: archive is healthy and all required connection groups are live;
- `DEGRADED`: a subset of streams or symbols has a connection, silence,
  quality, or manifest issue;
- `HALTED`: the process cannot prove durable capture and must not produce
  tradable derived state.

Global halt conditions include:

- raw archive write or finalization failure;
- queue overflow;
- archive root unavailable;
- free disk below the configured hard threshold;
- checksum conflict;
- pending-manifest age above its configured maximum;
- failure to persist or locally journal critical process-state transitions.

A partial WebSocket failure is normally `DEGRADED`, not global `HALTED`.

Recovery to `READY` means capture health has returned. It does not imply that
a future strategy is warmed up or ready to trade.

## 13. Disk Safety

The process checks archive-root free space at startup and periodically.
Configuration defines:

- warning free bytes or percentage;
- halt free bytes or percentage;
- recovery free bytes or percentage;
- check interval.

The recovery threshold is higher than the halt threshold to avoid rapid state
flapping.

At warning level, the process emits alerts. At halt level, it finalizes open
files where possible and enters `HALTED`. Raw retention remains permanent;
automatic deletion is outside this phase.

## 14. Shutdown

Graceful shutdown proceeds in this order:

1. stop accepting monitoring-universe updates;
2. stop issuing new subscribe commands;
3. request connection receivers to stop;
4. drain the bounded event queue;
5. finalize all archive files;
6. persist or journal all manifests;
7. write final quality and process-state records;
8. close WebSocket connections;
9. close PostgreSQL and file-system resources.

Shutdown has a configured deadline. Exceeding it leaves temporary files for
startup recovery rather than deleting or pretending to finalize them.

## 15. Configuration

New immutable configuration groups cover:

- Binance WebSocket market and public URLs;
- enabled streams;
- maximum subscriptions per connection;
- outbound control-message rate;
- reconnect backoff and jitter;
- proactive connection lifetime;
- ping/pong and silence thresholds;
- queue event and byte limits;
- archive group-commit event and time limits;
- archive root and Zstandard level;
- rotation byte limit and maximum open writers;
- disk warning, halt, and recovery thresholds;
- pending-manifest maximum age;
- shutdown deadline.

The behavior hash includes all values that affect captured content,
subscription layout, quality interpretation, or archive layout.

Secrets are not required for this public-data phase.

## 16. Observability

Structured logs and metrics include:

- process state;
- current monitoring generation and symbol count;
- desired and active subscription counts;
- connection count and state by route and stream;
- reconnect count and duration;
- messages and bytes received by stream;
- queue depth, byte estimate, and high-watermark duration;
- archive rows, bytes, open writers, and rotation duration;
- pending manifest count and oldest age;
- disk free bytes and percentage;
- quality-event counts by category;
- graceful-shutdown drain duration.

Critical alerts cover archive failure, queue overflow, disk halt, checksum
conflict, prolonged manifest backlog, all-connections-down, and repeated
reconnect loops.

## 17. Testing

### 17.1 Unit Tests

Unit tests cover:

- route and subscription-name generation;
- connection grouping and the 100-subscription cap;
- add-before-remove subscription differences;
- outbound control-message rate limiting;
- envelope timestamp and sequence rules;
- queue overflow state transition;
- archive path generation and rotation conditions;
- manifest calculations;
- state-machine transitions;
- duplicate, gap, regression, and silence quality rules;
- proactive connection replacement before 24 hours.

### 17.2 Integration Tests

Integration tests use a real temporary file system and PostgreSQL to verify:

- Zstandard JSONL output;
- hourly and size-based rotation;
- `fsync`, atomic rename, checksum, and manifest agreement;
- idempotent manifest insertion;
- pending-manifest journal replay;
- temporary-file recovery and quarantine;
- disk-threshold state changes;
- graceful queue draining.

### 17.3 End-to-End Tests

A local WebSocket server simulates:

- all five Binance payload types;
- subscribe and unsubscribe acknowledgements;
- dynamic monitoring-universe changes;
- disconnect and reconnect;
- duplicate and out-of-order events;
- sequence gaps and silent connections;
- delayed archive writes and queue saturation;
- rolling 24-hour connection replacement.

Tests assert that every durable archive acknowledgement maps to a recoverable
raw record. They also assert that future-consumer acknowledgements are never
emitted before the corresponding group commit and `fsync`. Crash tests verify
that any lost tail is bounded by the configured uncommitted batch and produces
an explicit session-gap quality record.

### 17.4 Live Smoke Test

A live public-data smoke test runs for at least 30 minutes without authenticated
account access. It verifies:

- all active monitoring symbols receive the configured streams where Binance
  produces events;
- archive partitions and manifests are created;
- connections remain stable or reconnect cleanly;
- queue depth remains bounded;
- no unexplained archive or checksum faults occur.

Sparse event streams, especially liquidation snapshots, are not required to
produce an event for every symbol during the smoke window. Subscription
acknowledgement and connection health prove coverage for silent streams.

## 18. Acceptance Criteria

This phase is complete only when:

1. the monitoring universe drives dynamic subscriptions without restarting the
   process;
2. all five configured streams use the correct 2026 Binance routes;
3. every durable archive acknowledgement maps to a recoverable raw record, and
   no future consumer sees an unacknowledged envelope;
4. archive files partition, rotate, checksum, and manifest correctly;
5. crash recovery preserves complete records and quarantines damaged input;
6. reconnects create new session IDs and explicit gap intervals;
7. queue overflow, disk exhaustion, and archive failure fail closed;
8. PostgreSQL manifest outages use a durable local journal and bounded grace
   period;
9. graceful shutdown drains or leaves recoverable temporary files;
10. no 15-second state or strategy-specific feature is introduced in this
    phase;
11. unit, integration, end-to-end, and 30-minute live smoke verification pass.

## 19. Deferred Work

The following work is explicitly deferred:

- normalized market-event schemas;
- 15-second closed market states;
- Parquet derivation and compaction;
- strategy-specific features and readiness;
- authenticated account streams;
- order execution and risk controls;
- operator frontend and alert delivery integrations.

The next backend phase will derive normalized events and 15-second market
states from archive-acknowledged inputs.

## 20. References

- Binance USD-M Futures WebSocket Market Streams:
  <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams>
- Binance important WebSocket route change notice:
  <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice>
- Binance liquidation-order streams:
  <https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams>
