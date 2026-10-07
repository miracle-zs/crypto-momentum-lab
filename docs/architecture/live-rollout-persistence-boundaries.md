# Live rollout persistence-boundary audit

This audit distinguishes a PostgreSQL adapter from a live-trading workflow.
The target is a one-way dependency from live application code to domain ports,
not an artificial count of zero PostgreSQL imports in the composition root.

| Location | Observed responsibility | Decision |
| --- | --- | --- |
| `persistence.postgres.live_runtime_assembly` | Creates engines, session factories, and concrete repositories. | Moved here from `live_rollout.database_assembly`; it is infrastructure assembly. |
| `live_rollout.runtime_orchestrator` | Composition root: selects concrete exchange/database adapters and owns their lifetime. | Retain direct construction where the component has no runtime behaviour beyond composition. Do not insert a pass-through port just to hide imports. |
| `live_rollout.execution_runtime` | Rebuilds the execution book before exposing a coordinator. | Retain for now. Its persistence construction is coupled to recovery ordering; extract a dedicated execution-book factory only when a second runtime needs it. |
| `live_rollout.exit_receipt_recovery` | Fail-closed recovery: combines durable receipt evidence with a fresh exchange account cut. | Retain for now. It is an application safety workflow, not a repository. A future move requires a narrow receipt-evidence reader port, preserving the exchange cut invariant. |
| `live_rollout.missing_order_resolution` | Operator command that verifies exchange absence before appending durable evidence. | Retain for now. The exchange verification and terminal-event rules belong together; a repository extraction must not gain write authority beyond the existing append operations. |
| `live_rollout.postgres_runtime` | Builds a cached live context and evaluates cross-store freshness/currentness. | Do not move wholesale. Split query adapters only after the context cache and currentness rules have explicit ports; moving the whole module would place live workflow policy in persistence. |
| `live_rollout.market_assembly` | Selects a market-state source and starts runtime transports. | Retain as runtime assembly. It is not a persistence adapter even when a PostgreSQL source is one option. |

The next extraction criterion is behavioural: a component can move into
`persistence.postgres` only when it can be described as translating one
persistence port without owning trading decisions, exchange verification, or
runtime lifecycle ordering.
