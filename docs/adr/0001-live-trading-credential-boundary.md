# ADR-0001: Separate read and trade credentials for live services

- Status: Proposed
- Date: 2026-09-04
- Context: [`project architecture design`](../superpowers/specs/2026-06-14-project-architecture-design.md)

## Context

The live deployment currently gives `live-strategy` and
`execution-account-live` the same `BINANCE_API_KEY`/`BINANCE_API_SECRET`
pair.  The two processes have different responsibilities:

- `execution-account-live` owns the private user-data stream and REST
  reconciliation needed to observe account state.
- `live-strategy` owns order submission and therefore requires write access.

Sharing the pair makes a compromise or configuration error in the strategy
process equivalent to a compromise of the account-observation process.  It
also makes it difficult to prove from configuration which component may place
orders.

## Decision

Introduce two explicit credential roles in configuration and deployment:

| Role | Environment variables | Intended permissions |
| --- | --- | --- |
| Read | `BINANCE_READ_API_KEY`, `BINANCE_READ_API_SECRET` | User-data stream, account/order reads, reconciliation |
| Trade | `BINANCE_TRADE_API_KEY`, `BINANCE_TRADE_API_SECRET` | Read permissions plus order submission/cancel operations |

The application boundary must select credentials by role rather than by a
generic global key.  A service may still use a single key temporarily during
migration, but that fallback is an explicitly named compatibility mode and is
not the target deployment contract.

The trade key must be restricted to the smallest Binance permission set that
supports the current execution client.  Withdrawal permission is not part of
either role.  The read key must not have order-trading permission.

## Migration

1. Add role-specific configuration fields and startup validation without
   changing the current defaults.
2. Provision and verify a read-only key against the account service in a
   non-production or shadow deployment.
3. Provision a trade key for `live-strategy`; keep the execution-account
   service on the read key.
4. Roll out one service at a time and verify private-stream health,
   reconciliation, and order submission before removing the compatibility
   fallback.
5. Record the key role and a non-secret key fingerprint in startup metadata;
   never log the key or secret itself.

## Rollback

If the role-specific rollout causes account-stream or order-submission
failures, restore the previous environment mapping in deployment configuration
and restart only the affected service.  Do not copy secrets into source files
or telemetry.  The compatibility mapping may remain available until the
read-only stream and trade path have passed the rollout checklist.

## Consequences

Positive:

- A read-only account process no longer needs order-trading permission.
- Configuration states the write boundary explicitly and can be audited.
- Key rotation and incident containment can be performed per service.

Costs and follow-up:

- Secret management and deployment configuration must carry two pairs.
- Startup diagnostics and run metadata need a safe credential-role marker.
- The Binance permission model and the exact REST methods used by each client
  must be verified before removing the compatibility fallback.

## Rejected alternatives

- **Continue sharing one key:** keeps the current least-privilege violation.
- **Move all private operations into one process:** increases coupling and
  expands the blast radius of that process.
- **Use a proxy as the first step:** adds another availability and failure
  boundary before the simpler credential split is measured.
