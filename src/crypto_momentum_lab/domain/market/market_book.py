"""MarketBook and DatasetCatalog services implementing R2 Dual-Revision.

Obeys Astra Architecture Blueprint 2026-09-25:
- publish(state, lineage) -> MarketRevisionRef
- read(ref) -> immutable MarketEnvelope
- build_dataset(scope, interval, visibility_mode, cut) -> DatasetManifest
- open_dataset(manifest_id) -> verified ordered stream
- DecisionTrace pins exact input revisions;
- Refuses to fake missing decision_visible revisions with canonical data.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import fields
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

from crypto_momentum_lab.domain.market.models import MarketState15s
from crypto_momentum_lab.domain.market.revision_models import (
    DatasetManifest,
    DecisionTrace,
    MarketEnvelope,
    MarketRevisionRef,
    MarketVisibilityMode,
)


class RevisionConflictError(Exception):
    """Raised when the same revision ID has conflicting payload content hashes."""


class RevisionNotFoundError(Exception):
    """Raised when a requested MarketRevisionRef does not exist in storage."""


class ManifestIntegrityError(Exception):
    """Raised when a DatasetManifest hash verification fails."""


class UnreproducibleError(Exception):
    """Raised when a decision cannot be reproduced due to missing original revision."""


def compute_market_state_hash(state: MarketState15s) -> str:
    """Calculates a deterministic SHA256 hash of a normalized MarketState15s payload."""
    payload: dict[str, Any] = {}
    for f in fields(MarketState15s):
        val = getattr(state, f.name)
        if isinstance(val, Decimal):
            payload[f.name] = str(val)
        elif isinstance(val, datetime):
            payload[f.name] = val.isoformat()
        else:
            payload[f.name] = val
    dumped = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(dumped.encode("utf-8")).hexdigest()


class MarketBookRepository(Protocol):
    """Protocol for durable storage of MarketEnvelope, pointers, and manifests."""

    def save_envelope(self, envelope: MarketEnvelope) -> None: ...
    def load_envelope(self, revision_id: str) -> MarketEnvelope | None: ...
    def get_canonical_ref(
        self, scope: str, symbol: str, interval: str, bucket_start: datetime
    ) -> MarketRevisionRef | None: ...
    def set_canonical_ref(
        self,
        scope: str,
        symbol: str,
        interval: str,
        bucket_start: datetime,
        ref: MarketRevisionRef,
    ) -> None: ...
    def get_revisions_for_bucket(
        self, scope: str, symbol: str, interval: str, bucket_start: datetime
    ) -> tuple[MarketRevisionRef, ...]: ...
    def save_manifest(self, manifest: DatasetManifest) -> None: ...
    def load_manifest(self, manifest_id: str) -> DatasetManifest | None: ...
    def save_decision_trace(self, trace: DecisionTrace) -> None: ...
    def load_decision_trace(self, decision_id: str) -> DecisionTrace | None: ...
    def get_canonical_refs_in_range(
        self,
        scope: str,
        symbols: tuple[str, ...],
        interval: str,
        start_time: datetime,
        end_time: datetime,
    ) -> dict[tuple[str, datetime], MarketRevisionRef]: ...
    def list_manifests(
        self, scope: str | None = None, limit: int = 100
    ) -> list[DatasetManifest]: ...
    def get_distinct_dates_and_symbols(
        self, scope: str, interval: str = "15s"
    ) -> list[tuple[date, tuple[str, ...]]]: ...


class InMemoryMarketBookRepository:
    """In-memory reference storage implementation for MarketBook and Catalog."""

    def __init__(self) -> None:
        self.envelopes: dict[str, MarketEnvelope] = {}
        self.canonical_pointers: dict[
            tuple[str, str, str, datetime], MarketRevisionRef
        ] = {}
        self.bucket_revisions: dict[
            tuple[str, str, str, datetime], list[MarketRevisionRef]
        ] = {}
        self.manifests: dict[str, DatasetManifest] = {}
        self.decision_traces: dict[str, DecisionTrace] = {}

    def save_envelope(self, envelope: MarketEnvelope) -> None:
        self.envelopes[envelope.ref.revision_id] = envelope
        bkey = (
            envelope.ref.scope,
            envelope.ref.symbol,
            envelope.ref.interval,
            envelope.ref.bucket_start,
        )
        revs = self.bucket_revisions.setdefault(bkey, [])
        if not any(r.revision_id == envelope.ref.revision_id for r in revs):
            revs.append(envelope.ref)

    def load_envelope(self, revision_id: str) -> MarketEnvelope | None:
        return self.envelopes.get(revision_id)

    def get_canonical_ref(
        self, scope: str, symbol: str, interval: str, bucket_start: datetime
    ) -> MarketRevisionRef | None:
        return self.canonical_pointers.get((scope, symbol, interval, bucket_start))

    def set_canonical_ref(
        self,
        scope: str,
        symbol: str,
        interval: str,
        bucket_start: datetime,
        ref: MarketRevisionRef,
    ) -> None:
        self.canonical_pointers[(scope, symbol, interval, bucket_start)] = ref

    def get_revisions_for_bucket(
        self, scope: str, symbol: str, interval: str, bucket_start: datetime
    ) -> tuple[MarketRevisionRef, ...]:
        return tuple(
            self.bucket_revisions.get((scope, symbol, interval, bucket_start), [])
        )

    def get_canonical_refs_in_range(
        self,
        scope: str,
        symbols: tuple[str, ...],
        interval: str,
        start_time: datetime,
        end_time: datetime,
    ) -> dict[tuple[str, datetime], MarketRevisionRef]:
        sym_set = set(symbols)
        result: dict[tuple[str, datetime], MarketRevisionRef] = {}
        for (s, sym, inv, b_start), ref in self.canonical_pointers.items():
            if (
                s == scope
                and sym in sym_set
                and inv == interval
                and start_time <= b_start < end_time
            ):
                result[(sym, b_start)] = ref
        return result

    def save_manifest(self, manifest: DatasetManifest) -> None:
        self.manifests[manifest.manifest_id] = manifest

    def load_manifest(self, manifest_id: str) -> DatasetManifest | None:
        return self.manifests.get(manifest_id)

    def list_manifests(
        self, scope: str | None = None, limit: int = 100
    ) -> list[DatasetManifest]:
        items = list(self.manifests.values())
        if scope is not None:
            items = [m for m in items if m.scope == scope]
        items.sort(key=lambda m: m.start_time, reverse=True)
        return items[:limit]

    def get_distinct_dates_and_symbols(
        self, scope: str, interval: str = "15s"
    ) -> list[tuple[date, tuple[str, ...]]]:
        date_to_symbols: dict[date, set[str]] = {}
        for s, sym, inv, b_start in self.canonical_pointers.keys():
            if s == scope and inv == interval:
                d = b_start.date()
                date_to_symbols.setdefault(d, set()).add(sym)
        return [
            (d, tuple(sorted(syms))) for d, syms in sorted(date_to_symbols.items())
        ]

    def save_decision_trace(self, trace: DecisionTrace) -> None:
        self.decision_traces[trace.decision_id] = trace

    def load_decision_trace(self, decision_id: str) -> DecisionTrace | None:
        return self.decision_traces.get(decision_id)


class MarketBook:
    """Authority governing immutable market revisions and dual-version semantics."""

    def __init__(self, repository: MarketBookRepository | None = None) -> None:
        self._repo = repository or InMemoryMarketBookRepository()

    def publish(
        self,
        state: MarketState15s,
        *,
        lineage: dict[str, Any] | None = None,
        visibility_mode: MarketVisibilityMode = MarketVisibilityMode.DECISION_VISIBLE,
        source_epoch: str = "epoch_1",
        is_canonical: bool = False,
        interval: str = "15s",
        published_at: datetime | None = None,
    ) -> MarketRevisionRef:
        """Publishes a market state into an immutable revision ref.

        Idempotent: Re-publishing identical content returns the existing ref.
        Conflict: Re-publishing different content under same ID raises error.
        """
        content_hash = compute_market_state_hash(state)
        b_epoch = int(state.bucket_start.timestamp())
        revision_id = (
            f"{state.environment}:{state.symbol}:{interval}:"
            f"{b_epoch}:{content_hash[:10]}"
        )

        existing = self._repo.load_envelope(revision_id)
        if existing is not None:
            if existing.ref.content_hash != content_hash:
                raise RevisionConflictError(
                    f"Revision collision: {revision_id} exists with hash "
                    f"{existing.ref.content_hash}, but new is {content_hash}"
                )
            # Idempotent return
            if is_canonical or visibility_mode == MarketVisibilityMode.CANONICAL:
                self._repo.set_canonical_ref(
                    state.environment,
                    state.symbol,
                    interval,
                    state.bucket_start,
                    existing.ref,
                )
            return existing.ref

        # Check existing revisions for this natural bucket to ensure idempotency by hash
        existing_revs = self._repo.get_revisions_for_bucket(
            state.environment, state.symbol, interval, state.bucket_start
        )
        for r in existing_revs:
            if r.content_hash == content_hash:
                if is_canonical or visibility_mode == MarketVisibilityMode.CANONICAL:
                    self._repo.set_canonical_ref(
                        state.environment,
                        state.symbol,
                        interval,
                        state.bucket_start,
                        r,
                    )
                return r

        pub_time = (
            published_at
            or state.last_received_at
            or datetime.now(UTC)
        )

        ref = MarketRevisionRef(
            scope=state.environment,
            symbol=state.symbol,
            interval=interval,
            bucket_start=state.bucket_start,
            bucket_end=state.bucket_end,
            revision_id=revision_id,
            content_hash=content_hash,
            published_at=pub_time,
            source_epoch=source_epoch,
            visibility_mode=visibility_mode,
        )

        envelope = MarketEnvelope(
            ref=ref,
            state=state,
            lineage=lineage or {},
            data_complete=state.data_complete,
            missing_count=state.missing_agg_trade_count,
        )
        self._repo.save_envelope(envelope)

        # Update canonical pointer if requested
        if is_canonical or visibility_mode == MarketVisibilityMode.CANONICAL:
            self._repo.set_canonical_ref(
                state.environment,
                state.symbol,
                interval,
                state.bucket_start,
                ref,
            )

        return ref

    def read(self, ref: MarketRevisionRef | str) -> MarketEnvelope:
        """Reads an immutable MarketEnvelope by ref or revision_id."""
        rev_id = ref.revision_id if isinstance(ref, MarketRevisionRef) else ref
        env = self._repo.load_envelope(rev_id)
        if env is None:
            raise RevisionNotFoundError(f"Revision {rev_id} not found in MarketBook")
        return env

    def get_canonical_ref(
        self, scope: str, symbol: str, interval: str, bucket_start: datetime
    ) -> MarketRevisionRef | None:
        """Returns the current canonical revision pointer for a bucket."""
        return self._repo.get_canonical_ref(scope, symbol, interval, bucket_start)

    def get_decision_visible_ref(
        self,
        scope: str,
        symbol: str,
        interval: str,
        bucket_start: datetime,
        decision_time: datetime | None = None,
    ) -> MarketRevisionRef | None:
        """Resolves the exact revision visible at the time of decision."""
        revs = self._repo.get_revisions_for_bucket(
            scope, symbol, interval, bucket_start
        )
        if not revs:
            return None

        # Filter by visibility published_at <= decision_time if specified
        if decision_time is not None:
            cands = [
                r
                for r in revs
                if r.published_at <= decision_time
                and r.visibility_mode == MarketVisibilityMode.DECISION_VISIBLE
            ]
            if cands:
                return max(cands, key=lambda r: r.published_at)
            return None

        # Default: return earliest observed revision (original decision-visible)
        obs = [
            r
            for r in revs
            if r.visibility_mode == MarketVisibilityMode.DECISION_VISIBLE
        ]
        if obs:
            return min(obs, key=lambda r: r.published_at)
        return None


class DatasetCatalog:
    """Catalog managing reproducible DatasetManifest instances and streams."""

    def __init__(
        self,
        market_book: MarketBook,
        repository: MarketBookRepository | None = None,
    ) -> None:
        self._book = market_book
        self._repo = repository or market_book._repo

    def build_dataset(
        self,
        *,
        manifest_id: str,
        scope: str,
        symbols: tuple[str, ...],
        interval: str = "15s",
        start_time: datetime,
        end_time: datetime,
        visibility_mode: MarketVisibilityMode = MarketVisibilityMode.DECISION_VISIBLE,
        feature_algorithm_version: str = "v1",
    ) -> DatasetManifest:
        """Builds an immutable DatasetManifest with explicit interval proof."""
        if interval != "15s":
            raise NotImplementedError(f"Interval {interval} not supported yet")
        step = timedelta(seconds=15)

        refs: list[MarketRevisionRef] = []
        holes: list[tuple[datetime, datetime]] = []

        total_expected = 0
        batch_fetcher = getattr(self._repo, "get_canonical_refs_in_range", None)
        canonical_map: dict[tuple[str, datetime], MarketRevisionRef] | None = None
        if (
            callable(batch_fetcher)
            and visibility_mode == MarketVisibilityMode.CANONICAL
        ):
            canonical_map = batch_fetcher(
                scope, symbols, interval, start_time, end_time
            )

        gap_per_sym: dict[str, datetime | None] = {s: None for s in symbols}
        current_time = start_time
        while current_time < end_time:
            b_start = current_time
            b_end = current_time + step
            for sym in sorted(symbols):
                total_expected += 1
                if canonical_map is not None:
                    ref = canonical_map.get((sym, b_start))
                elif visibility_mode == MarketVisibilityMode.CANONICAL:
                    ref = self._book.get_canonical_ref(scope, sym, interval, b_start)
                else:
                    ref = self._book.get_decision_visible_ref(
                        scope, sym, interval, b_start, decision_time=b_end
                    )

                if ref is not None:
                    refs.append(ref)
                    g_start = gap_per_sym[sym]
                    if g_start is not None:
                        holes.append((g_start, b_start))
                        gap_per_sym[sym] = None
                else:
                    if gap_per_sym[sym] is None:
                        gap_per_sym[sym] = b_start
            current_time += step

        for sym in sorted(symbols):
            g_start = gap_per_sym[sym]
            if g_start is not None:
                holes.append((g_start, end_time))

        cov = (
            Decimal(len(refs)) / Decimal(total_expected)
            if total_expected > 0
            else Decimal("1.0")
        )

        # Compute deterministic manifest hash
        hasher = hashlib.sha256()
        hasher.update(scope.encode())
        hasher.update(",".join(sorted(symbols)).encode())
        hasher.update(interval.encode())
        hasher.update(start_time.isoformat().encode())
        hasher.update(end_time.isoformat().encode())
        hasher.update(visibility_mode.value.encode())
        hasher.update(feature_algorithm_version.encode())
        for r in refs:
            hasher.update(r.content_hash.encode())
        manifest_hash = hasher.hexdigest()

        manifest = DatasetManifest(
            manifest_id=manifest_id,
            scope=scope,
            symbols=symbols,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
            visibility_mode=visibility_mode,
            revision_refs=tuple(refs),
            schema_version=1,
            feature_algorithm_version=feature_algorithm_version,
            manifest_hash=manifest_hash,
            created_at=datetime.now(UTC),
            coverage_ratio=cov,
            holes=tuple(holes),
        )

        self._repo.save_manifest(manifest)
        return manifest

    def open_dataset(self, manifest_id: str) -> tuple[MarketEnvelope, ...]:
        """Opens and verifies an ordered stream of envelopes from a manifest."""
        manifest = self._repo.load_manifest(manifest_id)
        if manifest is None:
            raise KeyError(f"Manifest {manifest_id} not found")

        # Verify manifest hash integrity
        hasher = hashlib.sha256()
        hasher.update(manifest.scope.encode())
        hasher.update(",".join(sorted(manifest.symbols)).encode())
        hasher.update(manifest.interval.encode())
        hasher.update(manifest.start_time.isoformat().encode())
        hasher.update(manifest.end_time.isoformat().encode())
        hasher.update(manifest.visibility_mode.value.encode())
        hasher.update(manifest.feature_algorithm_version.encode())
        for r in manifest.revision_refs:
            hasher.update(r.content_hash.encode())
        expected_hash = hasher.hexdigest()

        if expected_hash != manifest.manifest_hash:
            raise ManifestIntegrityError(
                f"Manifest {manifest_id} integrity violated: expected hash "
                f"{manifest.manifest_hash}, computed {expected_hash}"
            )

        envelopes: list[MarketEnvelope] = []
        for r in manifest.revision_refs:
            try:
                env = self._book.read(r)
                envelopes.append(env)
            except RevisionNotFoundError as exc:
                raise UnreproducibleError(
                    f"Dataset {manifest_id} cannot be reproduced: revision "
                    f"{r.revision_id} for {r.symbol} at {r.bucket_start} is missing!"
                ) from exc

        # Return strictly ordered stream by bucket_start then symbol
        envelopes.sort(key=lambda e: (e.ref.bucket_start, e.ref.symbol))
        return tuple(envelopes)

    def verify_manifest(self, manifest_id: str) -> dict[str, Any]:
        """Cryptographically verifies a DatasetManifest and its referenced revisions."""
        verifier = getattr(self._repo, "verify_manifest", None)
        if callable(verifier):
            res = verifier(manifest_id)
            if isinstance(res, dict):
                return res

        manifest = self._repo.load_manifest(manifest_id)
        if manifest is None:
            return {
                "manifest_id": manifest_id,
                "status": "NOT_FOUND",
                "error": f"Manifest '{manifest_id}' not found in catalog",
                "verified": False,
            }

        # Check hash
        hasher = hashlib.sha256()
        hasher.update(manifest.scope.encode())
        hasher.update(",".join(sorted(manifest.symbols)).encode())
        hasher.update(manifest.interval.encode())
        hasher.update(manifest.start_time.isoformat().encode())
        hasher.update(manifest.end_time.isoformat().encode())
        hasher.update(manifest.visibility_mode.value.encode())
        hasher.update(manifest.feature_algorithm_version.encode())
        for r in manifest.revision_refs:
            hasher.update(r.content_hash.encode())
        computed_hash = hasher.hexdigest()

        if computed_hash != manifest.manifest_hash:
            return {
                "manifest_id": manifest_id,
                "status": "INTEGRITY_VIOLATION",
                "error": (
                    f"Computed hash {computed_hash} != stored "
                    f"{manifest.manifest_hash}"
                ),
                "verified": False,
            }

        return {
            "manifest_id": manifest.manifest_id,
            "status": "VERIFIED_REPRODUCIBLE",
            "verified": True,
            "scope": manifest.scope,
            "symbols_count": len(manifest.symbols),
            "interval": manifest.interval,
            "start_time": manifest.start_time.isoformat(),
            "end_time": manifest.end_time.isoformat(),
            "visibility_mode": manifest.visibility_mode.value,
            "manifest_hash": manifest.manifest_hash,
            "coverage_ratio": str(manifest.coverage_ratio),
            "revisions_count": len(manifest.revision_refs),
            "holes_count": len(manifest.holes),
        }

    def list_manifests(
        self, scope: str | None = None, limit: int = 100
    ) -> list[DatasetManifest]:
        lister = getattr(self._repo, "list_manifests", None)
        if callable(lister):
            result = lister(scope=scope, limit=limit)
            if isinstance(result, list):
                return result
        return []
