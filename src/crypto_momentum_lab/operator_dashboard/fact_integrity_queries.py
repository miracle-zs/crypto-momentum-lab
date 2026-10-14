"""Read verified ledger coverage, independently of process/reconciliation flags."""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, and_, case, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from crypto_momentum_lab.domain.execution.evidence_codec import (
    recovery_checkpoint_head_binding,
)
from crypto_momentum_lab.domain.execution.recovery_codec import PositionRecoveryCodec
from crypto_momentum_lab.domain.execution.recovery_models import RecoverySchemaError
from crypto_momentum_lab.operator_dashboard.fact_proof import (
    INVALID,
    PROVEN,
    REASON_ACTIVE_POSITION_COUNT_MISMATCH,
    REASON_CHECKPOINT_MISSING,
    REASON_CHECKPOINT_ROW_MISSING,
    REASON_NO_RELEVANT_HEAD,
    HeadFacts,
    RecoveryCut,
    SuffixFacts,
    evaluate_head_proof,
)
from crypto_momentum_lab.persistence.postgres.execution_unit_of_work_models import (
    ExecutionBookHeadRow,
)
from crypto_momentum_lab.persistence.postgres.models import AccountReconciliationHeadRow
from crypto_momentum_lab.persistence.postgres.position_fact_journal_models import (
    PositionFactJournalEventRow,
    PositionRecoveryCheckpointRow,
)
from crypto_momentum_lab.persistence.postgres.recovery_relevance import (
    relevant_recovery_head,
)


@dataclass(frozen=True, slots=True)
class FactIntegritySummary:
    """Current-risk proof for one account plus the retained historical proof.

    ``gaps``/``observed_at`` describe only what still carries current risk.
    ``reasons``/``unknown_scopes`` name the missing evidence and where, so an
    UNKNOWN result is actionable instead of a bare flag. ``historical_*`` keeps
    the same verdict for closed, fully resolved objects without letting their
    staleness mark the live account unverified.
    """

    gaps: int | None
    observed_at: datetime | None
    reasons: tuple[str, ...] = ()
    unknown_scopes: tuple[str, ...] = ()
    historical_reasons: tuple[str, ...] = ()


def _checkpoint_suffix_source(
    account_labels: tuple[str, ...],
):
    head = ExecutionBookHeadRow
    cuts = PositionRecoveryCheckpointRow
    events = PositionFactJournalEventRow
    checkpoint_id = head.state_payload["recovery_checkpoint"][
        "checkpoint_id"
    ].astext
    revision_text = head.state_payload["journal_revision"].astext
    revision_is_integer = and_(
        func.jsonb_typeof(head.state_payload["journal_revision"]) == "number",
        revision_text.op("~")(r"^-?[0-9]+$"),
    )
    head_revision = case(
        (revision_is_integer, cast(revision_text, BigInteger)),
        else_=None,
    )
    bound = (
        select(
            head.environment.label("environment"),
            head.account_label.label("account_label"),
            head.symbol.label("symbol"),
            head.position_side.label("position_side"),
            head.stream_id.label("stream_id"),
            head.stream_epoch.label("stream_epoch"),
            cuts.source_revision.label("checkpoint_revision"),
            head_revision.label("head_revision"),
        )
        .select_from(head)
        .join(
            cuts,
            and_(
                cuts.environment == head.environment,
                cuts.account_label == head.account_label,
                cuts.symbol == head.symbol,
                cuts.position_side == head.position_side,
                cuts.stream_id == head.stream_id,
                cuts.stream_epoch == head.stream_epoch,
                cuts.checkpoint_id == checkpoint_id,
            ),
        )
        .where(
            head.environment == "live",
            head.account_label.in_(account_labels),
            checkpoint_id.is_not(None),
            checkpoint_id != "",
        )
        .cte("bound_checkpoints")
        .prefix_with("MATERIALIZED", dialect="postgresql")
    )
    return bound.join(
        events,
        and_(
            events.environment == bound.c.environment,
            events.account_label == bound.c.account_label,
            events.symbol == bound.c.symbol,
            events.position_side == bound.c.position_side,
            events.stream_id == bound.c.stream_id,
            events.stream_epoch == bound.c.stream_epoch,
            events.source_revision > bound.c.checkpoint_revision,
            or_(
                bound.c.head_revision.is_(None),
                events.source_revision <= bound.c.head_revision,
            ),
        ),
    )


@dataclass(slots=True)
class _AccountProof:
    current_cuts: list[datetime] = field(default_factory=list)
    historical_cuts: list[datetime] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    scopes: list[str] = field(default_factory=list)
    historical_reasons: list[str] = field(default_factory=list)
    invalid: int = 0
    current_unknown: bool = False
    historical_unknown: bool = False
    active_positions: int = 0
    saw_head: bool = False


def _scope_label(head: ExecutionBookHeadRow) -> str:
    return f"{head.symbol}:{head.position_side}"


def _head_facts(
    head: ExecutionBookHeadRow,
    *,
    active_quantity: Decimal,
    reconciliation_gap: Decimal | None,
) -> HeadFacts:
    payload = head.state_payload or {}
    revision = payload.get("journal_revision")
    unresolved_work = bool(
        payload.get("recovery_command_ids")
        or payload.get("external_recovery_ids")
        or payload.get("active_reservation_ids")
    )
    return HeadFacts(
        scope=_scope_label(head),
        updated_at=head.updated_at,
        journal_revision=revision if isinstance(revision, int) else None,
        active_quantity=active_quantity,
        unresolved_work=unresolved_work,
        reconciliation_gap=reconciliation_gap,
    )


async def load_fact_integrity(
    sessions: async_sessionmaker[AsyncSession],
    *,
    accounts: dict[str, AccountReconciliationHeadRow],
    now: datetime,
    max_age_seconds: float = 180.0,
) -> dict[str, FactIntegritySummary]:
    if not accounts:
        return {}
    async with sessions() as session:
        heads = (
            await session.scalars(
                select(ExecutionBookHeadRow).where(
                    ExecutionBookHeadRow.environment == "live",
                    ExecutionBookHeadRow.account_label.in_(tuple(accounts)),
                    relevant_recovery_head(),
                )
            )
        ).all()
        bindings = []
        for head in heads:
            binding = head.state_payload.get("recovery_checkpoint")
            if isinstance(binding, dict) and binding.get("checkpoint_id"):
                bindings.append(
                    (
                        head.account_label,
                        head.symbol,
                        head.position_side,
                        head.stream_id,
                        head.stream_epoch,
                        binding["checkpoint_id"],
                    )
                )
        checkpoint_id = ExecutionBookHeadRow.state_payload[
            "recovery_checkpoint"
        ]["checkpoint_id"].astext
        rows = (
            (
                await session.scalars(
                    select(PositionRecoveryCheckpointRow)
                    .select_from(ExecutionBookHeadRow)
                    .join(
                        PositionRecoveryCheckpointRow,
                        and_(
                            PositionRecoveryCheckpointRow.environment
                            == ExecutionBookHeadRow.environment,
                            PositionRecoveryCheckpointRow.account_label
                            == ExecutionBookHeadRow.account_label,
                            PositionRecoveryCheckpointRow.symbol
                            == ExecutionBookHeadRow.symbol,
                            PositionRecoveryCheckpointRow.position_side
                            == ExecutionBookHeadRow.position_side,
                            PositionRecoveryCheckpointRow.stream_id
                            == ExecutionBookHeadRow.stream_id,
                            PositionRecoveryCheckpointRow.stream_epoch
                            == ExecutionBookHeadRow.stream_epoch,
                            PositionRecoveryCheckpointRow.checkpoint_id
                            == checkpoint_id,
                        ),
                    )
                    .where(
                        ExecutionBookHeadRow.environment == "live",
                        ExecutionBookHeadRow.account_label.in_(tuple(accounts)),
                        checkpoint_id.is_not(None),
                        checkpoint_id != "",
                    )
                )
            ).all()
            if bindings
            else []
        )
        checkpoints = {
            (
                row.account_label,
                row.symbol,
                row.position_side,
                row.stream_id,
                row.stream_epoch,
                row.checkpoint_id,
            ): row
            for row in rows
        }
        # One aggregate pass proves which journal revisions after each recovery
        # cut are actually persisted, instead of replaying every suffix event.
        # The join is pinned to the exact referenced checkpoints so an older
        # checkpoint of the same scope cannot widen another scope's suffix.
        # Since that exact checkpoint set joins on every scope column, a second
        # event-scope tuple list would be redundant and doubles the bind count.
        suffix_by_scope: dict[tuple[str, str, str, str, str], SuffixFacts] = {}
        if bindings:
            events = PositionFactJournalEventRow
            suffix_rows = (
                await session.execute(
                    select(
                        events.account_label,
                        events.symbol,
                        events.position_side,
                        events.stream_id,
                        events.stream_epoch,
                        func.count(func.distinct(events.source_revision)),
                        func.max(events.source_revision),
                        func.max(events.occurred_at),
                    )
                    .select_from(_checkpoint_suffix_source(tuple(accounts)))
                    .where(
                        events.environment == "live",
                    )
                    .group_by(
                        events.account_label,
                        events.symbol,
                        events.position_side,
                        events.stream_id,
                        events.stream_epoch,
                    )
                )
            ).all()
            suffix_by_scope = {
                (row[0], row[1], row[2], row[3], row[4]): SuffixFacts(
                    distinct_revisions=int(row[5] or 0),
                    max_source_revision=(
                        int(row[6]) if row[6] is not None else None
                    ),
                    newest_occurred_at=row[7],
                )
                for row in suffix_rows
            }
            # Fetch the newest authoritative coverage event in the suffix
            # for each scope, decoding its real payload to obtain the
            # actual coverage end_at — not a proxy from occurred_at.
            newest_coverage_by_scope = await _load_suffix_coverage(
                session, bindings, checkpoints, heads, tuple(accounts)
            )
            for scope_key, coverage_end_at in newest_coverage_by_scope.items():
                if scope_key in suffix_by_scope:
                    existing = suffix_by_scope[scope_key]
                    suffix_by_scope[scope_key] = SuffixFacts(
                        distinct_revisions=existing.distinct_revisions,
                        max_source_revision=existing.max_source_revision,
                        newest_occurred_at=existing.newest_occurred_at,
                        newest_coverage_end_at=coverage_end_at,
                    )

    grouped: dict[str, list[ExecutionBookHeadRow]] = {
        account: [] for account in accounts
    }
    for head in heads:
        grouped[head.account_label].append(head)
    result: dict[str, FactIntegritySummary] = {}
    for account, account_heads in grouped.items():
        proof = _AccountProof(saw_head=bool(account_heads))
        if not account_heads:
            proof.reasons.append(REASON_NO_RELEVANT_HEAD)
        for head in account_heads:
            _apply_head(
                proof,
                account=account,
                head=head,
                checkpoints=checkpoints,
                suffix_by_scope=suffix_by_scope,
                now=now,
                max_age_seconds=max_age_seconds,
            )
        if proof.active_positions != accounts[account].position_count:
            proof.reasons.append(REASON_ACTIVE_POSITION_COUNT_MISMATCH)
            proof.scopes.append("*")
            proof.current_unknown = True
        current_reasons = tuple(dict.fromkeys(proof.reasons))
        if proof.invalid:
            gaps: int | None = proof.invalid
        elif proof.current_unknown:
            gaps = None
        else:
            gaps = 0
        # The reported cut is the oldest proven fact cut backing this account's
        # evidence, current or closed; ``gaps`` alone decides current risk.
        proven_cuts = proof.current_cuts + proof.historical_cuts
        result[account] = FactIntegritySummary(
            gaps=gaps,
            observed_at=min(proven_cuts) if proven_cuts else None,
            reasons=current_reasons,
            unknown_scopes=tuple(dict.fromkeys(proof.scopes)),
            # Integrity failures stay in ``gaps`` (a real conflict is never
            # "historical noise"); this bucket names what closed objects are
            # missing, and an empty tuple means their evidence is complete.
            historical_reasons=tuple(dict.fromkeys(proof.historical_reasons)),
        )
    return result


def _apply_head(
    proof: _AccountProof,
    *,
    account: str,
    head: ExecutionBookHeadRow,
    checkpoints: dict[
        tuple[str, str, str, str, str, str], PositionRecoveryCheckpointRow
    ],
    suffix_by_scope: dict[tuple[str, str, str, str, str], SuffixFacts],
    now: datetime,
    max_age_seconds: float,
) -> None:
    scope = _scope_label(head)
    binding = head.state_payload.get("recovery_checkpoint")
    # Without a decodable checkpoint the head's active quantity is unknown, so
    # a missing binding/row is treated as current risk (fail closed).
    if not isinstance(binding, dict):
        _record_unknown(proof, scope, REASON_CHECKPOINT_MISSING)
        return
    row = checkpoints.get(
        (
            account,
            head.symbol,
            head.position_side,
            head.stream_id,
            head.stream_epoch,
            str(binding.get("checkpoint_id")),
        )
    )
    if row is None:
        _record_unknown(proof, scope, REASON_CHECKPOINT_ROW_MISSING)
        return
    checkpoint = PositionRecoveryCodec.decode_checkpoint(row.payload)
    facts = _head_facts(
        head,
        active_quantity=checkpoint.projection.total_active_quantity,
        reconciliation_gap=checkpoint.projection.reconciliation_gap,
    )
    coverage = checkpoint.coverage
    recovery = RecoveryCut(
        event_cut=checkpoint.event_cut,
        source_revision=row.source_revision,
        identity_matches=(
            checkpoint.key.environment == "live"
            and checkpoint.key.account_label == account
            and (checkpoint.key.symbol, checkpoint.key.position_side.value)
            == (head.symbol, head.position_side)
        ),
        binding_matches=(
            recovery_checkpoint_head_binding(checkpoint) == binding
        ),
        coverage_authoritative=(
            coverage is not None and coverage.is_authoritative
        ),
        coverage_covers_cut=(
            coverage is not None and coverage.covers(checkpoint.event_cut)
        ),
        coverage_end_at=(
            coverage.end_at if coverage is not None else None
        ),
        has_conflicts=checkpoint.has_conflicts,
        has_synthetic_fills=checkpoint.has_synthetic_fills,
        has_late_events=checkpoint.has_late_events,
        has_integrity_issues=bool(checkpoint.integrity_issues),
        projection_ready=(
            checkpoint.projection.health_status.value == "READY"
            and checkpoint.projection.reconciliation_gap == 0
        ),
    )
    verdict = evaluate_head_proof(
        head=facts,
        recovery=recovery,
        suffix=suffix_by_scope.get(
            (
                head.account_label,
                head.symbol,
                head.position_side,
                head.stream_id,
                head.stream_epoch,
            ),
            SuffixFacts(),
        ),
        now=now,
        max_age_seconds=max_age_seconds,
    )
    if verdict.status == INVALID:
        proof.invalid += 1
        proof.reasons.append(verdict.reason)
        proof.scopes.append(scope)
        return
    if checkpoint.projection.total_active_quantity != 0:
        proof.active_positions += 1
    if verdict.status == PROVEN and verdict.cut is not None:
        if verdict.current:
            proof.current_cuts.append(verdict.cut)
        else:
            proof.historical_cuts.append(verdict.cut)
        return
    if verdict.status == PROVEN:
        return
    if verdict.current:
        proof.current_unknown = True
        proof.reasons.append(verdict.reason)
        proof.scopes.append(scope)
    else:
        proof.historical_unknown = True
        proof.historical_reasons.append(verdict.reason)


def _record_unknown(proof: _AccountProof, scope: str, reason: str) -> None:
    proof.current_unknown = True
    proof.reasons.append(reason)
    proof.scopes.append(scope)


async def _load_suffix_coverage(
    session: AsyncSession,
    bindings: list[tuple[str, str, str, str, str, str]],
    checkpoints: dict[
        tuple[str, str, str, str, str, str], PositionRecoveryCheckpointRow
    ],
    heads: list[ExecutionBookHeadRow],
    account_labels: tuple[str, ...],
) -> dict[tuple[str, str, str, str, str], datetime]:
    """Load the newest authoritative coverage end_at from suffix coverage events.

    For each scope, fetches coverage events (event_kind="coverage") persisted
    after the bound checkpoint, decodes their payloads using the real codec,
    validates authority, coverage interval, stream/epoch/environment, and
    anchor relationship, and returns the maximum valid ``end_at`` across all
    qualifying coverage events.  A newer revision with a smaller ``end_at``
    does not overwrite a larger valid ``end_at`` from an older revision,
    preventing watermark regression.
    """
    events = PositionFactJournalEventRow
    coverage_rows = (
        await session.execute(
            select(
                events.account_label,
                events.symbol,
                events.position_side,
                events.stream_id,
                events.stream_epoch,
                events.payload,
                events.source_revision,
            )
            .select_from(_checkpoint_suffix_source(account_labels))
            .where(
                events.environment == "live",
                events.event_kind == "coverage",
            )
        )
    ).all()

    # Build a lookup from scope key to the bound checkpoint for that scope.
    scope_to_bound: dict[
        tuple[str, str, str, str, str], PositionRecoveryCheckpointRow
    ] = {}
    for b in bindings:
        scope_k = (b[0], b[1], b[2], b[3], b[4])
        cp = checkpoints.get(b)
        if cp is not None:
            scope_to_bound[scope_k] = cp

    # Build per-scope max journal_revision from heads.  Coverage events
    # beyond the head's declared revision cannot prove current state.
    scope_max_revision: dict[tuple[str, str, str, str, str], int] = {}
    for head in heads:
        payload = head.state_payload or {}
        rev = payload.get("journal_revision")
        if isinstance(rev, int):
            scope_k = (
                head.account_label,
                head.symbol,
                head.position_side,
                head.stream_id,
                head.stream_epoch,
            )
            existing = scope_max_revision.get(scope_k)
            if existing is None or rev > existing:
                scope_max_revision[scope_k] = rev

    result: dict[tuple[str, str, str, str, str], datetime] = {}
    for row in coverage_rows:
        scope_key: tuple[str, str, str, str, str] = (
            row[0], row[1], row[2], row[3], row[4]
        )
        # Coverage events beyond the head's declared journal_revision
        # cannot prove current state.
        max_rev = scope_max_revision.get(scope_key)
        if max_rev is not None and row[6] > max_rev:
            continue
        payload = row[5]
        try:
            coverage = PositionRecoveryCodec.decode_coverage(payload)
        except RecoverySchemaError:
            # Malformed or schema-incompatible payload cannot prove anything.
            continue
        # Validate authority: must be authoritative (no gaps, confirmed,
        # page_exhausted, not_truncated, complete provenance with
        # source_anchor_event_cut <= start_at).
        if not coverage.is_authoritative:
            continue
        # Live coverage must have a stream_scope with environment == "live";
        # reject "legacy authoritative" entries that bypass stream identity
        # by having None stream_scope or a non-live environment.
        if coverage.stream_scope is None:
            continue
        if coverage.stream_scope.environment != "live":
            continue
        # Validate stream_scope matches the scope we are proving.
        cs = coverage.stream_scope
        if (
            cs.account_label != row[0]
            or cs.symbol != row[1]
            or cs.position_side.value != row[2]
            or cs.stream_id != row[3]
            or cs.stream_epoch != row[4]
        ):
            continue
        # Validate anchor: the coverage's provenance anchor must reference
        # the bound checkpoint for this scope.  For recovery_checkpoint
        # anchors, source_anchor_id and source_anchor_event_cut must match
        # the bound checkpoint.  For zero_snapshot anchors, the bound
        # checkpoint's own coverage must have proven a zero_snapshot source,
        # and the id/cut must strictly match; otherwise fail closed.
        bound_cp = scope_to_bound.get(scope_key)
        if bound_cp is not None and coverage.load_provenance is not None:
            lp = coverage.load_provenance
            if lp.source_anchor_kind == "recovery_checkpoint":
                if (
                    lp.source_anchor_id is not None
                    and lp.source_anchor_id != bound_cp.checkpoint_id
                ):
                    continue
                if (
                    lp.source_anchor_event_cut is not None
                    and lp.source_anchor_event_cut != bound_cp.event_cut
                ):
                    continue
            elif lp.source_anchor_kind == "zero_snapshot":
                # Fail closed: the bound checkpoint must have a known
                # zero_snapshot source we can compare against.
                try:
                    cp_decoded = PositionRecoveryCodec.decode_checkpoint(
                        bound_cp.payload
                    )
                except (RecoverySchemaError, ValueError):
                    continue
                cp_coverage = cp_decoded.coverage
                if cp_coverage is None or cp_coverage.load_provenance is None:
                    continue
                cp_lp = cp_coverage.load_provenance
                if cp_lp.source_anchor_kind != "zero_snapshot":
                    continue
                if (
                    lp.source_anchor_id is not None
                    and lp.source_anchor_id != cp_lp.source_anchor_id
                ):
                    continue
                if (
                    lp.source_anchor_event_cut is not None
                    and cp_lp.source_anchor_event_cut is not None
                    and lp.source_anchor_event_cut
                    != cp_lp.source_anchor_event_cut
                ):
                    continue
            else:
                # Unknown anchor kind cannot be verified.
                continue
        # The coverage must bridge from the bound recovery cut to the
        # current proof cut.  If the coverage interval starts after the
        # bound cut, there is a gap that cannot prove current risk.
        if bound_cp is not None:
            if not coverage.covers(bound_cp.event_cut):
                continue
        # Keep the maximum valid end_at per scope, preventing watermark
        # regression from a newer but less-extended coverage.
        existing = result.get(scope_key)
        if existing is None or coverage.end_at > existing:
            result[scope_key] = coverage.end_at
    return result


__all__ = ["FactIntegritySummary", "load_fact_integrity"]
