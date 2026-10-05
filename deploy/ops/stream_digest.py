"""Bounded, order-independent content digests for retention verification.

Retention verifies an archive before deleting its source rows.  Building one
ordered ``string_agg`` in PostgreSQL turns that safety check into an unbounded
sort and temporary-file workload.  This accumulator instead consumes one
canonical row at a time and retains only a count plus two 256-bit values.
"""

from __future__ import annotations

import hashlib

_ALGORITHM = "sha256-multiset-v1"
_MASK = (1 << 256) - 1


class StreamingContentDigest:
    """Accumulate a deterministic digest for a multiset of serialized rows.

    Each row is hashed independently with SHA-256.  The modular sum and XOR
    make the aggregate insensitive to scan order, so PostgreSQL can stream
    rows without an ``ORDER BY`` sort.  Keeping both independent accumulators
    makes an accidental multiset collision astronomically unlikely; the row
    count is included as an additional guard.
    """

    def __init__(self) -> None:
        self._count = 0
        self._sum = 0
        self._xor = 0

    def update(self, row: bytes) -> None:
        value = int.from_bytes(hashlib.sha256(row).digest(), "big")
        self._count += 1
        self._sum = (self._sum + value) & _MASK
        self._xor ^= value

    def hexdigest(self) -> str:
        return (
            f"{_ALGORITHM}:{self._count}:"
            f"{self._sum:064x}:{self._xor:064x}"
        )
