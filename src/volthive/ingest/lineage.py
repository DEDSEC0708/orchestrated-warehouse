"""Lineage stamping and row hashing for the raw layer.

Every raw row carries an audit block that answers, from the row alone: which
run wrote it, from which source system, from which file, at which line, and
which logical partition it belongs to. That block is what makes the
end-to-end lineage claim real - from one fact row you can reach the run, the
task, the source file, the exact line, and the git commit of the code that
processed it.

Two hashes are computed here and they mean different things:

``src_row_hash``
    A hash of the BUSINESS columns only, so "did this row actually change?"
    can be answered without comparing every column. Used by the staging dedupe.

``payload_hash``
    A hash of the ENTIRE original JSON line, byte for byte. Used to recognise
    an exact re-delivery - an OCPP retry storm sending the same record twice -
    which is expected protocol behaviour and must be counted, not treated as an
    error.

Both are NULL-safe in the way that matters. ``concat_ws`` and its Python
equivalent SKIP nulls, so ``('A', None, 'B')`` and ``('A', 'B', None)`` would
hash identically - and two genuinely different rows would look unchanged. Every
value is therefore coalesced to an explicit sentinel and joined with a
delimiter that cannot occur in the data. There is a unit test named for exactly
this, because it is the kind of bug that produces silently missing dimension
versions months later.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from typing import Any

__all__ = ["HASH_DELIMITER", "NULL_SENTINEL", "payload_hash", "row_hash"]

#: ASCII UNIT SEPARATOR. Cannot occur in JSON, CSV or SQL source data, and -
#: unlike the obvious NUL byte - it is representable in PostgreSQL text, which
#: matters because core.row_hash() must produce IDENTICAL hashes to this
#: function. PostgreSQL text cannot contain a NUL, so \0 was not an option.
HASH_DELIMITER = "\x1f"

#: ASCII RECORD SEPARATORs around the word NULL. Explicit stand-in for a null,
#: so a null and the literal string "None" - or a null and a missing field -
#: cannot collide.
NULL_SENTINEL = "\x1eNULL\x1e"


def row_hash(values: Sequence[Any]) -> str:
    """sha256 over an ordered sequence of values, NULL-safe.

    The ORDER of ``values`` is part of the hash's meaning, so callers must pass
    a fixed column order. Reordering the column list changes every hash and
    would make the next SCD2 merge treat every row as changed - which is why
    the tracked-column lists live in one place per dimension rather than being
    assembled ad hoc at each call site.
    """
    joined = HASH_DELIMITER.join(NULL_SENTINEL if value is None else str(value) for value in values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def payload_hash(raw_line: str) -> str:
    """sha256 of an original source line, exactly as delivered.

    Hashes the bytes as received rather than a re-serialised parse: a producer
    that emits the same record with different key ordering or whitespace has
    genuinely sent a different line, and re-serialising would hide that.
    """
    return hashlib.sha256(raw_line.encode("utf-8")).hexdigest()
