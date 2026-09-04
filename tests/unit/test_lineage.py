"""Row hashing: the NULL-safety property that SCD Type 2 depends on.

If two genuinely different dimension versions hash identically, the merge
classifies the second as UNCHANGED and silently drops it. The version is gone,
every fact after it resolves to the wrong attributes, and nothing fails. It is
the most expensive quiet bug available in this design, which is why it gets its
own test file.
"""

from __future__ import annotations

import pytest

from volthive.ingest.lineage import HASH_DELIMITER, NULL_SENTINEL, payload_hash, row_hash

pytestmark = pytest.mark.unit


class TestRowHash:
    def test_is_stable_across_calls(self) -> None:
        assert row_hash(["a", "b", None]) == row_hash(["a", "b", None])

    def test_distinguishes_null_placement(self) -> None:
        """THE bug this design exists to prevent.

        ``concat_ws`` and its Python equivalent SKIP nulls, so ('A', NULL, 'B')
        and ('A', 'B', NULL) would join to the same string and hash
        identically - and a real change would look like no change at all.
        """
        assert row_hash(["A", None, "B"]) != row_hash(["A", "B", None])

    def test_distinguishes_a_null_from_the_string_none(self) -> None:
        assert row_hash([None]) != row_hash(["None"])

    def test_distinguishes_a_null_from_an_empty_string(self) -> None:
        assert row_hash([None]) != row_hash([""])

    def test_order_is_part_of_the_hash(self) -> None:
        assert row_hash(["a", "b"]) != row_hash(["b", "a"])

    def test_concatenation_cannot_be_forged_across_a_field_boundary(self) -> None:
        """('ab', 'c') must not collide with ('a', 'bc').

        A delimiter that could occur in the data would let two different rows
        produce the same joined string. The unit separator cannot appear in
        JSON, CSV or SQL source data.
        """
        assert row_hash(["ab", "c"]) != row_hash(["a", "bc"])

    def test_returns_a_hex_sha256(self) -> None:
        digest = row_hash(["x"])
        assert len(digest) == 64
        assert set(digest) <= set("0123456789abcdef")

    def test_numeric_values_are_stringified_consistently(self) -> None:
        assert row_hash([1, 2]) == row_hash(["1", "2"])


class TestSentinels:
    def test_delimiter_avoids_the_nul_byte(self) -> None:
        """PostgreSQL text CANNOT contain a NUL byte.

        core.row_hash() must produce identical hashes to the Python function,
        so the obvious \\0 delimiter was unavailable and the constants have to
        stay representable in both.
        """
        assert "\x00" not in HASH_DELIMITER
        assert "\x00" not in NULL_SENTINEL


class TestPayloadHash:
    def test_identical_lines_share_a_hash(self) -> None:
        line = '{"transaction_id": "TXN-1"}'
        assert payload_hash(line) == payload_hash(line)

    def test_whitespace_difference_produces_a_different_hash(self) -> None:
        """Deliberate: the hash is of the BYTES AS DELIVERED.

        A producer that re-serialises with different spacing has genuinely sent
        a different line, and re-parsing to normalise it would hide that.
        """
        assert payload_hash('{"a": 1}') != payload_hash('{"a":1}')
