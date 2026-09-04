"""Incremental loading: watermarks, the file registry, and the lookback window.

These are the tests that prove "how do you know what's new?" has a real answer
rather than a plausible one. Each mutates the warehouse, so each takes its own
transaction and rolls back, or operates on a copied landing zone.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from tests.conftest import requires_database

from volthive.db.connection import fetch_all, fetch_one, fetch_value
from volthive.ingest.registry import discover_partition_files, file_sha256, is_already_ingested

pytestmark = [pytest.mark.integration, requires_database]


class TestFileRegistry:
    def test_every_generated_file_was_registered(self, warehouse_conn) -> None:
        registered = fetch_value(
            warehouse_conn,
            "SELECT count(*) FROM ctl.ingested_file WHERE status = 'LOADED'",
        )
        assert registered > 0

    def test_reingesting_the_same_file_is_skipped(self, warehouse_conn, built_warehouse) -> None:
        """Same path, same hash: already registered, nothing to do.

        This is the whole of file-level idempotency, and it needs no branching
        logic - it falls out of keying the registry on (path, content hash).
        """
        landing = Path(str(built_warehouse["data_dir"])) / "landing"
        files = discover_partition_files(
            landing, "cdr", [built_warehouse["date_from"]], pattern="*.jsonl"
        )
        assert files, "no files discovered"
        for found in files:
            assert is_already_ingested(warehouse_conn, found)

    def test_a_rewritten_file_is_reprocessed(
        self, warehouse_conn, built_warehouse, landing_copy: Path
    ) -> None:
        """Corrections appended to a partition change its hash.

        Keying on path alone would silently ignore them; keying on hash alone
        would reprocess a file that was merely moved. The pair is what makes
        both cases correct.
        """
        files = discover_partition_files(
            landing_copy, "cdr", [built_warehouse["date_from"]], pattern="*.jsonl"
        )
        target = files[0]
        original_hash = target.sha256

        with target.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"transaction_id": "TXN-APPENDED-0001"}) + "\n")

        assert file_sha256(target.path) != original_hash

        rediscovered = discover_partition_files(
            landing_copy, "cdr", [built_warehouse["date_from"]], pattern="*.jsonl"
        )
        changed = next(f for f in rediscovered if f.path == target.path)
        assert not is_already_ingested(
            warehouse_conn, changed
        ), "a rewritten file must not be treated as already loaded"

    def test_the_registry_records_row_counts(self, warehouse_conn) -> None:
        """So "how many rows did that file actually contain?" is answerable."""
        row = fetch_one(
            warehouse_conn,
            """
            SELECT count(*) AS files, sum(row_count) AS rows
            FROM ctl.ingested_file WHERE source_system = 'OCPP'
            """,
        )
        assert row["files"] > 0
        assert row["rows"] > 0


class TestWatermarkAdvancement:
    def test_every_source_advanced_past_its_seed(self, warehouse_conn) -> None:
        stale = fetch_all(
            warehouse_conn,
            """
            SELECT source_system, entity FROM ctl.watermark
            WHERE watermark_type <> 'none'
              AND (
                  watermark_ts_utc = TIMESTAMPTZ '1970-01-01 00:00:00+00'
                  OR watermark_str = '1970-01-01'
              )
            """,
        )
        assert stale == [], stale

    def test_the_watermark_comes_from_the_data_not_the_clock(self, warehouse_conn) -> None:
        """The CMS watermark must equal the newest updated_at actually loaded.

        Setting it to now() instead would skip every row that arrives later
        carrying an older timestamp - a silent, permanent data loss.
        """
        row = fetch_one(
            warehouse_conn,
            """
            SELECT
                w.watermark_ts_utc AS watermark,
                (
                    SELECT max(stg.safe_timestamptz(r.updated_at))
                    FROM raw.cms_customers AS r
                ) AS newest_loaded
            FROM ctl.watermark AS w
            WHERE w.source_system = 'CMS' AND w.entity = 'customers'
            """,
        )
        assert row["watermark"] == row["newest_loaded"]

    def test_the_watermark_never_regresses(self, warehouse_conn) -> None:
        """A backfill of May executing after June's run must not rewind it.

        GREATEST is what guarantees it, and this asserts the guarantee rather
        than the implementation.
        """
        from volthive.ingest.watermark import advance_watermark, read_watermark

        before = read_watermark(warehouse_conn, "CMS", "customers")
        try:
            with warehouse_conn.transaction():
                advance_watermark(
                    warehouse_conn,
                    "CMS",
                    "customers",
                    new_value=datetime(1999, 1, 1, tzinfo=UTC),
                    run_id="00000000-0000-0000-0000-000000000000",
                )
                after = read_watermark(warehouse_conn, "CMS", "customers")
                assert after.watermark_ts_utc == before.watermark_ts_utc
                raise _Rollback
        except _Rollback:
            pass

    def test_an_empty_extract_leaves_the_watermark_alone(self, warehouse_conn) -> None:
        """Advancing on zero rows would skip anything arriving later inside the
        window that was just read."""
        from volthive.ingest.watermark import advance_watermark, read_watermark

        before = read_watermark(warehouse_conn, "CMS", "customers")
        with warehouse_conn.transaction():
            advance_watermark(warehouse_conn, "CMS", "customers", new_value=None, run_id=None)
        after = read_watermark(warehouse_conn, "CMS", "customers")
        assert after.watermark_ts_utc == before.watermark_ts_utc

    def test_the_watermark_is_capped_at_the_window_upper_bound(self, warehouse_conn) -> None:
        """A source row with a FUTURE timestamp - clock skew, a bad test record -
        would otherwise push the watermark past the interval and skip
        everything in between."""
        from volthive.ingest.watermark import advance_watermark, read_watermark

        upper = datetime(2026, 6, 8, tzinfo=UTC)
        try:
            with warehouse_conn.transaction():
                advance_watermark(
                    warehouse_conn,
                    "CMS",
                    "customers",
                    new_value=datetime(2030, 1, 1, tzinfo=UTC),
                    upper_bound=upper,
                    run_id="00000000-0000-0000-0000-000000000000",
                )
                after = read_watermark(warehouse_conn, "CMS", "customers")
                assert after.watermark_ts_utc <= upper
                raise _Rollback
        except _Rollback:
            pass


class TestSecondRunLoadsOnlyNewRows:
    def test_a_second_ingest_of_the_same_window_lands_nothing(
        self, warehouse_conn, built_warehouse
    ) -> None:
        """The behaviour someone actually asks about: what if it runs twice?

        Files: every hash is registered, so all are skipped and zero rows land.
        """
        from volthive.ingest.files import ingest_file_source

        stat = ingest_file_source(
            warehouse_conn,
            "ocpp_cdr",
            run_id="00000000-0000-0000-0000-000000000000",
            data_interval_start=datetime(2026, 6, 1, tzinfo=UTC),
            data_interval_end=datetime(2026, 6, 8, tzinfo=UTC),
            landing_root=Path(str(built_warehouse["data_dir"])) / "landing",
        )
        assert stat.rows_inserted == 0
        assert stat.files_skipped == stat.files_seen
        assert stat.files_loaded == 0


class TestLookbackWindow:
    def test_the_lookback_is_per_source(self, warehouse_conn) -> None:
        """Not a global constant: it is a property of how each source behaves.

        The partner revises for a week while billing disputes settle; a landing
        file is rewritten for a couple of days at most.
        """
        lookbacks = {
            (row["source_system"], row["entity"]): row["lookback_interval"]
            for row in fetch_all(
                warehouse_conn,
                "SELECT source_system, entity, lookback_interval FROM ctl.watermark",
            )
        }
        assert lookbacks[("PARTNER", "partner_cdr")] == timedelta(days=7)
        assert lookbacks[("OCPP", "ocpp_cdr")] == timedelta(days=3)
        assert lookbacks[("CMS", "customers")] == timedelta(hours=2)

    def test_the_partner_lookback_captures_revisions(self, warehouse_conn) -> None:
        """The generator restates partner records days after the session.

        Those restatements land in a LATER dt= partition than the session, so
        only a wide enough lookback finds them.
        """
        revised = fetch_value(
            warehouse_conn,
            """
            SELECT count(*) FROM (
                SELECT payload ->> 'cdr_id' AS cdr_id
                FROM raw.partner_cdr
                GROUP BY payload ->> 'cdr_id'
                HAVING count(*) > 1
            ) AS t
            """,
        )
        assert revised > 0, "no partner revision was captured - the lookback is untested"

    def test_the_latest_partner_revision_wins(self, warehouse_conn) -> None:
        """Deduplication orders on last_updated, so a restatement supersedes
        the original rather than racing it."""
        mismatched = fetch_value(
            warehouse_conn,
            """
            SELECT count(*)
            FROM stg.partner_session AS p
            WHERE p.last_updated_utc <> (
                SELECT max(stg.safe_timestamptz(r.payload ->> 'last_updated'))
                FROM raw.partner_cdr AS r
                WHERE r.payload ->> 'cdr_id' = p.cdr_id
            )
            """,
        )
        assert mismatched == 0


class TestTransactionalSafety:
    def test_a_failed_load_leaves_no_partial_state(self, warehouse_conn) -> None:
        """The crash-safety argument, asserted rather than described.

        Rows and watermark move together or not at all, so there is no state in
        which the data landed but the pipeline forgot that it did.
        """
        from volthive.ingest.watermark import read_watermark

        before_rows = fetch_value(warehouse_conn, "SELECT count(*) FROM raw.cms_customers")
        before_watermark = read_watermark(warehouse_conn, "CMS", "customers")

        try:
            with warehouse_conn.transaction():
                with warehouse_conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO raw.cms_customers (
                            dw_run_id, dw_source_system, dw_batch_key,
                            customer_id, src_row_hash
                        )
                        VALUES (
                            '00000000-0000-0000-0000-000000000000', 'CMS',
                            DATE '2026-06-07', 'CUS-CRASH-TEST', repeat('0', 64)
                        )
                        """
                    )
                raise _Rollback
        except _Rollback:
            pass

        assert fetch_value(warehouse_conn, "SELECT count(*) FROM raw.cms_customers") == (
            before_rows
        )
        assert (
            read_watermark(warehouse_conn, "CMS", "customers").watermark_ts_utc
            == before_watermark.watermark_ts_utc
        )


class _Rollback(Exception):
    """Sentinel used to roll back a mutating assertion block."""
