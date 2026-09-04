"""Watermark window semantics.

Three properties, each of which is a specific and expensive bug avoided. They
are unit tests rather than integration tests because the logic is pure
arithmetic over an interval - and because a bug here would be invisible in an
integration test that happened to run at the right moment.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from volthive.ingest.watermark import EPOCH, ExtractWindow, Watermark, compute_window

pytestmark = pytest.mark.unit


def _timestamp_watermark(value: datetime, lookback: timedelta = timedelta(hours=2)) -> Watermark:
    return Watermark("CMS", "customers", "timestamp", value, None, lookback)


def _partition_watermark(value: str, lookback: timedelta = timedelta(days=3)) -> Watermark:
    return Watermark("OCPP", "ocpp_cdr", "date_partition", None, value, lookback)


INTERVAL_START = datetime(2026, 6, 14, tzinfo=UTC)
INTERVAL_END = datetime(2026, 6, 15, tzinfo=UTC)


class TestBoundedWindow:
    def test_upper_bound_is_the_data_interval_not_now(self) -> None:
        """THE line that separates a backfillable pipeline from a broken one.

        With now() as the upper bound, the same logical run extracts a
        different set depending on when it executes - so a rerun is not
        reproducible and a backfill silently pulls CURRENT data into a
        HISTORICAL partition.
        """
        window = compute_window(
            _timestamp_watermark(datetime(2026, 6, 13, 12, tzinfo=UTC)),
            data_interval_start=INTERVAL_START,
            data_interval_end=INTERVAL_END,
        )
        assert window.upper_inclusive == INTERVAL_END

    def test_lookback_is_applied_to_the_lower_bound(self) -> None:
        watermark_value = datetime(2026, 6, 14, 12, tzinfo=UTC)
        window = compute_window(
            _timestamp_watermark(watermark_value, timedelta(hours=2)),
            data_interval_start=watermark_value,
            data_interval_end=INTERVAL_END,
        )
        assert window.lower_exclusive == watermark_value - timedelta(hours=2)

    def test_lookback_is_per_source_not_global(self) -> None:
        """The partner revises for a week; files are rewritten for three days."""
        partner = compute_window(
            Watermark("PARTNER", "partner_cdr", "cursor", INTERVAL_START, None, timedelta(days=7)),
            data_interval_start=INTERVAL_START,
            data_interval_end=INTERVAL_END,
        )
        files = compute_window(
            _partition_watermark("2026-06-13"),
            data_interval_start=INTERVAL_START,
            data_interval_end=INTERVAL_END,
        )
        assert partner.lookback == timedelta(days=7)
        assert files.lookback == timedelta(days=3)

    def test_bootstrap_window_reaches_back_to_the_epoch(self) -> None:
        """A seeded epoch watermark must make the first run extract everything."""
        window = compute_window(
            _timestamp_watermark(EPOCH),
            data_interval_start=INTERVAL_START,
            data_interval_end=INTERVAL_END,
        )
        assert window.lower_exclusive <= EPOCH

    def test_backfill_does_not_re_extract_everything(self) -> None:
        """A backfill of an OLD window, with the watermark far ahead.

        Without clamping the lower bound to the interval, replaying May while
        the watermark sits in September would extract four months of data into
        May's partition.
        """
        window = compute_window(
            _timestamp_watermark(datetime(2026, 9, 1, tzinfo=UTC)),
            data_interval_start=datetime(2026, 5, 1, tzinfo=UTC),
            data_interval_end=datetime(2026, 5, 2, tzinfo=UTC),
        )
        assert window.lower_exclusive < datetime(2026, 5, 1, tzinfo=UTC)
        assert window.lower_exclusive > datetime(2026, 4, 30, tzinfo=UTC)

    def test_override_widens_the_window_for_a_repair(self) -> None:
        window = compute_window(
            _timestamp_watermark(INTERVAL_START),
            data_interval_start=INTERVAL_START,
            data_interval_end=INTERVAL_END,
            lookback_override=timedelta(days=30),
        )
        assert window.lower_exclusive == INTERVAL_START - timedelta(days=30)


class TestBatchKey:
    def test_batch_key_is_the_interval_start_day_not_the_end_day(self) -> None:
        """A daily run for the 14th has interval [14th 00:00, 15th 00:00).

        Its batch key must be the 14th. Taking the interval end's date would
        label every day's data with tomorrow's date - and the restatement
        window would then never line up with the data it was meant to rewrite.
        """
        window = compute_window(
            _timestamp_watermark(INTERVAL_START),
            data_interval_start=INTERVAL_START,
            data_interval_end=INTERVAL_END,
        )
        assert window.batch_key.isoformat() == "2026-06-14"


class TestPartitionWindow:
    def test_partitions_cover_the_lookback_and_the_interval(self) -> None:
        window = compute_window(
            _partition_watermark("2026-06-13", timedelta(days=3)),
            data_interval_start=INTERVAL_START,
            data_interval_end=INTERVAL_END,
        )
        assert [p.isoformat() for p in window.partitions] == [
            "2026-06-11",
            "2026-06-12",
            "2026-06-13",
            "2026-06-14",
        ]

    def test_partitions_come_from_the_interval_not_the_watermark(self) -> None:
        """During a backfill the watermark is ahead of the window being run.

        Looking back from the WATERMARK would scan days that have nothing to do
        with the window the run is meant to process.
        """
        window = compute_window(
            _partition_watermark("2026-09-30", timedelta(days=3)),
            data_interval_start=datetime(2026, 5, 10, tzinfo=UTC),
            data_interval_end=datetime(2026, 5, 11, tzinfo=UTC),
        )
        assert window.partitions[-1].isoformat() == "2026-05-10"
        assert len(window.partitions) == 4

    def test_timestamp_sources_have_no_partitions(self) -> None:
        window = compute_window(
            _timestamp_watermark(INTERVAL_START),
            data_interval_start=INTERVAL_START,
            data_interval_end=INTERVAL_END,
        )
        assert window.partitions == []


class TestWindowShape:
    def test_window_is_half_open(self) -> None:
        """(lower, upper] - consecutive windows neither skip nor duplicate.

        Inclusive on both ends double-counts the boundary instant; exclusive on
        both loses it. The naming makes the intent unmissable at the call site.
        """
        window = ExtractWindow(INTERVAL_START, INTERVAL_END, timedelta(0), [])
        assert window.lower_exclusive == INTERVAL_START
        assert window.upper_inclusive == INTERVAL_END
