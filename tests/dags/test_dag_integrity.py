"""DAG integrity: policy enforced as a test, not as documentation.

These run WITHOUT A DATABASE and in under a couple of seconds, so they can run
on every push and catch the class of mistake that is otherwise only discovered
when the scheduler picks the DAG up: an import error, a cycle, a task with no
timeout, a DAG file that has quietly accumulated business logic.

The last one matters most. A retry policy written in a document drifts within a
month. A retry policy written as an assertion is still true in a year, because
breaking it turns the build red.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.dags

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_FOLDER = REPO_ROOT / "dags"

EXPECTED_DAG_IDS = {
    "volthive_ingest_master",
    "volthive_ingest_sessions",
    "volthive_build_warehouse",
    "volthive_maintenance",
}

#: A DAG file longer than this is almost certainly holding business logic that
#: belongs in src/volthive/. The number is generous - these files carry a lot of
#: explanatory prose - and the point is to catch a trend, not to police style.
MAX_DAG_FILE_LINES = 320


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


class TestImportIntegrity:
    def test_no_import_errors(self, dagbag) -> None:
        assert dagbag.import_errors == {}, dagbag.import_errors

    def test_all_expected_dags_are_present(self, dagbag) -> None:
        assert set(dagbag.dags) >= EXPECTED_DAG_IDS

    def test_dagbag_imports_quickly(self) -> None:
        """Catches heavy top-level code, a real Airflow anti-pattern.

        The scheduler re-imports every DAG file on a short interval. A module
        that opens a connection or reads a config file at import time does so
        hundreds of times an hour, and turns a configuration mistake into a
        scheduler-wide outage rather than one failed task.
        """
        from airflow.models.dagbag import DagBag

        started = time.monotonic()
        DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)
        assert time.monotonic() - started < 10.0

    def test_no_cycles(self, dagbag) -> None:
        from airflow.utils.dag_cycle_tester import check_cycle

        for dag in dagbag.dags.values():
            check_cycle(dag)


class TestTaskPolicy:
    """Every task must be retryable, bounded and observable.

    These three assertions are the policy from dags/common/default_args.py,
    enforced. A task added later that forgets any of them fails the build.
    """

    def test_every_task_has_at_least_one_retry(self, dagbag) -> None:
        for dag in dagbag.dags.values():
            for task in dag.tasks:
                assert task.retries >= 1, f"{dag.dag_id}.{task.task_id}"

    def test_every_task_has_an_execution_timeout(self, dagbag) -> None:
        """A hung task with no timeout holds a worker slot until someone notices."""
        for dag in dagbag.dags.values():
            for task in dag.tasks:
                assert task.execution_timeout is not None, f"{dag.dag_id}.{task.task_id}"

    def test_every_task_has_a_failure_callback(self, dagbag) -> None:
        """Without it, a failure exists only in Airflow's own logs."""
        for dag in dagbag.dags.values():
            for task in dag.tasks:
                assert task.on_failure_callback, f"{dag.dag_id}.{task.task_id}"

    def test_ingestion_tasks_retry_more_than_transform_tasks(self, dagbag) -> None:
        """The policy that has an actual reason behind it.

        Ingestion failures are usually transient and retrying fixes them.
        Transform failures are usually deterministic, and retrying one three
        times produces the same error three times while delaying the real
        message.
        """
        dag = dagbag.dags["volthive_ingest_sessions"]
        ingest_task = dag.get_task("ingest_session_files")
        build = dagbag.dags["volthive_build_warehouse"]
        transform_task = build.get_task("staging.rebuild_staging")
        assert ingest_task.retries > transform_task.retries


class TestDagMetadata:
    def test_every_dag_has_an_owner_tags_and_docs(self, dagbag) -> None:
        for dag in dagbag.dags.values():
            assert dag.default_args.get("owner"), dag.dag_id
            assert dag.tags, dag.dag_id
            assert dag.doc_md, dag.dag_id

    def test_every_dag_serialises_runs_with_max_active_runs_one(self, dagbag) -> None:
        """Two concurrent runs would race on the same restatement window."""
        for dag in dagbag.dags.values():
            assert dag.max_active_runs == 1, dag.dag_id


class TestSchedulingPolicy:
    def test_catchup_settings_are_deliberate(self, dagbag) -> None:
        """Two DAGs, opposite settings, each correct for its own reason.

        Session ingest is PARTITION-oriented - each logical day maps to
        specific files - so replaying a missed day means reading that day's
        files. Master ingest is WATERMARK-oriented, so a missed day is covered
        automatically by the next run's widened predicate and replaying it
        thirty times would do the same work thirty times.
        """
        assert dagbag.dags["volthive_ingest_sessions"].catchup is True
        assert dagbag.dags["volthive_ingest_master"].catchup is False
        assert dagbag.dags["volthive_maintenance"].catchup is False

    def test_the_warehouse_build_is_dataset_scheduled(self, dagbag) -> None:
        """It must wait for BOTH upstream sources, not for a clock.

        Asserted through the TIMETABLE rather than through a DAG attribute:
        the attribute name changed between Airflow versions (and changes again
        in 3.x, where datasets become assets), while the timetable type is the
        stable statement of "this DAG is triggered by data, not by a clock".
        """
        from airflow.timetables.simple import DatasetTriggeredTimetable

        dag = dagbag.dags["volthive_build_warehouse"]
        assert isinstance(dag.timetable, DatasetTriggeredTimetable)

        condition = dag.timetable.dataset_condition
        uris = {getattr(obj, "uri", None) for obj in condition.objects}
        assert len(uris) == 2, uris

    def test_the_ingest_dags_publish_datasets(self, dagbag) -> None:
        producers = {
            "volthive_ingest_master": "publish_raw_cms_master",
            "volthive_ingest_sessions": "publish_raw_sessions",
        }
        for dag_id, task_id in producers.items():
            task = dagbag.dags[dag_id].get_task(task_id)
            assert task.outlets, f"{dag_id}.{task_id} publishes no dataset"


class TestThinDagFiles:
    def test_dag_files_stay_thin(self) -> None:
        """Business logic in a DAG file cannot be unit-tested.

        It also gets re-parsed by the scheduler every few seconds and turns a
        one-line fix into a DAG redeploy. The limit is generous; the point is
        to catch drift.
        """
        for path in DAGS_FOLDER.glob("volthive_*.py"):
            lines = len(path.read_text(encoding="utf-8").splitlines())
            assert lines <= MAX_DAG_FILE_LINES, f"{path.name} is {lines} lines"

    def test_no_database_calls_at_dag_import_time(self) -> None:
        """A connection opened at module scope runs on every scheduler parse.

        Under LocalExecutor the scheduler also forks, and a connection
        inherited by a child process is a connection two processes think they
        own. Every DAG here calls into volthive.pipeline from INSIDE a task.
        """
        import ast

        for path in DAGS_FOLDER.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in tree.body:
                # Anything at module level that is not an import, a constant, a
                # definition or the `with DAG(...)` block itself.
                if isinstance(node, ast.Expr | ast.Assign):
                    source = ast.dump(node)
                    assert "warehouse_connection" not in source, path.name
                    assert "psycopg" not in source, path.name


class TestFailureBehaviour:
    def test_the_run_is_always_closed_out(self, dagbag) -> None:
        """A run left RUNNING for ever misreports the health view."""
        for dag_id in ("volthive_ingest_master", "volthive_build_warehouse"):
            task = dagbag.dags[dag_id].get_task("close_pipeline_run")
            assert task.trigger_rule == "all_done"

    def test_the_advisory_lock_is_always_released(self, dagbag) -> None:
        """A lock that outlives a failed run blocks every subsequent one."""
        task = dagbag.dags["volthive_build_warehouse"].get_task("release_advisory_lock")
        assert task.trigger_rule == "all_done"

    def test_the_partner_ingest_is_not_blocked_by_the_file_sensor(self, dagbag) -> None:
        """A missing OCPP file must not block an unrelated source."""
        dag = dagbag.dags["volthive_ingest_sessions"]
        partner = dag.get_task("ingest_partner_cdrs")
        assert "wait_for_cdr_partition" not in partner.upstream_task_ids

    def test_the_file_sensor_soft_fails_and_reschedules(self, dagbag) -> None:
        """soft_fail: a missing upstream file is an upstream incident, not a bug.

        reschedule: a poke-mode sensor would hold a worker slot for 45 minutes.
        """
        sensor = dagbag.dags["volthive_ingest_sessions"].get_task("wait_for_cdr_partition")
        assert sensor.soft_fail is True
        assert sensor.mode == "reschedule"
