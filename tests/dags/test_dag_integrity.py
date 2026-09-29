"""DAG integrity tests. Run inside the Airflow image: `make test-dags`."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), known_pools={"default_pool", "spark"})


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_are_present(dagbag):
    assert set(dagbag.dags) == {"claims_lakehouse", "denials_worklist", "lakehouse_maintenance"}


@pytest.mark.parametrize(
    "dag_id", ["claims_lakehouse", "denials_worklist", "lakehouse_maintenance"]
)
def test_dags_follow_platform_conventions(dagbag, dag_id):
    dag = dagbag.dags.get(dag_id)
    assert dag.tags, "every DAG is tagged"
    assert dag.doc_md, "every DAG is documented"
    assert not dag.catchup, "backfills are explicit, never implicit"
    for t in dag.tasks:
        assert t.owner != "airflow", f"{t.task_id} needs a real owner"
        assert t.retries >= 1, f"{t.task_id} must retry transient failures"


def test_claims_lakehouse_topology(dagbag):
    dag = dagbag.dags.get("claims_lakehouse")
    assert dag.max_active_runs == 1
    silver_tasks = {t.task_id for t in dag.tasks if t.task_id.startswith("conform_silver.")}
    assert len(silver_tasks) == 6
    assert silver_tasks <= dag.get_task("quality_gate").upstream_task_ids
    assert "quality_gate" in dag.get_task("build_gold").upstream_task_ids
    orchestration = {"resolve_window", "extract_fhir_bulk_export"}
    spark_tasks = [t for t in dag.tasks if t.task_id not in orchestration]
    assert all(t.pool == "spark" for t in spark_tasks), "Spark sessions are bounded by a pool"
