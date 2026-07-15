import uuid

from dataregistry.api import query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import QCRun

engine = DataRegistryReadWriteDB().get_engine()


def test_insert_and_get_qc_run():
    run = QCRun(
        input_s3_path="uploads/gwas.tsv",
        pipeline="default",
        pinned_commit="abc123",
        status="SUBMITTED",
        submitted_by="tester",
    )
    run_id = query.insert_qc_run(engine, run)
    assert isinstance(run_id, str) and len(run_id) == 32

    fetched = query.get_qc_run_by_id(engine, run_id)
    assert fetched is not None
    assert fetched["input_s3_path"] == "uploads/gwas.tsv"
    assert fetched["pipeline"] == "default"
    assert fetched["pinned_commit"] == "abc123"
    assert fetched["status"] == "SUBMITTED"
    assert fetched["submitted_by"] == "tester"
    assert fetched["id"] == run_id


def test_get_qc_run_missing_returns_none():
    assert query.get_qc_run_by_id(engine, uuid.uuid4().hex) is None


from dataregistry.api.model import QCStepResult


def test_insert_and_list_step_results():
    run_id = query.insert_qc_run(engine, QCRun(input_s3_path="in.tsv", pipeline="default"))
    query.insert_qc_step_result(engine, QCStepResult(
        run_id=run_id, step="row_check.py", verdict="warn",
        metrics={"total_rows": 5, "errors_found": 2},
        messages=["Dropped 2 rows"], artifacts=[],
        step_index=0,
    ))
    query.insert_qc_step_result(engine, QCStepResult(
        run_id=run_id, step="qc_report.py", verdict="pass",
        metrics={}, messages=[],
        artifacts=[{"type": "report", "path": "qc_report.html", "label": "QC Report"}],
        step_index=1,
    ))
    steps = query.list_qc_step_results(engine, run_id)
    assert [s["step"] for s in steps] == ["row_check.py", "qc_report.py"]
    assert steps[0]["run_id"] == run_id  # binary(32) id decoded, round-trips (not double-encoded)
    assert steps[0]["verdict"] == "warn"
    assert steps[0]["metrics"]["errors_found"] == 2  # JSON decoded, not a string
    assert steps[1]["artifacts"][0]["path"] == "qc_report.html"


def test_update_qc_run_status_sets_fields_and_completed_at():
    run_id = query.insert_qc_run(engine, QCRun(input_s3_path="in.tsv", pipeline="default"))
    query.update_qc_run_status(
        engine, run_id, "COMPLETED", overall_verdict="warn",
        gwas_filtered_s3_key="qc/runs/x/gwas_filtered.tsv",
        qc_report_s3_key="qc/runs/x/qc_report.html", batch_job_id="J1",
    )
    run = query.get_qc_run_by_id(engine, run_id)
    assert run["status"] == "COMPLETED"
    assert run["overall_verdict"] == "warn"
    assert run["gwas_filtered_s3_key"] == "qc/runs/x/gwas_filtered.tsv"
    assert run["qc_report_s3_key"] == "qc/runs/x/qc_report.html"
    assert run["batch_job_id"] == "J1"
    assert run["completed_at"] is not None
