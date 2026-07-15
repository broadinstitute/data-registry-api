from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from dataregistry.server import app
from dataregistry.api.api import get_current_user

client = TestClient(app)


@pytest.fixture(autouse=True)
def _override_current_user():
    app.dependency_overrides[get_current_user] = lambda: SimpleNamespace(user_name="tester")
    yield
    app.dependency_overrides.pop(get_current_user, None)


@patch("dataregistry.api.qc.qc_runner.kick_off_qc_run", return_value="run999")
def test_start_qc_run_returns_run_id(kick):
    resp = client.post("/api/qc/run", json={
        "input_s3_path": "uploads/gwas.tsv",
        "pipeline": "default",
        "params": {"column_mapping": {"pvalue": "P"}},
    })
    assert resp.status_code == 200
    assert resp.json()["run_id"] == "run999"
    assert kick.call_args.kwargs.get("submitted_by") == "tester" or "tester" in kick.call_args.args


@patch("dataregistry.api.qc.s3.generate_presigned_url", return_value="https://signed")
@patch("dataregistry.api.qc.query.list_qc_step_results", return_value=[{"step": "row_check.py"}])
@patch("dataregistry.api.qc.query.get_qc_run_by_id")
def test_get_qc_run_presigns_outputs(get_run, list_steps, presign):
    get_run.return_value = {
        "id": "run999", "status": "COMPLETED", "overall_verdict": "warn",
        "gwas_filtered_s3_key": "qc/runs/run999/gwas_filtered.tsv",
        "qc_report_s3_key": "qc/runs/run999/qc_report.html",
    }
    resp = client.get("/api/qc/run/run999")
    assert resp.status_code == 200
    body = resp.json()
    assert body["run"]["gwas_filtered_url"] == "https://signed"
    assert body["run"]["qc_report_url"] == "https://signed"
    assert body["steps"][0]["step"] == "row_check.py"


@patch("dataregistry.api.qc.query.get_qc_run_by_id", return_value=None)
def test_get_qc_run_404(get_run):
    resp = client.get("/api/qc/run/nope")
    assert resp.status_code == 404
