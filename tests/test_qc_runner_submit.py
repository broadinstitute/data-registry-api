import json
import uuid
from unittest.mock import MagicMock, patch

from dataregistry.api import qc_runner


@patch("dataregistry.api.qc_runner.query")
@patch("dataregistry.api.qc_runner.boto3")
def test_submit_sends_wrapper_flags_and_ingests(boto3_mock, query_mock):
    batch = MagicMock()
    s3c = MagicMock()
    boto3_mock.client.side_effect = lambda svc, **kw: batch if svc == "batch" else s3c
    batch.submit_job.return_value = {"jobId": "JOB1"}
    batch.describe_jobs.return_value = {"jobs": [{"status": "SUCCEEDED"}]}
    s3c.get_object.return_value = {"Body": MagicMock(read=lambda: json.dumps({
        "status": "completed",
        "overall_verdict": "warn",
        "steps": [
            {"step": "row_check.py", "verdict": "warn", "metrics": {"errors_found": 2},
             "messages": ["Dropped 2 rows"], "artifacts": []},
            {"step": "qc_report.py", "verdict": "pass", "metrics": {}, "messages": [],
             "artifacts": [{"type": "report", "path": "qc_report.html", "label": "QC Report"}]},
        ],
        "outputs": {"gwas_filtered": "gwas_filtered.tsv", "qc_report": "qc_report.html"},
    }).encode())}

    run_id = uuid.uuid4().hex
    qc_runner._submit_and_await(
        engine=MagicMock(), run_id=run_id, input_s3_path="uploads/gwas.tsv",
        pipeline="default", params={"column_mapping": {"pvalue": "P"}}, commit="deadbeef",
    )

    # submitted with exactly the wrapper's six flags
    params = batch.submit_job.call_args.kwargs["parameters"]
    assert set(params) == {"input-s3-uri", "output-s3-prefix", "pipeline",
                           "params-json", "repo-url", "repo-commit"}
    assert params["input-s3-uri"].endswith("/uploads/gwas.tsv")
    assert params["output-s3-prefix"].endswith(f"/qc/runs/{run_id}")
    assert params["pipeline"] == "default"
    assert json.loads(params["params-json"]) == {"column_mapping": {"pvalue": "P"}}
    assert params["repo-commit"] == "deadbeef"

    # ingested: 2 step rows + a COMPLETED run with verdict + output keys
    assert query_mock.insert_qc_step_result.call_count == 2
    final = [c for c in query_mock.update_qc_run_status.call_args_list
             if c.args[2] == "COMPLETED"]
    assert final, "run should be marked COMPLETED"
    kwargs = final[-1].kwargs
    assert kwargs["overall_verdict"] == "warn"
    assert kwargs["gwas_filtered_s3_key"] == f"qc/runs/{run_id}/gwas_filtered.tsv"
    assert kwargs["qc_report_s3_key"] == f"qc/runs/{run_id}/qc_report.html"


@patch("dataregistry.api.qc_runner.query")
@patch("dataregistry.api.qc_runner.boto3")
def test_missing_result_json_marks_failed(boto3_mock, query_mock):
    batch = MagicMock()
    s3c = MagicMock()
    boto3_mock.client.side_effect = lambda svc, **kw: batch if svc == "batch" else s3c
    batch.submit_job.return_value = {"jobId": "JOB1"}
    batch.describe_jobs.return_value = {"jobs": [{"status": "FAILED"}]}
    s3c.get_object.side_effect = Exception("NoSuchKey")

    run_id = uuid.uuid4().hex
    qc_runner._submit_and_await(
        engine=MagicMock(), run_id=run_id, input_s3_path="in.tsv",
        pipeline="default", params={}, commit="c",
    )
    failed = [c for c in query_mock.update_qc_run_status.call_args_list
              if c.args[2] == "FAILED"]
    assert failed, "missing result.json should mark the run FAILED"
