from unittest.mock import MagicMock
from sgc_ma.submit_ma_batch import submit

def test_submit_dry_run_lists_only(monkeypatch):
    monkeypatch.setattr("sgc_ma.submit_ma_batch.select_cohorts",
                        lambda e, p, a, s: [{"dataset": "A"}, {"dataset": "B"}])
    batch = MagicMock()
    job = submit(engine=None, batch=batch, phenotype="ATOPIC_DERM", ancestry="EUR",
                 bucket="dig-data-registry", db_name="dataregistry", dry_run=True)
    assert job is None
    batch.submit_job.assert_not_called()

def test_submit_submits_job(monkeypatch):
    monkeypatch.setattr("sgc_ma.submit_ma_batch.select_cohorts",
                        lambda e, p, a, s: [{"dataset": "A", "file_id": "f1"},
                                        {"dataset": "B", "file_id": "f2"}])
    mock_query = MagicMock()
    monkeypatch.setattr("sgc_ma.submit_ma_batch.query", mock_query)

    calls = []
    mock_query.insert_sgc_ma_run.return_value = "run-abc"
    mock_query.insert_sgc_ma_run.side_effect = lambda *a, **kw: (calls.append("insert_run"), "run-abc")[1]

    batch = MagicMock()

    def _submit_job(**kw):
        calls.append("submit_job")
        return {"jobId": "job-123"}
    batch.submit_job.side_effect = _submit_job

    job = submit(engine=None, batch=batch, phenotype="ATOPIC_DERM", ancestry="EUR",
                 bucket="dig-data-registry", db_name="dataregistry", dry_run=False)
    assert job == "job-123"
    assert batch.submit_job.call_args.kwargs["parameters"]["run-id"] == "run-abc"

    # PENDING run row must be created before the Batch job is submitted
    assert calls == ["insert_run", "submit_job"]
    mock_query.insert_sgc_ma_run.assert_called_once_with(
        None, "ATOPIC_DERM", "EUR", sex="All", run_type="auto",
        dataset_file_ids=["f1", "f2"], maf_min=0.005, info_min=0.3)
    mock_query.update_sgc_ma_result.assert_called_once_with(
        None, "run-abc", status="PENDING", batch_job_id="job-123")
