from unittest.mock import MagicMock
from sgc_ma.submit_ma_batch import submit

def test_submit_dry_run_lists_only(monkeypatch):
    monkeypatch.setattr("sgc_ma.submit_ma_batch.select_cohorts",
                        lambda e, p, a: [{"dataset": "A"}, {"dataset": "B"}])
    batch = MagicMock()
    job = submit(engine=None, batch=batch, phenotype="ATOPIC_DERM", ancestry="EUR",
                 bucket="dig-data-registry", db_name="dataregistry", dry_run=True)
    assert job is None
    batch.submit_job.assert_not_called()

def test_submit_submits_job(monkeypatch):
    monkeypatch.setattr("sgc_ma.submit_ma_batch.select_cohorts",
                        lambda e, p, a: [{"dataset": "A"}, {"dataset": "B"}, {"dataset": "C"}])
    mock_query = MagicMock()
    monkeypatch.setattr("sgc_ma.submit_ma_batch.query", mock_query)

    calls = []
    mock_query.insert_sgc_ma_pending.side_effect = lambda *a, **kw: calls.append("insert_pending")

    batch = MagicMock()

    def _submit_job(**kw):
        calls.append("submit_job")
        return {"jobId": "job-123"}
    batch.submit_job.side_effect = _submit_job

    job = submit(engine=None, batch=batch, phenotype="ATOPIC_DERM", ancestry="EUR",
                 bucket="dig-data-registry", db_name="dataregistry", dry_run=False)
    assert job == "job-123"
    assert batch.submit_job.call_args.kwargs["parameters"]["phenotype"] == "ATOPIC_DERM"

    # PENDING row must be seeded before the Batch job is submitted
    assert calls == ["insert_pending", "submit_job"]
    mock_query.insert_sgc_ma_pending.assert_called_once_with(None, "ATOPIC_DERM", "EUR")
    mock_query.update_sgc_ma_result.assert_called_once_with(
        None, "ATOPIC_DERM", "EUR", status="PENDING", batch_job_id="job-123")
