from hcm_ma import submit_ma_batch as smb
from dataregistry.api import hcm_query


class FakeBatch:
    def __init__(self): self.kw = None
    def submit_job(self, **kw): self.kw = kw; return {"jobId": "job-xyz"}


def test_submit_run_uses_hcm_jobdef(monkeypatch):
    captured = {}
    monkeypatch.setattr(hcm_query, "update_hcm_ma_result",
                        lambda e, rid, **kw: captured.update(rid=rid, **kw))
    b = FakeBatch()
    job = smb.submit_run(engine=object(), batch=b, run_id="r1",
                         bucket="dig-data-registry", db_name="dataregistry_qa")
    assert job == "job-xyz"
    assert b.kw["jobDefinition"] == "hcm-gwas-ma-job"
    assert b.kw["parameters"] == {"run-id": "r1", "bucket": "dig-data-registry"}
    env = b.kw["containerOverrides"]["environment"]
    assert {"name": "DATA_REGISTRY_DB_NAME", "value": "dataregistry_qa"} in env
    assert captured["status"] == "PENDING" and captured["batch_job_id"] == "job-xyz"
