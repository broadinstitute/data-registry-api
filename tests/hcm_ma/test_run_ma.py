import types
import hcm_ma.run_ma as rm
from dataregistry.api import hcm_query
from hcm_ma import select as sel


def test_worker_applies_adapter_and_writes_result(monkeypatch, tmp_path):
    run_row = {"id": "r1", "label": "core", "dataset_file_ids": ["a"],
               "maf_min": 0.005, "info_min": 0.3}
    monkeypatch.setattr(rm.query_mod, "get_hcm_ma_run", lambda e, rid: run_row)
    updates = []
    monkeypatch.setattr(rm.query_mod, "update_hcm_ma_result",
                        lambda e, rid, **kw: updates.append(kw))
    monkeypatch.setattr(rm, "DataRegistryReadWriteDB",
                        lambda: types.SimpleNamespace(get_engine=lambda: object()))
    monkeypatch.setattr(sel, "select_files_by_ids", lambda e, ids: [{
        "file_id": "a", "cohort": "MGB", "dataset": "MGB.gz", "s3_path": "hcm/gwas/MGB.gz",
        "column_mapping": {"chromosome": "CHR", "beta": "BETA"}, "cases": 1, "controls": 2}])

    seen = {}
    def fake_meta_analyze(cohorts, chunks_fn, outdir, label=None, ignored=None,
                          maf_min=None, info_min=None):
        seen["cm"] = cohorts[0]["column_mapping"]
        seen["label"] = label
        return {"meta_lambda_gc": 1.0, "n_meta_variants": 5, "n_genome_wide_sig": 1,
                "n_cohorts": 1, "n_cohorts_used": 1, "total_cases": 1, "total_controls": 2}
    monkeypatch.setattr(rm, "meta_analyze", fake_meta_analyze)

    class FakeS3:
        def download_file(self, *a, **k): pass
        def upload_file(self, *a, **k): pass
    monkeypatch.setattr(rm.boto3, "client", lambda *a, **k: FakeS3())

    rm.main.callback(run_id="r1", bucket="b", local_out=str(tmp_path))

    # adapter translated canonical keys -> col_* before meta_analyze
    assert seen["cm"] == {"col_chromosome": "CHR", "col_beta": "BETA"}
    assert seen["label"] == "core"
    assert updates[-1]["status"] == "SUCCEEDED"
