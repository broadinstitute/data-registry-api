import pytest
from tests.conftest import db
from dataregistry.api import hcm_query as q


def test_insert_and_get_ma_run(api_client):
    e = db.get_engine()
    rid = q.insert_hcm_ma_run(e, label="core", dataset_file_ids=["a", "b"],
                              maf_min=0.005, info_min=0.3, submitted_by="reviewer")
    row = q.get_hcm_ma_run(e, rid)
    assert row["status"] == "PENDING"
    assert row["label"] == "core"
    assert row["dataset_file_ids"] == ["a", "b"]
    assert row["submitted_by"] == "reviewer"


def test_update_ma_result_partial_and_missing(api_client):
    e = db.get_engine()
    rid = q.insert_hcm_ma_run(e, label="x", dataset_file_ids=["a", "b"])
    q.update_hcm_ma_result(e, rid, status="SUCCEEDED", meta_lambda_gc=1.01,
                           n_meta_variants=42, manhattan_s3_key="hcm/ma/x/manhattan.png")
    row = q.get_hcm_ma_run(e, rid)
    assert row["status"] == "SUCCEEDED" and row["meta_lambda_gc"] == 1.01
    assert row["n_meta_variants"] == 42
    assert row["manhattan_s3_key"] == "hcm/ma/x/manhattan.png"
    with pytest.raises(ValueError):
        q.update_hcm_ma_result(e, "no-such-run", status="FAILED")


def test_get_ma_results_orders_desc(api_client):
    e = db.get_engine()
    r1 = q.insert_hcm_ma_run(e, label="first", dataset_file_ids=["a", "b"])
    r2 = q.insert_hcm_ma_run(e, label="second", dataset_file_ids=["c", "d"])
    ids = [r["id"] for r in q.get_hcm_ma_results(e)]
    assert set(ids) >= {r1, r2}
