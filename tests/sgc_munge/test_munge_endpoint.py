import asyncio

import pytest
from fastapi import HTTPException

from dataregistry.api import sgc, query
from tests.sgc_ma.test_ma_endpoints import make_user


def test_gwas_summary_sql_exposes_munge_status():
    from dataregistry.api.query import _ALL_SGC_GWAS_FILES_SQL
    assert "FROM sgc_gwas_munge_results mu" in _ALL_SGC_GWAS_FILES_SQL and "AS munge_status" in _ALL_SGC_GWAS_FILES_SQL


def test_get_munge_requires_review(monkeypatch):
    with pytest.raises(HTTPException) as e:
        asyncio.run(sgc.get_sgc_munge_result("f" * 32, user=make_user(False)))
    assert e.value.status_code == 403


def test_get_munge_404(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_munge_result", lambda eng, fid: None)
    with pytest.raises(HTTPException) as e:
        asyncio.run(sgc.get_sgc_munge_result("f" * 32, user=make_user()))
    assert e.value.status_code == 404


def test_get_munge_presigns_only_present_keys(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_munge_result", lambda eng, fid: {"id": "a" * 32, "file_id": fid, "status": "SUCCEEDED",
        "standardised_s3_key": "k1", "munge_json_s3_key": "k2", "ref_mismatch_s3_key": None, "n_dropped": {"maf": 1}})
    monkeypatch.setattr(sgc, "_qc_plots_presign", lambda k: f"https://x/{k}")
    r = asyncio.run(sgc.get_sgc_munge_result("f" * 32, user=make_user()))
    assert r.standardised_url == "https://x/k1" and r.ref_mismatch_url is None and r.n_dropped == {"maf": 1}
