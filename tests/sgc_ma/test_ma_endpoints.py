"""Unit tests for the /sgc/ma/results* endpoints in dataregistry.api.sgc.

These call the route functions directly (no TestClient/app, no real DB) because
tests/conftest.py's DB hook requires a reachable MySQL instance with a single
alembic head; this worktree currently has two heads (the untracked qc_run
migration files plus this branch's create_sgc_gwas_ma_results migration), so
the standard api_client fixture path can't be used here. query.get_sgc_ma_results
and boto3/_qc_plots_presign are monkeypatched instead.
"""
import asyncio
from io import BytesIO

import pytest
from fastapi import HTTPException

from dataregistry.api import sgc, query
from dataregistry.api.model import User


def make_user(with_review_perm: bool = True) -> User:
    return User(
        user_name="reviewer" if with_review_perm else "uploader",
        first_name=None,
        last_name=None,
        email=None,
        avatar=None,
        is_active=True,
        roles=[],
        groups=None,
        permissions=["sgc-review-data"] if with_review_perm else [],
        is_internal=True,
        api_token=None,
        id=1,
    )


def run(coro):
    return asyncio.run(coro)


MA_ROW = {
    "id": "abc123",
    "phenotype": "ATOPIC_DERM",
    "ancestry": "EUR",
    "status": "SUCCEEDED",
    "meta_lambda_gc": 1.02,
    "n_meta_variants": 1000,
    "n_genome_wide_sig": 5,
    "n_cohorts": 3,
    "n_cohorts_used": 3,
    "manhattan_s3_key": "ma/ATOPIC_DERM/EUR/manhattan.png",
    "qq_s3_key": "ma/ATOPIC_DERM/EUR/qq.png",
    "meta_s3_key": "ma/ATOPIC_DERM/EUR/meta.tsv.gz",
    "summary_json_s3_key": "ma/ATOPIC_DERM/EUR/summary.json",
    "summary_tsv_s3_key": "ma/ATOPIC_DERM/EUR/summary.tsv",
    "top_loci_s3_key": "ma/ATOPIC_DERM/EUR/top_loci.tsv",
    "batch_job_id": "job-1",
    "error_message": None,
    "created_at": None,
    "updated_at": None,
}


def test_list_sgc_ma_results_returns_rows(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_ma_results", lambda engine: [MA_ROW])
    result = run(sgc.list_sgc_ma_results(user=make_user()))
    assert result == [MA_ROW]


def test_list_sgc_ma_results_no_permission_403(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_ma_results", lambda engine: [MA_ROW])
    with pytest.raises(HTTPException) as exc_info:
        run(sgc.list_sgc_ma_results(user=make_user(with_review_perm=False)))
    assert exc_info.value.status_code == 403


def test_ma_lookup_not_found_404(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_ma_results", lambda engine: [])
    with pytest.raises(HTTPException) as exc_info:
        sgc._ma_lookup("NOPE", "EUR")
    assert exc_info.value.status_code == 404


@pytest.mark.parametrize("route_fn,key", [
    (sgc.get_ma_manhattan, "manhattan_s3_key"),
    (sgc.get_ma_qq, "qq_s3_key"),
    (sgc.get_ma_meta, "meta_s3_key"),
])
def test_ma_plot_routes_return_presigned_url(monkeypatch, route_fn, key):
    monkeypatch.setattr(query, "get_sgc_ma_results", lambda engine: [MA_ROW])
    monkeypatch.setattr(sgc, "_qc_plots_presign", lambda s3_key: f"https://presigned/{s3_key}")
    result = run(route_fn("ATOPIC_DERM", "EUR", user=make_user()))
    assert result == {"url": f"https://presigned/{MA_ROW[key]}"}


@pytest.mark.parametrize("route_fn", [sgc.get_ma_manhattan, sgc.get_ma_qq, sgc.get_ma_meta])
def test_ma_plot_routes_no_permission_403(monkeypatch, route_fn):
    monkeypatch.setattr(query, "get_sgc_ma_results", lambda engine: [MA_ROW])
    with pytest.raises(HTTPException) as exc_info:
        run(route_fn("ATOPIC_DERM", "EUR", user=make_user(with_review_perm=False)))
    assert exc_info.value.status_code == 403


def test_get_ma_summary_streams_json_from_s3(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_ma_results", lambda engine: [MA_ROW])
    mock_s3 = type("S3", (), {})()
    body = b'{"n_cohorts": 3, "lead_snps": 5}'
    mock_s3.get_object = lambda Bucket, Key: {"Body": BytesIO(body)}
    monkeypatch.setattr(sgc.boto3, "client", lambda *a, **kw: mock_s3)

    response = run(sgc.get_ma_summary("ATOPIC_DERM", "EUR", user=make_user()))
    assert response.body == body
    assert response.media_type == "application/json"


def test_get_ma_summary_no_permission_403(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_ma_results", lambda engine: [MA_ROW])
    with pytest.raises(HTTPException) as exc_info:
        run(sgc.get_ma_summary("ATOPIC_DERM", "EUR", user=make_user(with_review_perm=False)))
    assert exc_info.value.status_code == 403


def test_get_ma_top_loci_parses_tsv_to_rows(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_ma_results", lambda engine: [MA_ROW])
    tsv = "chrom\tpos\trsid\tp_value\n1\t12345\trs1\t1e-9\n2\t67890\trs2\t5e-10\n"
    mock_s3 = type("S3", (), {})()
    mock_s3.get_object = lambda Bucket, Key: {"Body": BytesIO(tsv.encode())}
    monkeypatch.setattr(sgc.boto3, "client", lambda *a, **kw: mock_s3)

    result = run(sgc.get_ma_top_loci("ATOPIC_DERM", "EUR", user=make_user()))
    assert result == [
        {"chrom": "1", "pos": "12345", "rsid": "rs1", "p_value": "1e-9"},
        {"chrom": "2", "pos": "67890", "rsid": "rs2", "p_value": "5e-10"},
    ]


def test_get_ma_top_loci_no_permission_403(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_ma_results", lambda engine: [MA_ROW])
    with pytest.raises(HTTPException) as exc_info:
        run(sgc.get_ma_top_loci("ATOPIC_DERM", "EUR", user=make_user(with_review_perm=False)))
    assert exc_info.value.status_code == 403
