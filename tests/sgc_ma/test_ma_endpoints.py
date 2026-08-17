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
    "label": None,
    "run_type": "auto",
    "dataset_file_ids": ["a", "b"],
    "maf_min": 0.005,
    "info_min": 0.3,
    "submitted_by": None,
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
    monkeypatch.setattr(query, "get_sgc_ma_run", lambda engine, run_id: None)
    with pytest.raises(HTTPException) as exc_info:
        sgc._ma_run_lookup("nope")
    assert exc_info.value.status_code == 404


@pytest.mark.parametrize("route_fn,key", [
    (sgc.get_ma_manhattan, "manhattan_s3_key"),
    (sgc.get_ma_qq, "qq_s3_key"),
    (sgc.get_ma_meta, "meta_s3_key"),
])
def test_ma_plot_routes_return_presigned_url(monkeypatch, route_fn, key):
    monkeypatch.setattr(query, "get_sgc_ma_run", lambda engine, run_id: MA_ROW)
    monkeypatch.setattr(sgc, "_qc_plots_presign", lambda s3_key: f"https://presigned/{s3_key}")
    result = run(route_fn("run-abc", user=make_user()))
    assert result == {"url": f"https://presigned/{MA_ROW[key]}"}


@pytest.mark.parametrize("route_fn", [sgc.get_ma_manhattan, sgc.get_ma_qq, sgc.get_ma_meta])
def test_ma_plot_routes_no_permission_403(monkeypatch, route_fn):
    monkeypatch.setattr(query, "get_sgc_ma_run", lambda engine, run_id: MA_ROW)
    with pytest.raises(HTTPException) as exc_info:
        run(route_fn("run-abc", user=make_user(with_review_perm=False)))
    assert exc_info.value.status_code == 403


def test_get_ma_summary_streams_json_from_s3(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_ma_run", lambda engine, run_id: MA_ROW)
    mock_s3 = type("S3", (), {})()
    body = b'{"n_cohorts": 3, "lead_snps": 5}'
    mock_s3.get_object = lambda Bucket, Key: {"Body": BytesIO(body)}
    monkeypatch.setattr(sgc.boto3, "client", lambda *a, **kw: mock_s3)

    response = run(sgc.get_ma_summary("run-abc", user=make_user()))
    assert response.body == body
    assert response.media_type == "application/json"


def test_get_ma_summary_no_permission_403(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_ma_run", lambda engine, run_id: MA_ROW)
    with pytest.raises(HTTPException) as exc_info:
        run(sgc.get_ma_summary("run-abc", user=make_user(with_review_perm=False)))
    assert exc_info.value.status_code == 403


def test_get_ma_top_loci_parses_tsv_to_rows(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_ma_run", lambda engine, run_id: MA_ROW)
    tsv = "chrom\tpos\trsid\tp_value\n1\t12345\trs1\t1e-9\n2\t67890\trs2\t5e-10\n"
    mock_s3 = type("S3", (), {})()
    mock_s3.get_object = lambda Bucket, Key: {"Body": BytesIO(tsv.encode())}
    monkeypatch.setattr(sgc.boto3, "client", lambda *a, **kw: mock_s3)

    result = run(sgc.get_ma_top_loci("run-abc", user=make_user()))
    assert result == [
        {"chrom": "1", "pos": "12345", "rsid": "rs1", "p_value": "1e-9"},
        {"chrom": "2", "pos": "67890", "rsid": "rs2", "p_value": "5e-10"},
    ]


def test_get_ma_top_loci_no_permission_403(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_ma_run", lambda engine, run_id: MA_ROW)
    with pytest.raises(HTTPException) as exc_info:
        run(sgc.get_ma_top_loci("run-abc", user=make_user(with_review_perm=False)))
    assert exc_info.value.status_code == 403


def test_list_ma_candidates_endpoint_returns_rows(monkeypatch):
    import sgc_ma.select as sel
    monkeypatch.setattr(sel, "list_ma_candidates",
                        lambda engine, p, a, s: [{"file_id": "1", "cohort": "C"}])
    result = run(sgc.list_ma_candidates_endpoint("PH", "EUR", user=make_user()))
    assert result == [{"file_id": "1", "cohort": "C"}]


def test_list_ma_candidates_endpoint_no_permission_403(monkeypatch):
    with pytest.raises(HTTPException) as e:
        run(sgc.list_ma_candidates_endpoint("PH", "EUR", user=make_user(with_review_perm=False)))
    assert e.value.status_code == 403


def test_launch_sgc_ma_run_requires_two_files():
    from dataregistry.api.model import MARunRequest
    req = MARunRequest(phenotype="PH", ancestry="EUR", file_ids=["only-one"])
    with pytest.raises(HTTPException) as e:
        run(sgc.launch_sgc_ma_run(req, user=make_user()))
    assert e.value.status_code == 400


def test_launch_sgc_ma_run_no_permission_403():
    from dataregistry.api.model import MARunRequest
    req = MARunRequest(phenotype="PH", ancestry="EUR", file_ids=["a", "b"])
    with pytest.raises(HTTPException) as e:
        run(sgc.launch_sgc_ma_run(req, user=make_user(with_review_perm=False)))
    assert e.value.status_code == 403


def test_launch_sgc_ma_run_invalid_bucket_400():
    from dataregistry.api.model import MARunRequest
    # sex="Male" with a non-Combined ancestry is not one of the nine valid
    # buckets. Two file_ids so the "needs two files" guard doesn't fire
    # first -- this test proves the bucket guard itself yields 400.
    req = MARunRequest(phenotype="PH", ancestry="AFR", sex="Male", file_ids=["a", "b"])
    with pytest.raises(HTTPException) as e:
        run(sgc.launch_sgc_ma_run(req, user=make_user()))
    assert e.value.status_code == 400


def test_launch_sgc_ma_run_creates_manual_run(monkeypatch):
    from dataregistry.api.model import MARunRequest
    import sgc_ma.submit_ma_batch as smb
    captured = {}

    def fake_insert(engine, phenotype, ancestry, **kw):
        captured.update(dict(phenotype=phenotype, ancestry=ancestry, **kw))
        return "run-xyz"
    monkeypatch.setattr(query, "insert_sgc_ma_run", fake_insert)
    monkeypatch.setattr(smb, "submit_run", lambda **kw: "job-1")
    monkeypatch.setattr(sgc.boto3, "client", lambda *a, **kw: object())

    req = MARunRequest(phenotype="PH", ancestry="EUR", file_ids=["a", "b"],
                       maf_min=0.01, info_min=0.4, label="core set")
    result = run(sgc.launch_sgc_ma_run(req, user=make_user()))
    assert result == {"run_id": "run-xyz"}
    assert captured["run_type"] == "manual"
    assert captured["dataset_file_ids"] == ["a", "b"]
    assert captured["submitted_by"] == "reviewer"        # make_user() default user_name
    assert captured["maf_min"] == 0.01 and captured["info_min"] == 0.4
    assert captured["label"] == "core set"


# --- DELETE /sgc/ma/runs/{run_id} ------------------------------------------------

# A run written under the current layout: every artifact lives beneath a prefix ending
# in the run id, so the whole prefix is safe to enumerate and delete.
SCOPED_RUN_ID = "d89831dada85428e86a8d0007362466c"
SCOPED_PREFIX = f"sgc/ma/SUBSTANCE_DERM/Combined/{SCOPED_RUN_ID}"
SCOPED_MA_ROW = {
    **MA_ROW,
    "id": SCOPED_RUN_ID,
    "manhattan_s3_key": f"{SCOPED_PREFIX}/manhattan.png",
    "qq_s3_key": f"{SCOPED_PREFIX}/qq.png",
    "meta_s3_key": f"{SCOPED_PREFIX}/meta.tsv.gz",
    "summary_json_s3_key": f"{SCOPED_PREFIX}/summary.json",
    "summary_tsv_s3_key": f"{SCOPED_PREFIX}/summary.tsv",
    "top_loci_s3_key": f"{SCOPED_PREFIX}/top_loci.tsv",
}


class FakeS3:
    def __init__(self, listing=()):
        self.listing = list(listing)
        self.deleted = []
        self.listed_prefixes = []

    def get_paginator(self, _name):
        outer = self

        class P:
            def paginate(self, Bucket, Prefix):
                outer.listed_prefixes.append(Prefix)
                return [{"Contents": [{"Key": k} for k in outer.listing
                                      if k.startswith(Prefix)]}]
        return P()

    def delete_objects(self, Bucket, Delete):
        self.deleted.extend(o["Key"] for o in Delete["Objects"])


class FakeBatch:
    def __init__(self):
        self.terminated = []

    def terminate_job(self, jobId, reason):
        self.terminated.append((jobId, reason))


def patch_aws(monkeypatch, s3_client, batch_client):
    """Route sgc.boto3.client() to the right double by service name."""
    monkeypatch.setattr(sgc.boto3, "client",
                        lambda service, **kw: batch_client if service == "batch" else s3_client)


def test_delete_ma_run_removes_whole_run_prefix(monkeypatch):
    """Sidecars no column records (e.g. summary.json.pre-backfill) go too."""
    sidecar = f"{SCOPED_PREFIX}/summary.json.pre-backfill"
    fake_s3 = FakeS3(listing=[SCOPED_MA_ROW["manhattan_s3_key"], sidecar])
    fake_batch = FakeBatch()
    patch_aws(monkeypatch, fake_s3, fake_batch)
    monkeypatch.setattr(query, "get_sgc_ma_run", lambda engine, run_id: SCOPED_MA_ROW)
    deleted_rows = []
    monkeypatch.setattr(query, "delete_sgc_ma_run",
                        lambda engine, run_id: deleted_rows.append(run_id) or True)

    result = run(sgc.delete_ma_run(SCOPED_RUN_ID, user=make_user()))

    assert sidecar in fake_s3.deleted
    assert deleted_rows == [SCOPED_RUN_ID]
    assert result["run_id"] == SCOPED_RUN_ID
    assert result["objects_deleted"] == len(fake_s3.deleted)
    assert fake_batch.terminated == []          # SUCCEEDED run: nothing to terminate


def test_delete_ma_run_legacy_row_never_lists_shared_prefix(monkeypatch):
    """Pre-multi-run rows share `.../{phenotype}/{ancestry}/` with other runs.

    Deleting that prefix would take a sibling run's results with it, so only the keys
    the row itself records may be deleted.
    """
    sibling = "ma/ATOPIC_DERM/EUR/other-run-id/meta.tsv.gz"
    fake_s3 = FakeS3(listing=[sibling])
    patch_aws(monkeypatch, fake_s3, FakeBatch())
    monkeypatch.setattr(query, "get_sgc_ma_run", lambda engine, run_id: MA_ROW)
    monkeypatch.setattr(query, "delete_sgc_ma_run", lambda engine, run_id: True)

    run(sgc.delete_ma_run("abc123", user=make_user()))

    assert fake_s3.listed_prefixes == []
    assert sibling not in fake_s3.deleted
    assert set(fake_s3.deleted) == {MA_ROW[c] for c in sgc.MA_ARTIFACT_KEY_COLUMNS}


def test_delete_ma_run_terminates_an_in_flight_batch_job(monkeypatch):
    fake_s3, fake_batch = FakeS3(), FakeBatch()
    patch_aws(monkeypatch, fake_s3, fake_batch)
    running = {**SCOPED_MA_ROW, "status": "RUNNING", "batch_job_id": "job-live"}
    monkeypatch.setattr(query, "get_sgc_ma_run", lambda engine, run_id: running)
    monkeypatch.setattr(query, "delete_sgc_ma_run", lambda engine, run_id: True)

    run(sgc.delete_ma_run(SCOPED_RUN_ID, user=make_user()))

    assert [j for j, _ in fake_batch.terminated] == ["job-live"]


def test_delete_ma_run_survives_a_failed_termination(monkeypatch):
    """A job that already exited must not block the delete."""
    from botocore.exceptions import ClientError
    fake_s3 = FakeS3()

    class ExplodingBatch:
        def terminate_job(self, jobId, reason):
            raise ClientError({"Error": {"Code": "ClientException"}}, "TerminateJob")
    patch_aws(monkeypatch, fake_s3, ExplodingBatch())
    running = {**SCOPED_MA_ROW, "status": "RUNNING", "batch_job_id": "job-gone"}
    monkeypatch.setattr(query, "get_sgc_ma_run", lambda engine, run_id: running)
    deleted_rows = []
    monkeypatch.setattr(query, "delete_sgc_ma_run",
                        lambda engine, run_id: deleted_rows.append(run_id) or True)

    run(sgc.delete_ma_run(SCOPED_RUN_ID, user=make_user()))

    assert deleted_rows == [SCOPED_RUN_ID]


def test_delete_ma_run_unknown_id_404(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_ma_run", lambda engine, run_id: None)
    with pytest.raises(HTTPException) as exc_info:
        run(sgc.delete_ma_run("nope", user=make_user()))
    assert exc_info.value.status_code == 404


def test_delete_ma_run_no_permission_403(monkeypatch):
    """Permission is checked before anything is looked up or deleted."""
    def explode(*a, **kw):
        raise AssertionError("must not touch the DB without permission")
    monkeypatch.setattr(query, "get_sgc_ma_run", explode)
    monkeypatch.setattr(query, "delete_sgc_ma_run", explode)
    with pytest.raises(HTTPException) as exc_info:
        run(sgc.delete_ma_run("run-abc", user=make_user(with_review_perm=False)))
    assert exc_info.value.status_code == 403
