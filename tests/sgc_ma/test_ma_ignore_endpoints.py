"""Unit tests for the /sgc/ma/ignore* admin CRUD endpoints in dataregistry.api.sgc.

Mirrors test_ma_endpoints.py: these call the route functions directly (no
TestClient/app, no real DB) with query.* monkeypatched and a hand-built User.
"""
import asyncio
import pytest
from fastapi import HTTPException

from dataregistry.api import sgc
from dataregistry.api import query
from dataregistry.api.model import User, MAIgnoreCreateRequest

IGNORE_ROW = {"id": "a" * 32, "file_id": "f" * 32, "cohort_id": "b" * 32, "cohort": "C",
              "dataset": "D", "phenotype": "PSOR", "ancestry": "EUR", "sex": "All",
              "reason": "Phenotyping error", "excluded_by": "rev1", "created_at": None}


def make_user(with_review_perm: bool = True, user_name: str | None = None,
              permissions: list | None = None) -> User:
    return User(
        user_name=user_name or ("reviewer" if with_review_perm else "uploader"),
        first_name=None, last_name=None, email=None, avatar=None,
        is_active=True, roles=[], groups=None,
        permissions=permissions if permissions is not None
        else (["sgc-review-data"] if with_review_perm else []),
        is_internal=True, api_token=None, id=1,
    )


def run(coro):
    return asyncio.run(coro)


def _req():
    return MAIgnoreCreateRequest(file_id="f" * 32, reason="Phenotyping error")


def test_list_ignore_returns_rows(monkeypatch):
    monkeypatch.setattr(query, "list_ma_ignore", lambda engine: [IGNORE_ROW])
    assert run(sgc.list_sgc_ma_ignore(user=make_user())) == [IGNORE_ROW]


def test_list_ignore_no_permission_403(monkeypatch):
    monkeypatch.setattr(query, "list_ma_ignore", lambda engine: [IGNORE_ROW])
    with pytest.raises(HTTPException) as exc:
        run(sgc.list_sgc_ma_ignore(user=make_user(with_review_perm=False)))
    assert exc.value.status_code == 403


def test_add_ignore_creates_entry(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_gwas_file_by_id", lambda engine, fid: {"id": fid})
    captured = {}
    def fake_insert(engine, file_id, reason, excluded_by):
        captured.update(file_id=file_id, excluded_by=excluded_by); return IGNORE_ROW
    monkeypatch.setattr(query, "insert_ma_ignore", fake_insert)
    result = run(sgc.add_sgc_ma_ignore(req=_req(), user=make_user()))
    assert result == IGNORE_ROW and captured["excluded_by"] == "reviewer"


def test_add_ignore_no_permission_403(monkeypatch):
    monkeypatch.setattr(query, "insert_ma_ignore", lambda *a, **k: IGNORE_ROW)
    with pytest.raises(HTTPException) as exc:
        run(sgc.add_sgc_ma_ignore(req=_req(), user=make_user(with_review_perm=False)))
    assert exc.value.status_code == 403


def test_add_ignore_unknown_file_400(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_gwas_file_by_id", lambda engine, fid: None)
    with pytest.raises(HTTPException) as exc:
        run(sgc.add_sgc_ma_ignore(req=_req(), user=make_user()))
    assert exc.value.status_code == 400


def test_delete_ignore_ok(monkeypatch):
    monkeypatch.setattr(query, "delete_ma_ignore", lambda engine, ignore_id: True)
    result = run(sgc.delete_sgc_ma_ignore(ignore_id="a" * 32, user=make_user()))
    assert "deleted" in result["message"]


def test_delete_ignore_missing_404(monkeypatch):
    monkeypatch.setattr(query, "delete_ma_ignore", lambda engine, ignore_id: False)
    with pytest.raises(HTTPException) as exc:
        run(sgc.delete_sgc_ma_ignore(ignore_id="a" * 32, user=make_user()))
    assert exc.value.status_code == 404


# --- GET /sgc/cohorts/{cohort_id}/ma-ignore (cohort-scoped, owner-visible) ---

COHORT_ID = "b" * 32


def _patch_cohort(monkeypatch, owner="uploader"):
    monkeypatch.setattr(query, "get_sgc_cohort_by_id",
                        lambda engine, cid: [{"id": cid, "uploaded_by": owner}])
    monkeypatch.setattr(query, "list_ma_ignore_for_cohort",
                        lambda engine, cid: [IGNORE_ROW], raising=False)


def test_cohort_ignore_owner_sees_own_rows(monkeypatch):
    _patch_cohort(monkeypatch, owner="uploader")
    result = run(sgc.list_sgc_cohort_ma_ignore(
        cohort_id=COHORT_ID, user=make_user(user_name="uploader", permissions=[])))
    assert result == [IGNORE_ROW]


def test_cohort_ignore_reviewer_sees_any_cohort(monkeypatch):
    _patch_cohort(monkeypatch, owner="someone-else")
    result = run(sgc.list_sgc_cohort_ma_ignore(
        cohort_id=COHORT_ID, user=make_user(user_name="reviewer", permissions=["sgc-review-data"])))
    assert result == [IGNORE_ROW]


def test_cohort_ignore_ma_reviewer_sees_any_cohort(monkeypatch):
    _patch_cohort(monkeypatch, owner="someone-else")
    result = run(sgc.list_sgc_cohort_ma_ignore(
        cohort_id=COHORT_ID, user=make_user(user_name="ma-rev", permissions=["sgc-review-ma"])))
    assert result == [IGNORE_ROW]


def test_cohort_ignore_non_owner_403(monkeypatch):
    _patch_cohort(monkeypatch, owner="someone-else")
    with pytest.raises(HTTPException) as exc:
        run(sgc.list_sgc_cohort_ma_ignore(
            cohort_id=COHORT_ID, user=make_user(user_name="uploader", permissions=[])))
    assert exc.value.status_code == 403


def test_cohort_ignore_unknown_cohort_404(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_cohort_by_id", lambda engine, cid: [])
    with pytest.raises(HTTPException) as exc:
        run(sgc.list_sgc_cohort_ma_ignore(cohort_id=COHORT_ID, user=make_user()))
    assert exc.value.status_code == 404
