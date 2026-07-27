"""Unit tests for the /sgc/ma/ignore* admin CRUD endpoints in dataregistry.api.sgc.

Mirrors test_ma_endpoints.py: these call the route functions directly (no
TestClient/app, no real DB) with query.* monkeypatched and a hand-built User.
"""
import asyncio
import pytest
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from dataregistry.api import sgc
from dataregistry.api import query
from dataregistry.api.model import User, MAIgnoreCreateRequest

IGNORE_ROW = {"id": "a" * 32, "cohort_id": "b" * 32, "phenotype": "PSOR",
              "ancestry": "EUR", "reason": "inflated", "excluded_by": "rev1",
              "created_at": None}


def make_user(with_review_perm: bool = True) -> User:
    return User(
        user_name="reviewer" if with_review_perm else "uploader",
        first_name=None, last_name=None, email=None, avatar=None,
        is_active=True, roles=[], groups=None,
        permissions=["sgc-review-data"] if with_review_perm else [],
        is_internal=True, api_token=None, id=1,
    )


def run(coro):
    return asyncio.run(coro)


def _req():
    return MAIgnoreCreateRequest(cohort_id="b" * 32, phenotype="PSOR",
                                 ancestry="EUR", reason="inflated")


def test_list_ignore_returns_rows(monkeypatch):
    monkeypatch.setattr(query, "list_ma_ignore", lambda engine: [IGNORE_ROW])
    assert run(sgc.list_sgc_ma_ignore(user=make_user())) == [IGNORE_ROW]


def test_list_ignore_no_permission_403(monkeypatch):
    monkeypatch.setattr(query, "list_ma_ignore", lambda engine: [IGNORE_ROW])
    with pytest.raises(HTTPException) as exc:
        run(sgc.list_sgc_ma_ignore(user=make_user(with_review_perm=False)))
    assert exc.value.status_code == 403


def test_add_ignore_creates_entry(monkeypatch):
    captured = {}
    def fake_insert(engine, cohort_id, phenotype, ancestry, reason, excluded_by):
        captured.update(dict(cohort_id=cohort_id, excluded_by=excluded_by))
        return IGNORE_ROW
    monkeypatch.setattr(query, "insert_ma_ignore", fake_insert)
    result = run(sgc.add_sgc_ma_ignore(req=_req(), user=make_user()))
    assert result == IGNORE_ROW
    assert captured["excluded_by"] == "reviewer"     # taken from the caller, not the body


def test_add_ignore_no_permission_403(monkeypatch):
    monkeypatch.setattr(query, "insert_ma_ignore", lambda *a, **k: IGNORE_ROW)
    with pytest.raises(HTTPException) as exc:
        run(sgc.add_sgc_ma_ignore(req=_req(), user=make_user(with_review_perm=False)))
    assert exc.value.status_code == 403


def test_add_ignore_unknown_cohort_400(monkeypatch):
    def boom(*a, **k):
        raise IntegrityError("insert", {}, Exception("FK fails"))
    monkeypatch.setattr(query, "insert_ma_ignore", boom)
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
