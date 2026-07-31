import asyncio
import pytest
from fastapi import HTTPException
from dataregistry.api import hcm, hcm_query
from dataregistry.api.model import User, HCMMARunRequest


def make_user(review=True):
    return User(user_name="reviewer" if review else "uploader", first_name=None,
                last_name=None, email=None, avatar=None, is_active=True, roles=[],
                groups=None, permissions=["hcm-review-data"] if review else [],
                is_internal=True, api_token=None, id=1)


def run(coro):
    return asyncio.run(coro)


def test_eligible_files_requires_review(monkeypatch):
    monkeypatch.setattr(hcm.hcm_ma_select, "list_eligible_files", lambda e: [])
    with pytest.raises(HTTPException) as x:
        run(hcm.list_hcm_ma_eligible_files(user=make_user(review=False)))
    assert x.value.status_code == 403


def test_run_rejects_fewer_than_two_cohorts(monkeypatch):
    monkeypatch.setattr(hcm.hcm_ma_select, "list_eligible_files",
                        lambda e: [{"file_id": "a", "cohort_name": "MGB", "eligible": True}])
    req = HCMMARunRequest(file_ids=["a"])
    with pytest.raises(HTTPException) as x:
        run(hcm.launch_hcm_ma_run(req, user=make_user()))
    assert x.value.status_code == 400


def test_run_rejects_two_files_same_cohort(monkeypatch):
    monkeypatch.setattr(hcm.hcm_ma_select, "list_eligible_files", lambda e: [
        {"file_id": "a", "cohort_name": "MGB", "eligible": True},
        {"file_id": "b", "cohort_name": "MGB", "eligible": True}])
    req = HCMMARunRequest(file_ids=["a", "b"])
    with pytest.raises(HTTPException) as x:
        run(hcm.launch_hcm_ma_run(req, user=make_user()))
    assert x.value.status_code == 400
    assert "cohort" in x.value.detail.lower()


def test_run_rejects_ineligible_file(monkeypatch):
    monkeypatch.setattr(hcm.hcm_ma_select, "list_eligible_files", lambda e: [
        {"file_id": "a", "cohort_name": "MGB", "eligible": True},
        {"file_id": "b", "cohort_name": "HUNT", "eligible": False}])
    req = HCMMARunRequest(file_ids=["a", "b"])
    with pytest.raises(HTTPException) as x:
        run(hcm.launch_hcm_ma_run(req, user=make_user()))
    assert x.value.status_code == 400


def test_run_happy_path_creates_run(monkeypatch):
    monkeypatch.setattr(hcm.hcm_ma_select, "list_eligible_files", lambda e: [
        {"file_id": "a", "cohort_name": "MGB", "eligible": True},
        {"file_id": "b", "cohort_name": "HUNT", "eligible": True}])
    captured = {}
    def fake_insert(engine, **kw):
        captured.update(kw); return "run-1"
    monkeypatch.setattr(hcm_query, "insert_hcm_ma_run", fake_insert)
    monkeypatch.setattr(hcm.hcm_ma_submit, "submit_run", lambda **kw: "job-1")
    monkeypatch.setattr(hcm.boto3, "client", lambda *a, **k: object())
    req = HCMMARunRequest(file_ids=["a", "b"], label="core")
    out = run(hcm.launch_hcm_ma_run(req, user=make_user()))
    assert out == {"run_id": "run-1"}
    assert captured["dataset_file_ids"] == ["a", "b"]
    assert captured["submitted_by"] == "reviewer"
    assert captured["label"] == "core"
