import asyncio
import pytest
from fastapi import HTTPException

from dataregistry.api import sgc, query
from dataregistry.api.model import User

JOB = {"id": "a" * 32, "file_id": "f" * 32, "source_genome_build": "hg19",
       "target_genome_build": "hg38", "batch_job_id": None, "status": "SUCCEEDED",
       "submitted_at": None, "completed_at": None, "submitted_by": "system",
       "unmapped_s3_path": "s3://dig-data-registry-qa/sgc/liftover/fff/unmapped.tsv",
       "summary": {"total_lifted": 9, "total_unmapped": 3}}


def make_user(with_review_perm: bool = True) -> User:
    return User(user_name="reviewer" if with_review_perm else "uploader",
               first_name=None, last_name=None, email=None, avatar=None,
               is_active=True, roles=[], groups=None,
               permissions=["sgc-review-data"] if with_review_perm else [],
               is_internal=True, api_token=None, id=1)


def run(coro):
    return asyncio.run(coro)


def test_get_liftover_returns_job_for_reviewer(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_liftover_job_for_file", lambda engine, fid: JOB)
    result = run(sgc.get_sgc_liftover(file_id="f" * 32, user=make_user()))
    assert result["status"] == "SUCCEEDED" and result["summary"]["total_lifted"] == 9


def test_get_liftover_404_when_absent(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_liftover_job_for_file", lambda engine, fid: None)
    with pytest.raises(HTTPException) as exc:
        run(sgc.get_sgc_liftover(file_id="f" * 32, user=make_user()))
    assert exc.value.status_code == 404


def test_get_liftover_403_for_non_reviewer(monkeypatch):
    monkeypatch.setattr(query, "get_sgc_liftover_job_for_file", lambda engine, fid: JOB)
    with pytest.raises(HTTPException) as exc:
        run(sgc.get_sgc_liftover(file_id="f" * 32, user=make_user(with_review_perm=False)))
    assert exc.value.status_code == 403
