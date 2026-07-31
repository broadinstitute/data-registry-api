import asyncio

import pytest
from fastapi import HTTPException

from dataregistry.api import hcm, hcm_query
from dataregistry.api.model import User

JOB = {"id": "a" * 32, "file_id": "f" * 32, "source_genome_build": "hg19",
       "target_genome_build": "hg38", "batch_job_id": None, "status": "SUCCEEDED",
       "submitted_at": None, "completed_at": None, "submitted_by": "system",
       "unmapped_s3_path": "s3://dig-data-registry-qa/hcm/liftover/fff/unmapped.tsv",
       "summary": {"total_lifted": 9, "total_unmapped": 3}}


def make_user(review=True):
    return User(user_name="reviewer" if review else "uploader", first_name=None,
                last_name=None, email=None, avatar=None, is_active=True, roles=[],
                groups=None, permissions=["hcm-review-data"] if review else [],
                is_internal=True, api_token=None, id=1)


def run(coro):
    return asyncio.run(coro)


def test_get_liftover_returns_job_for_reviewer(monkeypatch):
    monkeypatch.setattr(hcm_query, "get_hcm_liftover_job_for_file", lambda engine, fid: JOB)
    result = run(hcm.get_hcm_liftover(file_id="f" * 32, user=make_user()))
    assert result["status"] == "SUCCEEDED" and result["summary"]["total_lifted"] == 9


def test_get_liftover_404_when_absent(monkeypatch):
    monkeypatch.setattr(hcm_query, "get_hcm_liftover_job_for_file", lambda engine, fid: None)
    with pytest.raises(HTTPException) as exc:
        run(hcm.get_hcm_liftover(file_id="f" * 32, user=make_user()))
    assert exc.value.status_code == 404


def test_get_liftover_403_for_non_reviewer(monkeypatch):
    monkeypatch.setattr(hcm_query, "get_hcm_liftover_job_for_file", lambda engine, fid: JOB)
    with pytest.raises(HTTPException) as exc:
        run(hcm.get_hcm_liftover(file_id="f" * 32, user=make_user(review=False)))
    assert exc.value.status_code == 403


def test_unmapped_url_returns_presigned_for_reviewer(monkeypatch):
    monkeypatch.setattr(hcm_query, "get_hcm_liftover_job_for_file", lambda engine, fid: JOB)
    captured = {}

    def fake_signed(bucket, key):
        captured["bucket"], captured["key"] = bucket, key
        return "https://signed.example/unmapped.tsv"

    monkeypatch.setattr(hcm.s3, "get_signed_url", fake_signed)
    result = run(hcm.get_hcm_liftover_unmapped_url(file_id="f" * 32, user=make_user()))
    assert result["presigned_url"] == "https://signed.example/unmapped.tsv"
    # parsed the ACTUAL bucket/key from the s3:// URI (not BASE_BUCKET)
    assert captured["bucket"] == "dig-data-registry-qa"
    assert captured["key"] == "hcm/liftover/fff/unmapped.tsv"


def test_unmapped_url_404_when_no_job(monkeypatch):
    monkeypatch.setattr(hcm_query, "get_hcm_liftover_job_for_file", lambda engine, fid: None)
    with pytest.raises(HTTPException) as exc:
        run(hcm.get_hcm_liftover_unmapped_url(file_id="f" * 32, user=make_user()))
    assert exc.value.status_code == 404


def test_unmapped_url_404_when_no_unmapped_path(monkeypatch):
    job = {**JOB, "unmapped_s3_path": None}
    monkeypatch.setattr(hcm_query, "get_hcm_liftover_job_for_file", lambda engine, fid: job)
    with pytest.raises(HTTPException) as exc:
        run(hcm.get_hcm_liftover_unmapped_url(file_id="f" * 32, user=make_user()))
    assert exc.value.status_code == 404


def test_unmapped_url_403_for_non_reviewer(monkeypatch):
    monkeypatch.setattr(hcm_query, "get_hcm_liftover_job_for_file", lambda engine, fid: JOB)
    with pytest.raises(HTTPException) as exc:
        run(hcm.get_hcm_liftover_unmapped_url(file_id="f" * 32, user=make_user(review=False)))
    assert exc.value.status_code == 403


def test_run_all_403_for_non_reviewer():
    with pytest.raises(HTTPException) as exc:
        run(hcm.run_all_hcm_liftover(background_tasks=None, user=make_user(review=False)))
    assert exc.value.status_code == 403


def test_run_all_happy_path_submits_wave_and_reports_counts(monkeypatch):
    liftable = [{"file_id": f"f{i}" * 4} for i in range(3)]
    unrecognized = [{"file_id": "u" * 32}]

    def fake_select_liftable(engine, limit):
        assert limit is None
        return liftable, unrecognized

    def fake_plan_liftover_wave(rows, cap):
        assert rows == liftable
        assert cap == hcm.HCM_LIFTOVER_WAVE_CAP
        return rows[:2], len(rows) - 2

    submitted = []

    def fake_kick_off(background_tasks, file_row, submitted_by):
        submitted.append(file_row["file_id"])
        return f"lid-{file_row['file_id']}"

    import hcm_liftover.submit_liftover_batch as submit_liftover_batch
    monkeypatch.setattr(submit_liftover_batch, "select_liftable", fake_select_liftable)
    monkeypatch.setattr(submit_liftover_batch, "plan_liftover_wave", fake_plan_liftover_wave)
    monkeypatch.setattr(hcm, "_kick_off_hcm_liftover", fake_kick_off)

    result = run(hcm.run_all_hcm_liftover(background_tasks=None, user=make_user()))

    assert result["submitted"] == 2
    assert result["remaining"] == 1
    assert result["unrecognized"] == 1
    assert submitted == [liftable[0]["file_id"], liftable[1]["file_id"]]
