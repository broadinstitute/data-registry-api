import os
import sys
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import scripts.restore_hcm_liftover as hcm_restore
import scripts.restore_sgc_liftover as sgc_restore
from dataregistry.api import hcm_query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.hcm_model import HCMGWASFile


def _make_hcm_file(engine, genome_build="GRCh37", s3_path="hcm/gwas/x/f.tsv") -> str:
    return hcm_query.insert_hcm_gwas_file(engine, HCMGWASFile(
        cohort_name="RestoreTest", sarc="ALL", ancestry="EUR", sex="ALL",
        genome_build=genome_build, software="REGENIE", analyst="t",
        file_name="f.tsv", file_size=10, s3_path=s3_path, uploaded_by="t",
        column_mapping={"chromosome": "CHR", "position": "POS",
                        "effect_allele": "A1", "non_effect_allele": "A2"}))


def test_no_job_raises(monkeypatch):
    monkeypatch.setattr(hcm_restore.query, "get_hcm_liftover_job_for_file",
                        lambda engine, fid: None)
    with pytest.raises(SystemExit):
        hcm_restore.restore(object(), Mock(), "FID", "bkt")


def test_dry_run_touches_nothing(monkeypatch):
    monkeypatch.setattr(hcm_restore.query, "get_hcm_liftover_job_for_file",
                        lambda engine, fid: {"id": "JOB1", "original_s3_path": "s3://bkt/a/orig.tsv",
                                             "status": "SUCCEEDED"})
    monkeypatch.setattr(hcm_restore, "_get_file_s3_path", lambda engine, fid: "hcm/gwas/x/f.tsv")
    set_calls, del_calls = [], []
    monkeypatch.setattr(hcm_restore.query, "set_hcm_gwas_file_build",
                        lambda *a, **k: set_calls.append(a))
    monkeypatch.setattr(hcm_restore, "_delete_liftover_jobs_for_file",
                        lambda *a, **k: del_calls.append(a))
    s3 = Mock()
    plan = hcm_restore.restore(object(), s3, "FID", "bkt", dry_run=True)
    assert plan["dry_run"] is True
    s3.head_object.assert_called_once()          # guard still runs
    s3.copy_object.assert_not_called()
    assert set_calls == [] and del_calls == []


def test_missing_archive_raises(monkeypatch):
    monkeypatch.setattr(hcm_restore.query, "get_hcm_liftover_job_for_file",
                        lambda engine, fid: {"id": "JOB1", "original_s3_path": "s3://bkt/a/orig.tsv",
                                             "status": "SUCCEEDED"})
    monkeypatch.setattr(hcm_restore, "_get_file_s3_path", lambda engine, fid: "hcm/gwas/x/f.tsv")
    s3 = Mock()
    s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
    with pytest.raises(SystemExit):
        hcm_restore.restore(object(), s3, "FID", "bkt")


def test_missing_archive_non_404_propagates(monkeypatch):
    """A non-not-found ClientError (e.g. 403/throttling) must NOT be mislabeled as
    'archive not found' -- it should propagate so the operator sees the real cause."""
    monkeypatch.setattr(hcm_restore.query, "get_hcm_liftover_job_for_file",
                        lambda engine, fid: {"id": "JOB1", "original_s3_path": "s3://bkt/a/orig.tsv",
                                             "status": "SUCCEEDED"})
    monkeypatch.setattr(hcm_restore, "_get_file_s3_path", lambda engine, fid: "hcm/gwas/x/f.tsv")
    s3 = Mock()
    s3.head_object.side_effect = ClientError({"Error": {"Code": "403"}}, "HeadObject")
    with pytest.raises(ClientError):
        hcm_restore.restore(object(), s3, "FID", "bkt")


def test_bucket_mismatch_raises(monkeypatch):
    """--bucket must match the archive's own bucket, or we could write archived
    bytes to a stray key in the wrong bucket while never actually restoring the
    real file."""
    monkeypatch.setattr(hcm_restore.query, "get_hcm_liftover_job_for_file",
                        lambda engine, fid: {"id": "JOB1", "original_s3_path": "s3://bkt/a/orig.tsv",
                                             "status": "SUCCEEDED"})
    monkeypatch.setattr(hcm_restore, "_get_file_s3_path", lambda engine, fid: "hcm/gwas/x/f.tsv")
    s3 = Mock()
    with pytest.raises(SystemExit):
        hcm_restore.restore(object(), s3, "FID", "other")
    s3.head_object.assert_not_called()
    s3.copy_object.assert_not_called()


def test_in_flight_job_refused(monkeypatch):
    monkeypatch.setattr(hcm_restore.query, "get_hcm_liftover_job_for_file",
                        lambda engine, fid: {"id": "JOB1", "original_s3_path": "s3://bkt/a/orig.tsv",
                                             "status": "RUNNING"})
    monkeypatch.setattr(hcm_restore, "_get_file_s3_path", lambda engine, fid: "hcm/gwas/x/f.tsv")
    set_calls, del_calls = [], []
    monkeypatch.setattr(hcm_restore.query, "set_hcm_gwas_file_build",
                        lambda *a, **k: set_calls.append(a))
    monkeypatch.setattr(hcm_restore, "_delete_liftover_jobs_for_file",
                        lambda *a, **k: del_calls.append(a))
    s3 = Mock()
    with pytest.raises(SystemExit):
        hcm_restore.restore(object(), s3, "FID", "bkt")
    s3.copy_object.assert_not_called()
    assert set_calls == [] and del_calls == []


def test_live_restore_copies_flips_deletes(monkeypatch):
    monkeypatch.setattr(hcm_restore.query, "get_hcm_liftover_job_for_file",
                        lambda engine, fid: {"id": "JOB1", "original_s3_path": "s3://bkt/a/orig.tsv",
                                             "status": "SUCCEEDED"})
    monkeypatch.setattr(hcm_restore, "_get_file_s3_path", lambda engine, fid: "hcm/gwas/x/f.tsv")
    flips, deletes = [], []
    monkeypatch.setattr(hcm_restore.query, "set_hcm_gwas_file_build",
                        lambda engine, fid, build: flips.append((fid, build)))
    monkeypatch.setattr(hcm_restore, "_delete_liftover_jobs_for_file",
                        lambda engine, fid: deletes.append(fid))
    s3 = Mock()
    hcm_restore.restore(object(), s3, "FID", "bkt", to_build="GRCh37")
    s3.copy_object.assert_called_once_with(
        Bucket="bkt", Key="hcm/gwas/x/f.tsv",
        CopySource={"Bucket": "bkt", "Key": "a/orig.tsv"})
    assert flips == [("FID", "GRCh37")]
    assert deletes == ["FID"]


def test_real_db_flip_and_delete(api_client):
    """End-to-end against the test DB: real file + job rows, mock S3."""
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_hcm_file(engine, genome_build="GRCh38")   # simulate a post-lift file
    job_id = hcm_query.insert_hcm_liftover_pending(
        engine, file_id, "hg19", "hg38", "s3://bkt/hcm/liftover/_archive/x/f.tsv",
        "s3://bkt/hcm/liftover/x/unmapped.tsv", "system")
    hcm_query.update_hcm_liftover_job(engine, job_id, status="SUCCEEDED", completed=True)

    hcm_restore.restore(engine, Mock(), file_id, "bkt", to_build="GRCh37")

    files = {f["id"].replace('-', ''): f for f in hcm_query.get_all_hcm_gwas_files(engine)}
    assert files[file_id.replace('-', '')]["genome_build"] == "GRCh37"
    assert hcm_query.get_hcm_liftover_job_for_file(engine, file_id) is None


def test_real_db_multiple_job_rows_all_deleted(api_client):
    """A file with a stale FAILED job row plus a newer SUCCEEDED job row: restore
    must delete BOTH rows (delete keys on file_id, not just the single latest
    job_id) -- otherwise the older FAILED row survives and the file keeps showing
    "Failed" instead of "Needs liftover" after the restore."""
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_hcm_file(engine, genome_build="GRCh38")

    failed_job_id = hcm_query.insert_hcm_liftover_pending(
        engine, file_id, "hg19", "hg38", "s3://bkt/hcm/liftover/_archive/x/f-old.tsv",
        "s3://bkt/hcm/liftover/x/unmapped-old.tsv", "system")
    hcm_query.update_hcm_liftover_job(engine, failed_job_id, status="FAILED", completed=True)

    succeeded_job_id = hcm_query.insert_hcm_liftover_pending(
        engine, file_id, "hg19", "hg38", "s3://bkt/hcm/liftover/_archive/x/f.tsv",
        "s3://bkt/hcm/liftover/x/unmapped.tsv", "system")
    hcm_query.update_hcm_liftover_job(engine, succeeded_job_id, status="SUCCEEDED", completed=True)

    hcm_restore.restore(engine, Mock(), file_id, "bkt", to_build="GRCh37")

    files = {f["id"].replace('-', ''): f for f in hcm_query.get_all_hcm_gwas_files(engine)}
    assert files[file_id.replace('-', '')]["genome_build"] == "GRCh37"
    assert hcm_query.get_hcm_liftover_job_for_file(engine, file_id) is None


def test_sgc_no_job_raises(monkeypatch):
    monkeypatch.setattr(sgc_restore.query, "get_sgc_liftover_job_for_file",
                        lambda engine, fid: None)
    with pytest.raises(SystemExit):
        sgc_restore.restore(object(), Mock(), "FID", "bkt")


def test_sgc_dry_run_touches_nothing(monkeypatch):
    monkeypatch.setattr(sgc_restore.query, "get_sgc_liftover_job_for_file",
                        lambda engine, fid: {"id": "JOB1", "original_s3_path": "s3://bkt/a/orig.tsv",
                                             "status": "SUCCEEDED"})
    monkeypatch.setattr(sgc_restore, "_get_file_s3_path", lambda engine, fid: "sgc/gwas/x/f.tsv")
    set_calls, del_calls = [], []
    monkeypatch.setattr(sgc_restore.query, "set_sgc_gwas_file_build",
                        lambda *a, **k: set_calls.append(a))
    monkeypatch.setattr(sgc_restore, "_delete_liftover_jobs_for_file",
                        lambda *a, **k: del_calls.append(a))
    s3 = Mock()
    plan = sgc_restore.restore(object(), s3, "FID", "bkt", dry_run=True)
    assert plan["dry_run"] is True
    s3.head_object.assert_called_once()          # guard still runs
    s3.copy_object.assert_not_called()
    assert set_calls == [] and del_calls == []


def test_sgc_missing_archive_raises(monkeypatch):
    monkeypatch.setattr(sgc_restore.query, "get_sgc_liftover_job_for_file",
                        lambda engine, fid: {"id": "JOB1", "original_s3_path": "s3://bkt/a/orig.tsv",
                                             "status": "SUCCEEDED"})
    monkeypatch.setattr(sgc_restore, "_get_file_s3_path", lambda engine, fid: "sgc/gwas/x/f.tsv")
    s3 = Mock()
    s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
    with pytest.raises(SystemExit):
        sgc_restore.restore(object(), s3, "FID", "bkt")


def test_sgc_in_flight_job_refused(monkeypatch):
    monkeypatch.setattr(sgc_restore.query, "get_sgc_liftover_job_for_file",
                        lambda engine, fid: {"id": "JOB1", "original_s3_path": "s3://bkt/a/orig.tsv",
                                             "status": "RUNNING"})
    monkeypatch.setattr(sgc_restore, "_get_file_s3_path", lambda engine, fid: "sgc/gwas/x/f.tsv")
    set_calls, del_calls = [], []
    monkeypatch.setattr(sgc_restore.query, "set_sgc_gwas_file_build",
                        lambda *a, **k: set_calls.append(a))
    monkeypatch.setattr(sgc_restore, "_delete_liftover_jobs_for_file",
                        lambda *a, **k: del_calls.append(a))
    s3 = Mock()
    with pytest.raises(SystemExit):
        sgc_restore.restore(object(), s3, "FID", "bkt")
    s3.copy_object.assert_not_called()
    assert set_calls == [] and del_calls == []


def test_sgc_live_restore_copies_flips_deletes(monkeypatch):
    monkeypatch.setattr(sgc_restore.query, "get_sgc_liftover_job_for_file",
                        lambda engine, fid: {"id": "JOB1", "original_s3_path": "s3://bkt/a/orig.tsv",
                                             "status": "SUCCEEDED"})
    monkeypatch.setattr(sgc_restore, "_get_file_s3_path", lambda engine, fid: "sgc/gwas/x/f.tsv")
    flips, deletes = [], []
    monkeypatch.setattr(sgc_restore.query, "set_sgc_gwas_file_build",
                        lambda engine, fid, build: flips.append((fid, build)))
    monkeypatch.setattr(sgc_restore, "_delete_liftover_jobs_for_file",
                        lambda engine, fid: deletes.append(fid))
    s3 = Mock()
    sgc_restore.restore(object(), s3, "FID", "bkt", to_build="GRCh37")
    s3.copy_object.assert_called_once_with(
        Bucket="bkt", Key="sgc/gwas/x/f.tsv",
        CopySource={"Bucket": "bkt", "Key": "a/orig.tsv"})
    assert flips == [("FID", "GRCh37")] and deletes == ["FID"]
