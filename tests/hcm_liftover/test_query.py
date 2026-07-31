import json

from sqlalchemy import text

from dataregistry.api import hcm_query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.hcm_model import HCMGWASFile


def _make_file(engine) -> str:
    """Insert a hcm_gwas_files row (GRCh37), return the file id (dashless hex)."""
    return hcm_query.insert_hcm_gwas_file(engine, HCMGWASFile(
        cohort_name="LiftTestCohort", sarc="ALL", ancestry="EUR", sex="ALL",
        genome_build="GRCh37", software="REGENIE", analyst="tester",
        file_name="f.tsv", file_size=10, s3_path="hcm/gwas/x/f.tsv",
        uploaded_by="tester", column_mapping={"col_chromosome": "CHR"}))


def test_liftover_job_round_trip(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine)
    lid = hcm_query.insert_hcm_liftover_pending(
        engine, file_id, "hg19", "hg38",
        "s3://b/archive/f.tsv", "s3://b/unmapped.tsv", "tester")
    assert len(lid) == 32
    rows = hcm_query.get_hcm_liftover_jobs(engine)
    assert [r["id"] for r in rows] == [lid]
    assert rows[0]["status"] == "PENDING" and rows[0]["file_id"] == file_id

    hcm_query.update_hcm_liftover_job(engine, lid, status="SUCCEEDED",
                                      summary={"lifted": 5}, log="ok", completed=True)
    rows = hcm_query.get_hcm_liftover_jobs(engine)
    assert rows[0]["status"] == "SUCCEEDED"
    assert rows[0]["summary"] == {"lifted": 5}
    assert rows[0]["completed_at"] is not None


def test_set_hcm_gwas_file_build(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine)   # starts GRCh37
    hcm_query.set_hcm_gwas_file_build(engine, file_id, "GRCh38")
    with engine.connect() as c:
        got = c.execute(text(
            "SELECT genome_build FROM hcm_gwas_files WHERE id = :id"),
            {"id": file_id}).scalar()
    assert got == "GRCh38"


def test_get_hcm_liftover_job_for_file_returns_latest(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine)
    with engine.connect() as c:
        # two jobs for the same file, explicit timestamps + a summary on the later one
        c.execute(text("""
            INSERT INTO hcm_liftover_jobs
                (id, file_id, source_genome_build, target_genome_build, status,
                 submitted_at, submitted_by, summary)
            VALUES
                (:a, :fid, 'hg19', 'hg38', 'FAILED',    '2020-01-01 00:00:00', 'system', NULL),
                (:b, :fid, 'hg19', 'hg38', 'SUCCEEDED', '2020-01-02 00:00:00', 'system', :summ)
        """), {"a": "a" * 32, "b": "b" * 32, "fid": file_id,
               "summ": json.dumps({"total_lifted": 9})})
        c.commit()
    got = hcm_query.get_hcm_liftover_job_for_file(engine, file_id)
    assert got is not None
    assert got["status"] == "SUCCEEDED"                 # the later job
    assert got["summary"] == {"total_lifted": 9}        # decoded to a dict


def test_get_hcm_liftover_job_for_file_none_when_absent(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine)                         # a file with no liftover job
    assert hcm_query.get_hcm_liftover_job_for_file(engine, file_id) is None


def test_liftover_end_to_end_updates_file_build(api_client):
    """Full flow the brief describes: pending job -> succeed with summary ->
    mark the file's genome_build column GRCh38."""
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine)
    lid = hcm_query.insert_hcm_liftover_pending(
        engine, file_id, "GRCh37", "GRCh38",
        "s3://b/archive/f.tsv", "s3://b/unmapped.tsv", "tester")

    got = hcm_query.get_hcm_liftover_job_for_file(engine, file_id)
    assert got["id"] == lid
    assert got["status"] == "PENDING"

    hcm_query.update_hcm_liftover_job(
        engine, lid, status="SUCCEEDED", summary={"total_lifted": 42}, completed=True)
    hcm_query.set_hcm_gwas_file_build(engine, file_id, "GRCh38")

    with engine.connect() as c:
        build = c.execute(text(
            "SELECT genome_build FROM hcm_gwas_files WHERE id = :id"),
            {"id": file_id}).scalar()
    assert build == "GRCh38"

    got = hcm_query.get_hcm_liftover_job_for_file(engine, file_id)
    assert got["status"] == "SUCCEEDED"
    assert got["summary"] == {"total_lifted": 42}
    assert got["completed_at"] is not None
