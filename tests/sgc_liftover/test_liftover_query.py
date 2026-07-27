import json
from sqlalchemy import text
from dataregistry.api import query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import SGCCohort


def _make_file(engine) -> str:
    """Insert a cohort + a sgc_gwas_files row (raw), return the file id (hex)."""
    import uuid
    cohort_id = query.upsert_sgc_cohort(engine, SGCCohort(
        name="LiftTestCohort", uploaded_by="tester", total_sample_size=100,
        number_of_males=50, number_of_females=50))
    file_id = str(uuid.uuid4()).replace('-', '')
    with engine.connect() as c:
        c.execute(text("""
            INSERT INTO sgc_gwas_files
                (id, cohort_id, dataset, phenotype, ancestry, file_name, file_size,
                 s3_path, uploaded_by, column_mapping, metadata)
            VALUES (:id, :cohort_id, 'DS', 'PSOR', 'EAS', 'f.tsv', 10,
                    'sgc/gwas/x/DS/PSOR/f.tsv', 'tester',
                    :cm, JSON_OBJECT('genome_build', 'GRCh37'))
        """), {"id": file_id, "cohort_id": cohort_id,
               "cm": json.dumps({"col_chromosome": "CHR"})})
        c.commit()
    return file_id


def test_liftover_job_round_trip(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine)
    lid = query.insert_sgc_liftover_pending(
        engine, file_id, "hg19", "hg38",
        "s3://b/archive/f.tsv", "s3://b/unmapped.tsv", "tester")
    assert len(lid) == 32
    rows = query.get_sgc_liftover_jobs(engine)
    assert [r["id"] for r in rows] == [lid]
    assert rows[0]["status"] == "PENDING" and rows[0]["file_id"] == file_id

    query.update_sgc_liftover_job(engine, lid, status="SUCCEEDED",
                                  summary={"lifted": 5}, log="ok", completed=True)
    rows = query.get_sgc_liftover_jobs(engine)
    assert rows[0]["status"] == "SUCCEEDED"
    assert rows[0]["summary"] == {"lifted": 5}
    assert rows[0]["completed_at"] is not None


def test_set_sgc_gwas_file_build(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine)   # starts GRCh37 (cohort/file metadata)
    query.set_sgc_gwas_file_build(engine, file_id, "GRCh38")
    with engine.connect() as c:
        got = c.execute(text(
            "SELECT JSON_UNQUOTE(JSON_EXTRACT(metadata,'$.genome_build')) "
            "FROM sgc_gwas_files WHERE id = :id"), {"id": file_id}).scalar()
    assert got == "GRCh38"
