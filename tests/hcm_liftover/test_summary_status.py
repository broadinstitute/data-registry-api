"""get_all_hcm_gwas_files enriches every row with liftover_status, mirroring
get_all_sgc_gwas_files (dataregistry/api/query.py). Job status wins over the
file's genome_build when a hcm_liftover_jobs row exists for the file."""
from dataregistry.api import hcm_query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.hcm_model import HCMGWASFile


def _make_file(engine, genome_build: str, file_name: str) -> str:
    """Insert a hcm_gwas_files row with the given build, return the file id
    (dashless hex, as returned by insert_hcm_gwas_file)."""
    return hcm_query.insert_hcm_gwas_file(engine, HCMGWASFile(
        cohort_name="SummaryStatusCohort", sarc="ALL", ancestry="EUR", sex="ALL",
        genome_build=genome_build, software="REGENIE", analyst="tester",
        file_name=file_name, file_size=10, s3_path=f"hcm/gwas/x/{file_name}",
        uploaded_by="tester", column_mapping={"col_chromosome": "CHR"}))


def _status_for(engine, file_name: str) -> str:
    """Look up the liftover_status of the row with the given file_name from
    get_all_hcm_gwas_files (avoids dashed-vs-dashless id formatting churn)."""
    rows = hcm_query.get_all_hcm_gwas_files(engine)
    matches = [r for r in rows if r["file_name"] == file_name]
    assert len(matches) == 1
    return matches[0]["liftover_status"]


def test_grch38_native_file_with_no_job_reads_native(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    _make_file(engine, "GRCh38", "native.tsv")
    assert _status_for(engine, "native.tsv") == "GRCh38 (native)"


def test_grch37_file_with_no_job_needs_liftover(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    _make_file(engine, "GRCh37", "needs_liftover.tsv")
    assert _status_for(engine, "needs_liftover.tsv") == "Needs liftover"


def test_grch37_file_with_succeeded_job_reads_lifted(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_file(engine, "GRCh37", "lifted.tsv")
    lid = hcm_query.insert_hcm_liftover_pending(
        engine, file_id, "GRCh37", "GRCh38",
        "s3://b/archive/lifted.tsv", "s3://b/unmapped.tsv", "tester")
    hcm_query.update_hcm_liftover_job(engine, lid, status="SUCCEEDED", completed=True)
    assert _status_for(engine, "lifted.tsv") == "Lifted to GRCh38"
