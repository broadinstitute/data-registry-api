"""hcm_gwas_ma_results exists with the expected columns after migration."""
from sqlalchemy import text
from tests.conftest import db

EXPECTED = {
    "id", "label", "status", "dataset_file_ids", "maf_min", "info_min",
    "meta_lambda_gc", "n_meta_variants", "n_genome_wide_sig", "n_cohorts",
    "n_cohorts_used", "total_cases", "total_controls", "manhattan_s3_key",
    "qq_s3_key", "meta_s3_key", "summary_json_s3_key", "summary_tsv_s3_key",
    "top_loci_s3_key", "batch_job_id", "error_message", "submitted_by",
    "created_at", "updated_at",
}


def test_hcm_gwas_ma_results_columns(api_client):
    with db.get_engine().connect() as c:
        cols = {r[0] for r in c.execute(text("SHOW COLUMNS FROM hcm_gwas_ma_results"))}
    assert EXPECTED <= cols
