from sqlalchemy import text
from dataregistry.api.db import DataRegistryReadWriteDB

def test_sgc_gwas_munge_results_schema(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    with engine.connect() as c:
        cols = {r[0] for r in c.execute(text("SHOW COLUMNS FROM sgc_gwas_munge_results"))}
        ddl = c.execute(text("SHOW CREATE TABLE sgc_gwas_munge_results")).fetchone()[1].replace(" ", "")
    assert {"id","file_id","status","n_in","n_out","n_with_rsid","n_swapped","n_dropped","has_eaf","has_info",
            "maf_min","info_min","ref_version","engine","standardised_s3_key","munge_json_s3_key",
            "ref_mismatch_s3_key","error_message","created_at","updated_at","batch_job_id"} <= cols
    assert "UNIQUEKEY" in ddl and "(`file_id`)" in ddl and "ONDELETECASCADE" in ddl
