from sqlalchemy import text
from dataregistry.api.db import DataRegistryReadWriteDB


def test_sgc_liftover_jobs_table_shape(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    with engine.connect() as c:
        ddl = c.execute(text("SHOW CREATE TABLE sgc_liftover_jobs")).fetchone()[1]
        cols = {r[0] for r in c.execute(text("SHOW COLUMNS FROM sgc_liftover_jobs"))}
    flat = ddl.replace(" ", "")
    assert {"id", "file_id", "source_genome_build", "target_genome_build", "batch_job_id",
            "status", "submitted_at", "completed_at", "submitted_by",
            "original_s3_path", "unmapped_s3_path", "summary", "log"} <= cols
    assert "FOREIGNKEY" in flat and "`sgc_gwas_files`" in ddl and "ONDELETECASCADE" in flat
    assert "(`file_id`)REFERENCES`sgc_gwas_files`" in flat
