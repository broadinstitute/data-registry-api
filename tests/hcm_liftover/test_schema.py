from sqlalchemy import text
from tests.conftest import db


def test_hcm_liftover_jobs_schema(api_client):
    with db.get_engine().connect() as c:
        cols = {r[0] for r in c.execute(text("SHOW COLUMNS FROM hcm_liftover_jobs"))}
        assert {"id", "file_id", "source_genome_build", "target_genome_build", "batch_job_id",
                "status", "submitted_at", "completed_at", "submitted_by", "original_s3_path",
                "unmapped_s3_path", "summary", "log"} <= cols
        tgt = c.execute(text("SELECT target_genome_build FROM portal_liftover_config WHERE portal_id='hcm'")).scalar()
        assert tgt == "hg38"
