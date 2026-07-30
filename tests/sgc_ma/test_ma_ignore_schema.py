from sqlalchemy import text
from dataregistry.api.db import DataRegistryReadWriteDB


def test_sgc_ma_ignore_is_file_based(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    with engine.connect() as c:
        cols = {r[0] for r in c.execute(text("SHOW COLUMNS FROM sgc_ma_ignore"))}
        ddl = c.execute(text("SHOW CREATE TABLE sgc_ma_ignore")).fetchone()[1].replace(" ", "")
    assert {"id", "file_id", "reason", "excluded_by", "created_at"} <= cols
    assert "cohort_id" not in cols and "ancestry" not in cols and "sex" not in cols
    assert "UNIQUEKEY" in ddl and "(`file_id`)" in ddl
    assert "FOREIGNKEY" in ddl and "`sgc_gwas_files`" in ddl and "ONDELETECASCADE" in ddl
