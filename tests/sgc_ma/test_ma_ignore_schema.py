from sqlalchemy import text
from dataregistry.api.db import DataRegistryReadWriteDB


def test_sgc_ma_ignore_table_shape(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    with engine.connect() as c:
        ddl = c.execute(text("SHOW CREATE TABLE sgc_ma_ignore")).fetchone()[1]
        cols = {r[0] for r in c.execute(text("SHOW COLUMNS FROM sgc_ma_ignore"))}
    flat = ddl.replace(" ", "")
    assert {"id", "cohort_id", "phenotype", "ancestry", "reason",
            "excluded_by", "created_at", "sex"} <= cols
    # unique key on the quadruple
    assert "UNIQUEKEY" in flat and "(`cohort_id`,`phenotype`,`ancestry`,`sex`)" in flat
    # FK to sgc_cohorts with cascade delete
    assert "FOREIGNKEY" in flat and "`sgc_cohorts`" in ddl and "ONDELETECASCADE" in flat
