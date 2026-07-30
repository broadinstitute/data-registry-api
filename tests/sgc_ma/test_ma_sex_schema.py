import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from dataregistry.api.db import DataRegistryReadWriteDB


def test_results_has_sex(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    with engine.connect() as c:
        cols = {r[0] for r in c.execute(text("SHOW COLUMNS FROM sgc_gwas_ma_results"))}
    assert "sex" in cols


def test_check_rejects_invalid_bucket(api_client):
    # sgc_ma_ignore is file-based (no sex/ancestry/bucket CHECK); the nine-bucket
    # CHECK still lives on sgc_gwas_ma_results, so exercise it there.
    engine = DataRegistryReadWriteDB().get_engine()
    with pytest.raises(DBAPIError):
        with engine.connect() as c:
            c.execute(text(
                "INSERT INTO sgc_gwas_ma_results (id, phenotype, ancestry, sex, status) "
                "VALUES (:id, 'PSOR', 'AFR', 'Male', 'PENDING')"),
                {"id": "1" * 32})
            c.commit()


def test_cohort_codes_table_shape(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    with engine.connect() as c:
        cols = {r[0] for r in c.execute(text("SHOW COLUMNS FROM sgc_cohort_codes"))}
    assert {"code", "cohort_id"} <= cols
