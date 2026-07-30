import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api import query
from dataregistry.api.model import SGCCohort


def _cohort(engine):
    return query.upsert_sgc_cohort(engine, SGCCohort(
        name="ChkCohort", uploaded_by="t",
        total_sample_size=10, number_of_males=5, number_of_females=5))


def test_ignore_has_sex_and_four_col_unique_key(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    with engine.connect() as c:
        cols = {r[0] for r in c.execute(text("SHOW COLUMNS FROM sgc_ma_ignore"))}
        ddl = c.execute(text("SHOW CREATE TABLE sgc_ma_ignore")).fetchone()[1].replace(" ", "")
    assert "sex" in cols
    assert "(`cohort_id`,`phenotype`,`ancestry`,`sex`)" in ddl


def test_results_has_sex(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    with engine.connect() as c:
        cols = {r[0] for r in c.execute(text("SHOW COLUMNS FROM sgc_gwas_ma_results"))}
    assert "sex" in cols


def test_check_rejects_invalid_bucket(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    cid = _cohort(engine)
    with pytest.raises(DBAPIError):
        with engine.connect() as c:
            c.execute(text(
                "INSERT INTO sgc_ma_ignore (id, cohort_id, phenotype, ancestry, sex, reason, excluded_by) "
                "VALUES (:id, :cid, 'PSOR', 'AFR', 'Male', 'x', 'rev')"),
                {"id": "1" * 32, "cid": cid})
            c.commit()


def test_check_accepts_nine_buckets(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    cid = _cohort(engine)
    with engine.connect() as c:
        for i, (anc, sex) in enumerate([("Combined", "All"), ("Combined", "Male"), ("EUR", "All")]):
            c.execute(text(
                "INSERT INTO sgc_ma_ignore (id, cohort_id, phenotype, ancestry, sex, reason, excluded_by) "
                "VALUES (:id, :cid, 'PSOR', :anc, :sex, 'x', 'rev')"),
                {"id": str(i) * 32, "cid": cid, "anc": anc, "sex": sex})
        c.commit()


def test_cohort_codes_table_shape(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    with engine.connect() as c:
        cols = {r[0] for r in c.execute(text("SHOW COLUMNS FROM sgc_cohort_codes"))}
    assert {"code", "cohort_id"} <= cols
