import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from dataregistry.api import query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import SGCCohort, MAIgnoreEntry, MAIgnoreCreateRequest


def _make_cohort(engine, name="IgnoreTestCohort") -> str:
    return query.upsert_sgc_cohort(engine, SGCCohort(
        name=name, uploaded_by="testuser", total_sample_size=100,
        number_of_males=50, number_of_females=50))


def test_insert_list_delete_round_trip(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    cohort_id = _make_cohort(engine)
    entry = query.insert_ma_ignore(engine, cohort_id, "PSOR", "EUR", "All",
                                   "lambda_gc 1.4 > threshold", "reviewer1")
    assert entry["ancestry"] == "EUR" and entry["sex"] == "All"
    assert len(entry["id"]) == 32
    rows = query.list_ma_ignore(engine)
    assert [r["id"] for r in rows] == [entry["id"]] and rows[0]["sex"] == "All"
    assert query.delete_ma_ignore(engine, entry["id"]) is True


def test_same_cohort_pheno_ancestry_different_sex_are_distinct_rows(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    cohort_id = _make_cohort(engine)
    a = query.insert_ma_ignore(engine, cohort_id, "PSOR", "Combined", "Male", "r", "rev")
    b = query.insert_ma_ignore(engine, cohort_id, "PSOR", "Combined", "Female", "r", "rev")
    assert a["id"] != b["id"] and len(query.list_ma_ignore(engine)) == 2


def test_insert_upserts_on_full_key(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    cohort_id = _make_cohort(engine)
    first = query.insert_ma_ignore(engine, cohort_id, "PSOR", "EUR", "All", "reason A", "rev1")
    second = query.insert_ma_ignore(engine, cohort_id, "PSOR", "EUR", "All", "reason B", "rev2")
    assert first["id"] == second["id"] and second["reason"] == "reason B"
    assert len(query.list_ma_ignore(engine)) == 1


def test_insert_unknown_cohort_id_raises_integrity_error(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    with pytest.raises(IntegrityError):
        query.insert_ma_ignore(engine, "0" * 32, "PSOR", "EUR", "All", "x", "rev1")


def test_delete_missing_returns_false(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    assert query.delete_ma_ignore(engine, "deadbeef" * 4) is False


def test_ma_ignore_models_construct():
    e = MAIgnoreEntry(id="a" * 32, cohort_id="b" * 32, phenotype="PSOR",
                      ancestry="EUR", sex="All", reason="r", excluded_by="rev1")
    assert e.sex == "All" and e.created_at is None
    req = MAIgnoreCreateRequest(cohort_id="b" * 32, phenotype="PSOR",
                                ancestry="EUR", reason="r")
    assert req.sex == "All"          # default


def test_ignored_cohorts_returns_entries_for_pheno_ancestry(api_client):
    from sgc_ma.select import ignored_cohorts
    engine = DataRegistryReadWriteDB().get_engine()
    cohort_id = _make_cohort(engine, name="IgnoredNameCohort")
    query.insert_ma_ignore(engine, cohort_id, "PSOR", "EUR", "All", "inflated", "rev1")
    query.insert_ma_ignore(engine, cohort_id, "PSOR", "AFR", "All", "other anc", "rev1")
    got = ignored_cohorts(engine, "PSOR", "EUR")
    assert len(got) == 1 and got[0]["cohort"] == "IgnoredNameCohort"


def test_resolve_cohort_code(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    cohort_id = _make_cohort(engine)
    with engine.connect() as c:
        c.execute(text("INSERT INTO sgc_cohort_codes (code, cohort_id) VALUES ('ZZ', :cid)"),
                  {"cid": cohort_id})
        c.commit()
    assert query.resolve_cohort_code(engine, "ZZ") == cohort_id
    assert query.resolve_cohort_code(engine, "zz") == cohort_id     # case-insensitive
    assert query.resolve_cohort_code(engine, "NOPE") is None
