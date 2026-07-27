import pytest
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

    entry = query.insert_ma_ignore(engine, cohort_id, "PSOR", "EUR",
                                   "lambda_gc 1.4 > threshold", "reviewer1")
    assert entry["cohort_id"] == cohort_id
    assert entry["phenotype"] == "PSOR" and entry["ancestry"] == "EUR"
    assert entry["reason"] == "lambda_gc 1.4 > threshold"
    assert entry["excluded_by"] == "reviewer1"
    assert len(entry["id"]) == 32

    rows = query.list_ma_ignore(engine)
    assert [r["id"] for r in rows] == [entry["id"]]

    assert query.delete_ma_ignore(engine, entry["id"]) is True
    assert query.list_ma_ignore(engine) == []


def test_insert_upserts_reason_on_conflict(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    cohort_id = _make_cohort(engine)
    first = query.insert_ma_ignore(engine, cohort_id, "PSOR", "EUR", "reason A", "rev1")
    second = query.insert_ma_ignore(engine, cohort_id, "PSOR", "EUR", "reason B", "rev2")
    assert first["id"] == second["id"]          # same row (unique triple), not a new one
    assert second["reason"] == "reason B" and second["excluded_by"] == "rev2"
    assert len(query.list_ma_ignore(engine)) == 1


def test_insert_unknown_cohort_id_raises_integrity_error(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    with pytest.raises(IntegrityError):
        query.insert_ma_ignore(engine, "0" * 32, "PSOR", "EUR", "x", "rev1")


def test_delete_missing_returns_false(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    assert query.delete_ma_ignore(engine, "deadbeef" * 4) is False


def test_ma_ignore_models_construct():
    e = MAIgnoreEntry(id="a" * 32, cohort_id="b" * 32, phenotype="PSOR",
                      ancestry="EUR", reason="r", excluded_by="rev1")
    assert e.phenotype == "PSOR" and e.created_at is None
    req = MAIgnoreCreateRequest(cohort_id="b" * 32, phenotype="PSOR",
                                ancestry="EUR", reason="r")
    assert req.reason == "r"


def test_ignored_cohorts_returns_entries_for_pheno_ancestry(api_client):
    from sgc_ma.select import ignored_cohorts
    engine = DataRegistryReadWriteDB().get_engine()
    cohort_id = _make_cohort(engine, name="IgnoredNameCohort")
    query.insert_ma_ignore(engine, cohort_id, "PSOR", "EUR", "inflated", "rev1")
    query.insert_ma_ignore(engine, cohort_id, "PSOR", "AFR", "other anc", "rev1")
    got = ignored_cohorts(engine, "PSOR", "EUR")
    assert len(got) == 1
    assert got[0]["cohort_id"] == cohort_id
    assert got[0]["cohort"] == "IgnoredNameCohort"
    assert got[0]["reason"] == "inflated"
