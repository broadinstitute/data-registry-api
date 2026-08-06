import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from dataregistry.api import query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import SGCCohort, MAIgnoreEntry, MAIgnoreCreateRequest


def _make_cohort_and_file(engine, name="IgFileCohort", pheno="PSOR", anc="EUR", sex="All"):
    import uuid, json
    cohort_id = query.upsert_sgc_cohort(engine, SGCCohort(
        name=name, uploaded_by="t", total_sample_size=10, number_of_males=5, number_of_females=5))
    file_id = str(uuid.uuid4()).replace('-', '')
    with engine.connect() as c:
        c.execute(text("""INSERT INTO sgc_gwas_files
            (id, cohort_id, dataset, phenotype, ancestry, file_name, file_size, s3_path,
             uploaded_at, uploaded_by, column_mapping, metadata)
            VALUES (:id,:coh,:ds,:p,:a,'f',1,'s3://x',NOW(),'t','{}',:meta)"""),
            {"id": file_id, "coh": cohort_id, "ds": "DS." + name, "p": pheno, "a": anc,
             "meta": json.dumps({"sex": sex})})
        c.commit()
    return cohort_id, file_id


def test_insert_list_delete_round_trip(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    _, file_id = _make_cohort_and_file(engine)
    entry = query.insert_ma_ignore(engine, file_id, "Phenotyping error", "Jake")
    assert entry["file_id"] == file_id and entry["reason"] == "Phenotyping error"
    assert entry["excluded_by"] == "Jake" and entry["phenotype"] == "PSOR" and entry["ancestry"] == "EUR"
    assert len(entry["id"]) == 32
    rows = query.list_ma_ignore(engine)
    assert [r["id"] for r in rows] == [entry["id"]] and rows[0]["cohort"] == "IgFileCohort"
    assert query.delete_ma_ignore(engine, entry["id"]) is True
    assert query.list_ma_ignore(engine) == []


def test_insert_upserts_on_file_id(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    _, file_id = _make_cohort_and_file(engine)
    a = query.insert_ma_ignore(engine, file_id, "reason A", "rev1")
    b = query.insert_ma_ignore(engine, file_id, "reason B", "rev2")
    assert a["id"] == b["id"] and b["reason"] == "reason B"
    assert len(query.list_ma_ignore(engine)) == 1


def test_insert_unknown_file_id_raises(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    with pytest.raises(IntegrityError):
        query.insert_ma_ignore(engine, "0" * 32, "x", "rev")


def test_replace_swaps_the_whole_list(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    _, old_a = _make_cohort_and_file(engine, "OldA")
    _, old_b = _make_cohort_and_file(engine, "OldB")
    _, new_id = _make_cohort_and_file(engine, "New")
    query.insert_ma_ignore(engine, old_a, "stale", "rev1")
    query.insert_ma_ignore(engine, old_b, "stale", "rev1")
    counts = query.replace_ma_ignore(engine, [
        {"file_id": new_id, "reason": "fresh", "excluded_by": "rev2"}])
    assert counts == {"removed": 2, "added": 1}
    rows = query.list_ma_ignore(engine)
    assert [r["file_id"] for r in rows] == [new_id]
    assert rows[0]["reason"] == "fresh" and rows[0]["excluded_by"] == "rev2"


def test_replace_collapses_repeated_file_ids(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    _, file_id = _make_cohort_and_file(engine)
    counts = query.replace_ma_ignore(engine, [
        {"file_id": file_id, "reason": "first", "excluded_by": "rev1"},
        {"file_id": file_id, "reason": "second", "excluded_by": "rev2"}])
    assert counts == {"removed": 0, "added": 1}
    rows = query.list_ma_ignore(engine)
    assert len(rows) == 1 and rows[0]["reason"] == "second"


def test_replace_is_atomic_when_an_insert_fails(api_client):
    """A bad file_id part-way through must roll the delete back, not empty the list."""
    engine = DataRegistryReadWriteDB().get_engine()
    _, existing = _make_cohort_and_file(engine, "Existing")
    _, good = _make_cohort_and_file(engine, "Good")
    query.insert_ma_ignore(engine, existing, "keep me", "rev1")
    with pytest.raises(IntegrityError):
        query.replace_ma_ignore(engine, [
            {"file_id": good, "reason": "ok", "excluded_by": "rev2"},
            {"file_id": "0" * 32, "reason": "bad fk", "excluded_by": "rev2"}])
    rows = query.list_ma_ignore(engine)
    assert [r["file_id"] for r in rows] == [existing] and rows[0]["reason"] == "keep me"


def test_delete_all_removes_every_row(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    for name in ("A", "B", "C"):
        _, file_id = _make_cohort_and_file(engine, name)
        query.insert_ma_ignore(engine, file_id, "r", "rev1")
    assert query.delete_all_ma_ignore(engine) == 3
    assert query.list_ma_ignore(engine) == []


def test_delete_all_on_empty_list_is_a_no_op(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    assert query.delete_all_ma_ignore(engine) == 0


def test_models_construct():
    e = MAIgnoreEntry(id="a" * 32, file_id="b" * 32, reason="r", excluded_by="rev")
    assert e.file_id == "b" * 32 and e.cohort is None
    req = MAIgnoreCreateRequest(file_id="b" * 32, reason="r")
    assert req.reason == "r"
