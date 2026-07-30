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


def test_models_construct():
    e = MAIgnoreEntry(id="a" * 32, file_id="b" * 32, reason="r", excluded_by="rev")
    assert e.file_id == "b" * 32 and e.cohort is None
    req = MAIgnoreCreateRequest(file_id="b" * 32, reason="r")
    assert req.reason == "r"
