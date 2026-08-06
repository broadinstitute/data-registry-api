"""Unit tests for the file-id based MA ignore-list bulk-upload parser, plus end-to-end
tests of the delete-all endpoint against the test DB."""
import json
import uuid

import pytest
from sqlalchemy import text

from dataregistry.api import query
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import SGCCohort, User
from dataregistry.api.sgc import get_sgc_user, parse_bulk_ignore_rows
from dataregistry.server import app

REVIEWER = User(user_name='reviewer', roles=[], permissions=['sgc-review-data'])
UPLOADER = User(user_name='uploader', roles=[], permissions=[])


def test_csv_with_header():
    assert parse_bulk_ignore_rows("file_id,reason,excluded_by\nAAA,Phenotyping error,Jake\n") == [
        {"file_id": "AAA", "reason": "Phenotyping error", "excluded_by": "Jake"}]


def test_quoted_space_template_format():
    content = '"file_id" "reason" "excluded_by"\n"AAA" "Old submission" "Jake Saklatvala"\n'
    assert parse_bulk_ignore_rows(content) == [
        {"file_id": "AAA", "reason": "Old submission", "excluded_by": "Jake Saklatvala"}]


def test_tsv_and_no_header_positional():
    assert parse_bulk_ignore_rows("BBB\tbad build\tRev\n") == [
        {"file_id": "BBB", "reason": "bad build", "excluded_by": "Rev"}]


def test_rows_without_file_id_skipped_and_empty():
    assert parse_bulk_ignore_rows(",no id,x\n") == []
    assert parse_bulk_ignore_rows("") == []


@pytest.fixture(autouse=True)
def sgc_reviewer(api_client):
    """Authenticate every endpoint call below as a reviewer."""
    app.dependency_overrides[get_sgc_user] = lambda: REVIEWER
    yield
    app.dependency_overrides.pop(get_sgc_user, None)


def _make_gwas_file(engine, name="BulkCohort", pheno="PSOR", anc="EUR"):
    """A cohort + GWAS file the ignore-list FK can point at. Returns the file id."""
    cohort_id = query.upsert_sgc_cohort(engine, SGCCohort(
        name=name, uploaded_by="t", total_sample_size=10, number_of_males=5, number_of_females=5))
    file_id = str(uuid.uuid4()).replace('-', '')
    with engine.connect() as c:
        c.execute(text("""INSERT INTO sgc_gwas_files
            (id, cohort_id, dataset, phenotype, ancestry, file_name, file_size, s3_path,
             uploaded_at, uploaded_by, column_mapping, metadata)
            VALUES (:id,:coh,:ds,:p,:a,'f',1,'s3://x',NOW(),'t','{}',:meta)"""),
            {"id": file_id, "coh": cohort_id, "ds": "DS." + name, "p": pheno, "a": anc,
             "meta": json.dumps({"sex": "All"})})
        c.commit()
    return file_id


def _ignored_file_ids(engine):
    return {r["file_id"] for r in query.list_ma_ignore(engine)}


def test_delete_all_removes_every_entry(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    for name in ("A", "B", "C"):
        query.insert_ma_ignore(engine, _make_gwas_file(engine, name), "r", "prev-reviewer")

    resp = api_client.delete("/api/sgc/ma/ignore")

    assert resp.status_code == 200 and resp.json() == {"removed": 3}
    assert query.list_ma_ignore(engine) == []


def test_delete_all_on_an_empty_list_removes_nothing(api_client):
    resp = api_client.delete("/api/sgc/ma/ignore")
    assert resp.status_code == 200 and resp.json() == {"removed": 0}


def test_delete_all_requires_review_permission(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    kept = _make_gwas_file(engine)
    query.insert_ma_ignore(engine, kept, "keep me", "prev-reviewer")
    app.dependency_overrides[get_sgc_user] = lambda: UPLOADER

    resp = api_client.delete("/api/sgc/ma/ignore")

    assert resp.status_code == 403
    assert _ignored_file_ids(engine) == {kept}


def test_delete_one_still_resolves_alongside_delete_all(api_client):
    """The new no-parameter DELETE must not shadow DELETE /sgc/ma/ignore/{ignore_id}."""
    engine = DataRegistryReadWriteDB().get_engine()
    keep = query.insert_ma_ignore(engine, _make_gwas_file(engine, "Keep"), "r", "prev-reviewer")
    drop = query.insert_ma_ignore(engine, _make_gwas_file(engine, "Drop"), "r", "prev-reviewer")

    resp = api_client.delete(f"/api/sgc/ma/ignore/{drop['id']}")

    assert resp.status_code == 200 and "deleted" in resp.json()["message"]
    assert _ignored_file_ids(engine) == {keep["file_id"]}
    assert api_client.delete(f"/api/sgc/ma/ignore/{'0' * 32}").status_code == 404
