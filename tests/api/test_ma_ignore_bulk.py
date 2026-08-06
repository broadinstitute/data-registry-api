"""Unit tests for the file-id based MA ignore-list bulk-upload parser, plus end-to-end
tests of the bulk-replace and delete-all endpoints against the test DB."""
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


def _upload(api_client, content: str):
    return api_client.post("/api/sgc/ma/ignore/bulk",
                           files={"file": ("ignore.csv", content.encode("utf-8"), "text/csv")})


def _ignored_file_ids(engine):
    return {r["file_id"] for r in query.list_ma_ignore(engine)}


def test_bulk_upload_replaces_the_whole_list(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    old_a, old_b = _make_gwas_file(engine, "OldA"), _make_gwas_file(engine, "OldB")
    new_id = _make_gwas_file(engine, "New")
    query.insert_ma_ignore(engine, old_a, "stale", "prev-reviewer")
    query.insert_ma_ignore(engine, old_b, "stale", "prev-reviewer")

    resp = _upload(api_client, f"file_id,reason,excluded_by\n{new_id},fresh list,Jake\n")

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"added": 1, "removed": 2, "skipped_count": 0, "skipped": []}
    assert _ignored_file_ids(engine) == {new_id}


def test_bulk_upload_keeps_entries_that_are_in_the_new_file(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    keep, drop = _make_gwas_file(engine, "Keep"), _make_gwas_file(engine, "Drop")
    query.insert_ma_ignore(engine, keep, "old reason", "prev-reviewer")
    query.insert_ma_ignore(engine, drop, "old reason", "prev-reviewer")

    resp = _upload(api_client, f"file_id,reason,excluded_by\n{keep},new reason,Jake\n")

    assert resp.json() == {"added": 1, "removed": 2, "skipped_count": 0, "skipped": []}
    rows = query.list_ma_ignore(engine)
    assert [r["file_id"] for r in rows] == [keep]
    assert rows[0]["reason"] == "new reason" and rows[0]["excluded_by"] == "Jake"


def test_bulk_upload_defaults_excluded_by_to_the_uploading_user(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    file_id = _make_gwas_file(engine)
    _upload(api_client, f"file_id,reason\n{file_id},no excluded_by column\n")
    assert query.list_ma_ignore(engine)[0]["excluded_by"] == REVIEWER.user_name


def test_bulk_upload_mixed_file_replaces_with_valid_rows_and_reports_skipped(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    old_id, good_id = _make_gwas_file(engine, "Old"), _make_gwas_file(engine, "Good")
    query.insert_ma_ignore(engine, old_id, "stale", "prev-reviewer")
    bogus = "0" * 32

    resp = _upload(api_client, f"file_id,reason,excluded_by\n{good_id},ok,Jake\n{bogus},nope,Jake\n")

    body = resp.json()
    assert body["added"] == 1 and body["removed"] == 1 and body["skipped_count"] == 1
    assert body["skipped"] == [{"file_id": bogus, "reason": "no GWAS file with that id"}]
    assert _ignored_file_ids(engine) == {good_id}


def test_bulk_upload_with_no_parseable_rows_400s_and_leaves_the_list_alone(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    kept = _make_gwas_file(engine)
    query.insert_ma_ignore(engine, kept, "keep me", "prev-reviewer")

    resp = _upload(api_client, ",no id,x\n")

    assert resp.status_code == 400
    assert _ignored_file_ids(engine) == {kept}


def test_bulk_upload_with_only_invalid_file_ids_400s_and_leaves_the_list_alone(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    kept = _make_gwas_file(engine)
    query.insert_ma_ignore(engine, kept, "keep me", "prev-reviewer")

    resp = _upload(api_client, "file_id,reason,excluded_by\n" + "0" * 32 + ",nope,Jake\n")

    assert resp.status_code == 400
    assert "unchanged" in resp.json()["detail"]
    assert _ignored_file_ids(engine) == {kept}


def test_bulk_upload_requires_review_permission(api_client):
    engine = DataRegistryReadWriteDB().get_engine()
    kept = _make_gwas_file(engine)
    query.insert_ma_ignore(engine, kept, "keep me", "prev-reviewer")
    app.dependency_overrides[get_sgc_user] = lambda: UPLOADER

    resp = _upload(api_client, "file_id\n" + _make_gwas_file(engine, "Other") + "\n")

    assert resp.status_code == 403
    assert _ignored_file_ids(engine) == {kept}


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
