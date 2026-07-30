"""Unit tests for the target-based MA ignore-list bulk-upload parser + validation."""
import asyncio
import io
from starlette.datastructures import UploadFile, Headers
from dataregistry.api.sgc import parse_bulk_ignore_rows, is_valid_bucket
from dataregistry.api import sgc, query
from dataregistry.api.model import User


def test_tsv_with_header():
    content = "code\tphenotype\tancestry\tsex\treason\nBV\tPSOR\tEUR\tAll\tlow N\n"
    assert parse_bulk_ignore_rows(content) == [
        {"code": "BV", "phenotype": "PSOR", "ancestry": "EUR", "sex": "All", "reason": "low N"}]


def test_csv_no_header_positional():
    content = "BV,PSOR,Combined,Male,bad lambda\n"
    assert parse_bulk_ignore_rows(content) == [
        {"code": "BV", "phenotype": "PSOR", "ancestry": "Combined", "sex": "Male", "reason": "bad lambda"}]


def test_sex_defaults_to_all_when_absent():
    content = "code,phenotype,ancestry\nBV,PSOR,EUR\n"
    assert parse_bulk_ignore_rows(content) == [
        {"code": "BV", "phenotype": "PSOR", "ancestry": "EUR", "sex": "All", "reason": ""}]


def test_quoted_comma_reason_and_blank_lines():
    content = 'code,phenotype,ancestry,sex,reason\n\nBV,PSOR,EUR,All,"low N, ambiguous"\n\n'
    assert parse_bulk_ignore_rows(content) == [
        {"code": "BV", "phenotype": "PSOR", "ancestry": "EUR", "sex": "All", "reason": "low N, ambiguous"}]


def test_rows_without_code_skipped_and_empty():
    assert parse_bulk_ignore_rows(",PSOR,EUR,All,x\n") == []
    assert parse_bulk_ignore_rows("") == []


def test_is_valid_bucket():
    assert is_valid_bucket("Combined", "All")
    assert is_valid_bucket("Combined", "Male")
    assert is_valid_bucket("EUR", "All")
    assert not is_valid_bucket("AFR", "Male")      # non-All sex requires pooled ancestry
    assert not is_valid_bucket("XX", "All")        # unknown ancestry
    assert not is_valid_bucket("EUR", "Other")     # unknown sex


def _user():
    return User(user_name="reviewer", first_name=None, last_name=None, email=None, avatar=None,
                is_active=True, roles=[], groups=None, permissions=["sgc-review-data"],
                is_internal=True, api_token=None, id=1)


def _upload(text_content):
    return UploadFile(filename="x.csv", file=io.BytesIO(text_content.encode()),
                      headers=Headers({"content-type": "text/csv"}))


def test_bulk_endpoint_partial_success(monkeypatch):
    monkeypatch.setattr(query, "resolve_cohort_code",
                        lambda engine, code: "c" * 32 if code.upper() == "BV" else None)
    applied = []
    monkeypatch.setattr(query, "insert_ma_ignore",
                        lambda engine, cohort_id, phenotype, ancestry, sex, reason, excluded_by:
                        applied.append((cohort_id, ancestry, sex)) or {"id": "1"})
    content = ("code,phenotype,ancestry,sex,reason\n"
               "BV,PSOR,Combined,Male,ok\n"       # valid
               "NOPE,PSOR,EUR,All,x\n"            # unknown code
               "BV,PSOR,AFR,Male,x\n")            # invalid bucket
    res = asyncio.run(sgc.bulk_add_sgc_ma_ignore(file=_upload(content), user=_user()))
    assert res["added"] == 1 and res["skipped_count"] == 2
    assert applied == [("c" * 32, "Combined", "Male")]
    reasons = {s["code"]: s["reason"] for s in res["skipped"]}
    assert "unknown cohort code" in reasons["NOPE"]
    assert "invalid target" in reasons["BV"]
