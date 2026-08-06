"""Unit tests for the file-id based MA ignore-list bulk-upload parser."""
from dataregistry.api.sgc import parse_bulk_ignore_rows


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
