from dataregistry.api.peg_validation import validate_peg_files
from tests.peg_fixtures import helpers


def _run(files):
    return validate_peg_files(
        files["list"][1], files["list"][0],
        files["matrix"][1], files["matrix"][0],
        files["metadata"][1], files["metadata"][0],
    )


def test_valid_submission_reports_success():
    report = _run(helpers.valid_files())
    assert report["status"] == "success"
    assert set(report["files"]) == {"list", "matrix", "metadata", "cross_validation"}
    assert report["summary"]["total_errors"] == 0
    assert report["files"]["list"]["path"] == helpers.LIST_NAME
    assert report["files"]["matrix"]["path"] == helpers.MATRIX_NAME
    assert report["files"]["metadata"]["path"] == helpers.METADATA_NAME
    assert report["files"]["cross_validation"]["path"] is None
    assert report["files"]["cross_validation"]["counts"]["info"] >= 1


def test_broken_list_header_is_error_and_skips_cross_validation():
    report = _run(helpers.broken_list_header(helpers.valid_files()))
    assert report["status"] == "error"
    assert report["files"]["list"]["status"] == "error"
    assert any("GeneSymbol" in e["message"] for e in report["files"]["list"]["errors"])
    cross = report["files"]["cross_validation"]
    assert cross["counts"]["errors"] == 0
    assert any(w["step"] == "Cross-validation skipped" for w in cross["warnings"])


def test_broken_matrix_row_has_column_errors():
    report = _run(helpers.broken_matrix_row(helpers.valid_files()))
    assert report["status"] == "error"
    matrix_errors = report["files"]["matrix"]["errors"]
    col = next(e for e in matrix_errors if "column_errors" in e)
    first = col["column_errors"][0]
    assert first["column"] == "PrimaryVariantID"
    assert first["rows"] == [1]
    assert first["hint"]


def test_metadata_missing_sheet_is_error():
    report = _run(helpers.metadata_missing_sheet(helpers.valid_files()))
    assert report["status"] == "error"
    assert any(helpers.last_sheet_name() in e["message"] for e in report["files"]["metadata"]["errors"])


def test_garbage_metadata_does_not_raise():
    report = _run(helpers.garbage_metadata(helpers.valid_files()))
    assert report["status"] == "error"
    assert report["files"]["metadata"]["counts"]["errors"] >= 1


def test_cross_validation_string_details_are_folded_into_message():
    report = _run(helpers.broken_list_row_mismatch(helpers.valid_files()))
    assert report["status"] == "error"
    cross_errors = report["files"]["cross_validation"]["errors"]
    assert any("chr2:20000:A:G" in e["message"] for e in cross_errors)


def test_validator_exception_becomes_parse_error(monkeypatch):
    import dataregistry.api.peg_validation as pv

    def boom(path, error_limit=50):
        raise RuntimeError("toolkit blew up")

    monkeypatch.setattr(pv, "validate_list", boom)
    report = _run(helpers.valid_files())
    assert report["status"] == "error"
    err = report["files"]["list"]["errors"][0]
    assert err["step"] == "List - Parse"
    assert "toolkit blew up" in err["message"]
