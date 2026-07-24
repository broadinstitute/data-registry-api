from scripts.backfill_sgc_ma_totals import needs_backfill


def test_needs_backfill_accepts_succeeded_row_with_summary():
    assert needs_backfill({"status": "SUCCEEDED", "summary_json_s3_key": "sgc/ma/PH/EUR/summary.json"})


def test_needs_backfill_rejects_unfinished_or_artifactless_rows():
    # a run still going, or one that failed before writing a summary, has nothing to sum
    assert not needs_backfill({"status": "RUNNING", "summary_json_s3_key": "k"})
    assert not needs_backfill({"status": "FAILED", "summary_json_s3_key": "k"})
    assert not needs_backfill({"status": "SUCCEEDED", "summary_json_s3_key": None})
    assert not needs_backfill({"status": "SUCCEEDED"})
