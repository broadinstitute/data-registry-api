import json

import pytest


def test_to_worker_column_mapping():
    from sgc_liftover.submit_liftover_batch import to_worker_column_mapping
    sgc = {"col_chromosome": "CHR", "col_position": "POS",
           "col_effect_allele": "A1", "col_non_effect_allele": "A2",
           "col_beta": "BETA", "col_se": "SE", "col_pvalue": "P"}
    assert to_worker_column_mapping(sgc) == {
        "chromosome": "CHR", "position": "POS", "ref": "A2", "alt": "A1"}


def test_to_worker_column_mapping_missing_key_raises():
    from sgc_liftover.submit_liftover_batch import to_worker_column_mapping
    with pytest.raises(ValueError):
        to_worker_column_mapping({"col_chromosome": "CHR"})   # missing pos/alleles


def test_ucsc_source_build():
    from sgc_liftover.submit_liftover_batch import ucsc_source_build
    assert ucsc_source_build("GRCh37") == "hg19"
    with pytest.raises(ValueError):
        ucsc_source_build("GRCh38")   # already target — should never be lifted
    with pytest.raises(ValueError):
        ucsc_source_build(None)


def test_selection_sql_finds_non_grch38_without_completed_job():
    from sgc_liftover.submit_liftover_batch import _LIFT_LIST_SQL
    assert "LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id" in _LIFT_LIST_SQL
    # exclude files that already have a SUCCEEDED or in-flight (PENDING/RUNNING) lift
    assert "lj.status IN ('SUCCEEDED','PENDING','RUNNING')" in _LIFT_LIST_SQL


def test_callback_flips_build_on_success(monkeypatch):
    import sgc_liftover.submit_liftover_batch as drv
    calls = {}
    monkeypatch.setattr(drv.query, "update_sgc_liftover_job",
                        lambda *a, **k: calls.setdefault("job", k))
    monkeypatch.setattr(drv.query, "set_sgc_gwas_file_build",
                        lambda engine, file_id, build: calls.setdefault("flip", (file_id, build)))
    cb = drv.make_liftover_callback("FILE1")
    cb(object(), 'LIFTOVER_SUMMARY_JSON: {"lifted": 3}', "LID1", "SUCCEEDED")
    assert calls["job"]["status"] == "SUCCEEDED" and calls["job"]["summary"] == {"lifted": 3}
    assert calls["flip"] == ("FILE1", "GRCh38")


def test_callback_does_not_flip_on_failure(monkeypatch):
    import sgc_liftover.submit_liftover_batch as drv
    flips = []
    monkeypatch.setattr(drv.query, "update_sgc_liftover_job", lambda *a, **k: None)
    monkeypatch.setattr(drv.query, "set_sgc_gwas_file_build",
                        lambda *a, **k: flips.append(a))
    cb = drv.make_liftover_callback("FILE1")
    cb(object(), 'boom', "LID1", "FAILED")
    assert flips == []   # build NOT flipped on failure


def test_callback_parses_nested_summary_json_in_full(monkeypatch):
    # Regression for the non-greedy `\{.*?\}` bug: the worker's summary is
    # nested (unmapped_breakdown, per_chromosome are dicts), so a non-greedy
    # match truncated at the first inner `}` and json.loads raised, silently
    # dropping the summary. The greedy regex must capture the whole object.
    import sgc_liftover.submit_liftover_batch as drv
    nested_summary = {
        "total_lifted": 5,
        "unmapped_breakdown": {"partial": 2},
        "per_chromosome": {"1": {"lifted": 5}},
    }
    complete_log = "LIFTOVER_SUMMARY_JSON: " + json.dumps(nested_summary, separators=(",", ":"))

    calls = {}
    monkeypatch.setattr(drv.query, "update_sgc_liftover_job",
                        lambda *a, **k: calls.setdefault("job", k))
    monkeypatch.setattr(drv.query, "set_sgc_gwas_file_build",
                        lambda engine, file_id, build: calls.setdefault("flip", (file_id, build)))
    cb = drv.make_liftover_callback("FILE1")
    cb(object(), complete_log, "LID1", "SUCCEEDED")

    assert calls["job"]["summary"] == nested_summary   # parsed in full, not truncated
    assert calls["job"]["status"] == "SUCCEEDED"
    assert calls["flip"] == ("FILE1", "GRCh38")


def test_partition_liftable_splits_grch37_grch38_and_unrecognized():
    from sgc_liftover.submit_liftover_batch import _partition_liftable
    rows = [
        {"file_id": "F37", "genome_build": "GRCh37"},
        {"file_id": "F19", "genome_build": "hg19"},
        {"file_id": "F38", "genome_build": "GRCh38"},   # already target -- not liftable, not reported
        {"file_id": "FBAD", "genome_build": "hg17"},    # unrecognized -- must be reported
        {"file_id": "FNONE", "genome_build": None},     # absent -- must be reported
    ]
    liftable, unrecognized = _partition_liftable(rows)
    assert [r["file_id"] for r in liftable] == ["F37", "F19"]
    assert [r["file_id"] for r in unrecognized] == ["FBAD", "FNONE"]
