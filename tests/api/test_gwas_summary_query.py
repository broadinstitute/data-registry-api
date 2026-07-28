"""Shape guards for the augmented gwas-summary query. The row-value logic is
unit-tested via classify_liftover_status (tests/sgc_ma/test_select.py); the
actual SQL/JSON wiring is verified end-to-end on QA. These guards catch a
dropped JOIN or renamed helper column in review, mirroring
tests/sgc_liftover/test_submit_liftover_batch.py::test_selection_sql_finds_*."""
from dataregistry.api.query import _ALL_SGC_GWAS_FILES_SQL


def test_summary_sql_coalesces_build_from_file_then_cohort():
    assert "LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id" in _ALL_SGC_GWAS_FILES_SQL
    assert "COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build'))" in _ALL_SGC_GWAS_FILES_SQL
    assert "JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))" in _ALL_SGC_GWAS_FILES_SQL
    assert "AS genome_build_raw" in _ALL_SGC_GWAS_FILES_SQL


def test_summary_sql_selects_latest_liftover_status():
    assert "FROM sgc_liftover_jobs lj" in _ALL_SGC_GWAS_FILES_SQL
    assert "WHERE lj.file_id = f.id" in _ALL_SGC_GWAS_FILES_SQL
    assert "ORDER BY lj.submitted_at DESC" in _ALL_SGC_GWAS_FILES_SQL
    assert "AS latest_liftover_status" in _ALL_SGC_GWAS_FILES_SQL
