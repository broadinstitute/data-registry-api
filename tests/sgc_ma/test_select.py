import pytest
from sgc_ma.select import normalize_build, include_file, _SQL

@pytest.mark.parametrize("raw,expected", [
    ("GRCh38", "GRCh38"),
    ("GRCh38 / hg38", "GRCh38"),
    ("GrCh38", "GRCh38"),
    ("GRCh37 liftover to GRCh38", "GRCh38"),
    ("GRCh37", "GRCh37"),
    ("hg19", "GRCh37"),
    (None, None),
])
def test_normalize_build(raw, expected):
    assert normalize_build(raw) == expected

def _row(**kw):
    base = dict(sex="All", genome_build="GRCh38", dataset="CHOP.v1")
    base.update(kw); return base

def test_include_file_happy():
    assert include_file(_row()) is True

def test_include_file_excludes_sex_subsets():
    assert include_file(_row(sex="Male")) is False
    assert include_file(_row(sex="Female")) is False

def test_include_file_excludes_grch37():
    assert include_file(_row(genome_build="GRCh37")) is False

def test_include_file_excludes_preexisting_ma():
    assert include_file(_row(dataset="meta_analysis_atopic_dermatitis_full")) is False

def test_selection_sql_requires_qc_success_and_reads_cohort_build():
    assert "p.status = 'SUCCEEDED'" in _SQL
    assert "sgc_gwas_cohorts" in _SQL
    assert "$.genome_build" in _SQL
