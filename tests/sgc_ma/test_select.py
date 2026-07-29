import pytest
from sgc_ma.select import normalize_build, include_file, _SQL
from sgc_ma.select import classify_liftover_status

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


@pytest.mark.parametrize("build,job_status,expected", [
    ("GRCh38", None, "GRCh38 (native)"),
    ("GRCh37", None, "Needs liftover"),
    (None, None, "Unknown build"),
    ("GRCh37", "PENDING", "In progress"),
    ("GRCh37", "RUNNING", "In progress"),
    ("GRCh38", "SUCCEEDED", "Lifted to GRCh38"),
    ("GRCh37", "SUCCEEDED", "Lifted to GRCh38"),   # re-run after failure resolved
    ("GRCh37", "FAILED", "Failed"),
    (None, "FAILED", "Failed"),
    (None, "SUCCEEDED", "Lifted to GRCh38"),        # job status wins over unknown build
])
def test_classify_liftover_status(build, job_status, expected):
    assert classify_liftover_status(build, job_status) == expected

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

def test_selection_sql_exposes_registry_cohort_name():
    # sc.name is the authoritative disambiguator when the free-text `dataset`
    # label was mis-copied between cohorts (the "two GEL datasets" bug).
    assert "LEFT JOIN sgc_cohorts sc ON sc.id = f.cohort_id" in _SQL
    assert "sc.name AS cohort" in _SQL
    assert "AS cohort_id" in _SQL


def test_selection_sql_left_joins_ignore_list():
    from sgc_ma.select import _SQL
    assert ("LEFT JOIN sgc_ma_ignore mi ON mi.cohort_id = f.cohort_id "
            "AND mi.phenotype = f.phenotype AND mi.ancestry = f.ancestry") in _SQL
    assert "mi.reason AS ignore_reason" in _SQL


def test_not_ignored_predicate():
    from sgc_ma.select import not_ignored
    assert not_ignored({"ignore_reason": None}) is True
    assert not_ignored({}) is True                       # column absent -> not ignored
    assert not_ignored({"ignore_reason": "lambda too high"}) is False


def test_selection_sql_prefers_per_file_build():
    from sgc_ma.select import _SQL
    # a lifted file carries its own metadata.genome_build; prefer it over the cohort's
    assert ("COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build')), "
            "JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))) AS genome_build") in _SQL


from types import SimpleNamespace
from sgc_ma.select import list_ma_candidates, select_cohorts_by_file_ids


class _FakeResult:
    def __init__(self, rows): self._rows = rows
    def __iter__(self):
        for r in self._rows:
            yield SimpleNamespace(_mapping=r)


class _FakeConn:
    def __init__(self, rows): self._rows = rows
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, *a, **k): return _FakeResult(self._rows)


class _FakeEngine:
    def __init__(self, rows): self._rows = rows
    def connect(self): return _FakeConn(self._rows)


def _cand_row(**kw):
    base = dict(file_id="1", dataset="A", s3_path="p", column_mapping="{}", cases=1, controls=1,
                cohort_id="c1", cohort="CohortA", ignore_reason=None, sex="All", genome_build="GRCh38")
    base.update(kw); return base


def test_list_ma_candidates_filters_and_maps():
    rows = [_cand_row(file_id="1"),
            _cand_row(file_id="2", sex="Male"),                       # excluded: sex subset
            _cand_row(file_id="3", dataset="meta_analysis_x")]        # excluded: MA product
    out = list_ma_candidates(_FakeEngine(rows), "PH", "EUR")
    assert [c["file_id"] for c in out] == ["1"]
    assert out[0]["cohort"] == "CohortA" and out[0]["ignored"] is False


def test_list_ma_candidates_flags_ignored_but_keeps_it():
    rows = [_cand_row(file_id="1", ignore_reason="high lambda")]
    out = list_ma_candidates(_FakeEngine(rows), "PH", "EUR")
    assert len(out) == 1 and out[0]["ignored"] is True               # shown, flagged, not dropped


def test_select_cohorts_by_file_ids_coerces_and_normalizes():
    rows = [_cand_row(file_id="1", column_mapping='{"col_chromosome": "CHR"}',
                      genome_build="GRCh37 liftover to GRCh38")]
    out = select_cohorts_by_file_ids(_FakeEngine(rows), ["1"])
    assert out[0]["column_mapping"] == {"col_chromosome": "CHR"}     # JSON -> dict
    assert out[0]["genome_build"] == "GRCh38"                        # normalized


def test_select_cohorts_by_file_ids_empty_shortcircuits():
    assert select_cohorts_by_file_ids(_FakeEngine([]), []) == []


def test_sql_by_ids_targets_id_in():
    from sgc_ma.select import _SQL_BY_IDS
    assert "WHERE f.id IN :file_ids" in _SQL_BY_IDS
    assert "f.phenotype = :phenotype" not in _SQL_BY_IDS
