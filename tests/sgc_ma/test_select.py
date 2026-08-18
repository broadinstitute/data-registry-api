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

def test_include_file_excludes_grch37():
    assert include_file(_row(genome_build="GRCh37")) is False

def test_include_file_allows_meta_analysis_named_uploads():
    # Rotterdam's contributed GWAS are named meta_analysis_* (they meta-analyze
    # RS-I/II/III internally). Portal MA products never land in sgc_gwas_files
    # (they live under their own run-id S3 prefix + sgc_gwas_ma_results), so
    # dataset naming must not exclude an upload.
    assert include_file(_row(dataset="meta_analysis_atopic_dermatitis_full")) is True

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


def test_selection_sql_ignore_join_is_file_keyed():
    from sgc_ma.select import _SQL
    assert "LEFT JOIN sgc_ma_ignore mi ON mi.file_id = f.id" in _SQL
    assert ":target_ancestry" not in _SQL and ":target_sex" not in _SQL
    assert "WHERE f.phenotype = :phenotype" in _SQL


def test_select_cohorts_drops_ignored_files():
    # a cohort's only eligible EUR/All file is ignored -> cohort contributes nothing
    rows = [_cand_row(file_id="1", cohort_id="c1", ancestry="EUR", sex="All", ignore_reason="bad"),
            _cand_row(file_id="2", cohort_id="c2", ancestry="EUR", sex="All", ignore_reason=None)]
    # select_cohorts needs >=... just assert the ignored file is excluded from the resolved set
    from sgc_ma.select import resolve_target, include_file
    eligible = [r for r in rows if include_file(r) and r.get("ignore_reason") is None]
    selected, _ = resolve_target(eligible, "EUR", "All")
    assert [r["file_id"] for r in selected] == ["2"]


def test_selection_sql_selects_file_ancestry():
    from sgc_ma.select import _SQL
    assert "f.ancestry AS ancestry" in _SQL


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


from sgc_ma.select import resolve_target


def _f(cohort_id, ancestry, sex, file_id="x"):
    return dict(file_id=file_id, cohort_id=cohort_id, ancestry=ancestry, sex=sex,
                dataset="d", cohort="C", genome_build="GRCh38")


def test_resolve_combined_prefers_combined_else_fallback():
    rows = [
        _f("multi", "Combined", "All", "m-comb"),
        _f("multi", "EUR", "All", "m-eur"),          # multi-ancestry: also has per-ancestry
        _f("single", "EUR", "All", "s-eur"),         # single-ancestry fallback cohort
    ]
    selected, warnings = resolve_target(rows, "Combined", "All")
    got = {r["cohort_id"]: r["file_id"] for r in selected}
    assert got == {"multi": "m-comb", "single": "s-eur"} and warnings == []


def test_resolve_combined_ambiguous_fallback_skips_and_warns():
    rows = [_f("c", "EUR", "All", "e"), _f("c", "AFR", "All", "a")]   # no combined, two All
    selected, warnings = resolve_target(rows, "Combined", "All")
    assert selected == [] and warnings and warnings[0]["cohort_id"] == "c"


def test_resolve_sex_stratified_any_ancestry_deduped_prefers_combined():
    rows = [
        _f("biovu", "Combined", "Female", "b-comb"),
        _f("biovu", "SAS", "Female", "b-sas"),       # BioVU stray -> dropped
        _f("gnh", "SAS", "Female", "g-sas"),         # single-ancestry -> kept
        _f("x", "EUR", "All", "x-all"),              # wrong sex -> excluded
    ]
    selected, _ = resolve_target(rows, "Combined", "Female")
    got = {r["cohort_id"]: r["file_id"] for r in selected}
    assert got == {"biovu": "b-comb", "gnh": "g-sas"}


def test_resolve_ancestry_stratified_requires_ancestry_and_all():
    rows = [
        _f("a", "EUR", "All", "a-eur"),
        _f("b", "EUR", "Male", "b-eur-m"),           # wrong sex
        _f("c", "AFR", "All", "c-afr"),              # wrong ancestry
    ]
    selected, _ = resolve_target(rows, "EUR", "All")
    assert [r["file_id"] for r in selected] == ["a-eur"]


from sgc_ma.select import matches_target


@pytest.mark.parametrize("target_ancestry,target_sex,row,expected", [
    # Combined/All: any ancestry accepted at sex='All', other sexes rejected
    ("Combined", "All", dict(ancestry="EUR", sex="All"), True),
    ("Combined", "All", dict(ancestry="SAS", sex="All"), True),
    ("Combined", "All", dict(ancestry="SAS", sex="Female"), False),
    # Combined/Female: any ancestry accepted at sex='Female' only
    ("Combined", "Female", dict(ancestry="SAS", sex="Female"), True),
    ("Combined", "Female", dict(ancestry="SAS", sex="All"), False),
    ("Combined", "Female", dict(ancestry="SAS", sex="Male"), False),
    # ancestry-stratified (e.g. EUR/All): exact ancestry AND sex must be 'All'
    ("EUR", "All", dict(ancestry="EUR", sex="All"), True),
    ("EUR", "All", dict(ancestry="EUR", sex="Female"), False),   # stratified targets are sex='All' only
    ("EUR", "All", dict(ancestry="AFR", sex="All"), False),      # wrong ancestry
])
def test_matches_target(target_ancestry, target_sex, row, expected):
    assert matches_target(row, target_ancestry, target_sex) is expected


from types import SimpleNamespace
from sgc_ma.select import list_ma_candidates, select_cohorts_by_file_ids, select_cohorts


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
                cohort_id="c1", cohort="CohortA", ignore_reason=None, ancestry="EUR",
                sex="All", genome_build="GRCh38")
    base.update(kw); return base


def test_list_ma_candidates_resolves_target_and_maps():
    rows = [_cand_row(file_id="1", cohort_id="c1"),
            _cand_row(file_id="2", cohort_id="c2", dataset="meta_analysis_x")]  # included: naming is not identity
    out = list_ma_candidates(_FakeEngine(rows), "PH", "EUR", "All")
    assert [c["file_id"] for c in out] == ["1", "2"]
    assert out[0]["cohort"] == "CohortA" and "ignored" not in out[0]


def test_list_ma_candidates_drops_ignored_files():
    # matches select_cohorts: ignored files are dropped before resolve, so the
    # manual-launch preview can't diverge from what auto/battery actually submits.
    rows = [_cand_row(file_id="1", cohort_id="c1", ignore_reason="high lambda"),
            _cand_row(file_id="2", cohort_id="c2", ignore_reason=None)]
    out = list_ma_candidates(_FakeEngine(rows), "PH", "EUR", "All")
    assert [c["file_id"] for c in out] == ["2"]
    assert "ignored" not in out[0]


def test_select_cohorts_drops_ignored_file_before_resolve():
    # direct test of select_cohorts() itself (not just resolve_target()): one
    # cohort's only eligible file is ignored and must contribute nothing, while
    # the other cohort's clean file is selected.
    rows = [_cand_row(file_id="1", cohort_id="c1", ignore_reason="bad", ancestry="EUR", sex="All"),
            _cand_row(file_id="2", cohort_id="c2", ignore_reason=None, ancestry="EUR", sex="All")]
    out = select_cohorts(_FakeEngine(rows), "PH", "EUR", "All")
    assert [r["file_id"] for r in out] == ["2"]


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


from sgc_ma.select import ignored_cohorts


def _ignored_row(**kw):
    base = dict(file_id="f1", cohort="CohortA", dataset="DS.A", ancestry="EUR",
               reason="high lambda", sex="All", genome_build="GRCh38")
    base.update(kw); return base


def test_ignored_cohorts_sql_selects_bucket_fields():
    from sgc_ma.select import _IGNORED_SQL
    # same expressions as _SQL, so filtering here can't drift from selection
    assert "JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.sex')) AS sex" in _IGNORED_SQL
    assert ("COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build')), "
            "JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))) AS genome_build") in _IGNORED_SQL
    assert "LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id" in _IGNORED_SQL
    assert "WHERE f.phenotype = :phenotype" in _IGNORED_SQL


def test_ignored_cohorts_drops_wrong_sex_for_combined_all_target():
    # the exact prod bug (run d89831dada85428e86a8d0007362466c): a Female-stratified
    # ignore-list file must not show up as "Ignored" on a Combined/All run, where it
    # was never a candidate in the first place.
    rows = [_ignored_row(file_id="f591ffbc", cohort="Born in Bradford CoreExome SAS",
                         ancestry="SAS", sex="Female", reason="high lambda")]
    out = ignored_cohorts(_FakeEngine(rows), "SUBSTANCE_DERM", "Combined", "All")
    assert out == []


def test_ignored_cohorts_drops_grch37_entry():
    rows = [_ignored_row(genome_build="GRCh37", ancestry="Combined", sex="All")]
    out = ignored_cohorts(_FakeEngine(rows), "PH", "Combined", "All")
    assert out == []


def test_ignored_cohorts_keeps_legitimate_same_bucket_entry():
    rows = [
        _ignored_row(file_id="keep", ancestry="EUR", sex="All", reason="lambda too high"),
        _ignored_row(file_id="drop", ancestry="AFR", sex="All", reason="wrong ancestry bucket"),
    ]
    out = ignored_cohorts(_FakeEngine(rows), "PH", "EUR", "All")
    assert [r["file_id"] for r in out] == ["keep"]
    assert out[0]["reason"] == "lambda too high"
    assert out[0]["cohort"] == "CohortA" and out[0]["dataset"] == "DS.A"
    assert out[0]["ancestry"] == "EUR" and out[0]["sex"] == "All"


def test_ignored_cohorts_reports_meta_analysis_named_uploads():
    # An ignore-listed upload named meta_analysis_* is still a real candidate
    # for the bucket, so its ignore entry must show in the run summary.
    rows = [_ignored_row(dataset="meta_analysis_ph_full", ancestry="Combined", sex="All")]
    out = ignored_cohorts(_FakeEngine(rows), "PH", "Combined", "All")
    assert [r["dataset"] for r in out] == ["meta_analysis_ph_full"]
