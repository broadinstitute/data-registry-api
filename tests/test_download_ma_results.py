"""Unit tests for the pure logic in scripts/sgc/download_ma_results.py."""
import importlib.util
import os

_SCRIPT = os.path.join(os.path.dirname(__file__), "..", "scripts", "sgc", "download_ma_results.py")
_spec = importlib.util.spec_from_file_location("download_ma_results", _SCRIPT)
assert _spec is not None and _spec.loader is not None
dl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dl)


def _run(pheno="PSOR", anc="EUR", sex="All", status="SUCCEEDED",
         created: "str | None" = "2026-08-10T13:00:00", **kw):
    return {"id": kw.get("id", "r1"), "phenotype": pheno, "ancestry": anc, "sex": sex,
            "status": status, "created_at": created, **kw}


class TestSelectLatestRuns:
    def test_keeps_only_succeeded(self):
        runs = [_run(status="SUCCEEDED", id="a"), _run(anc="AFR", status="FAILED", id="b"),
                _run(anc="EAS", status="RUNNING", id="c")]
        selected = dl.select_latest_runs(runs)
        assert [r["id"] for r in selected] == ["a"]

    def test_one_per_target_latest_by_created_at(self):
        runs = [_run(id="old", created="2026-08-01T00:00:00"),
                _run(id="new", created="2026-08-10T00:00:00"),
                _run(id="other-target", anc="AFR", created="2026-07-01T00:00:00")]
        selected = dl.select_latest_runs(runs)
        assert {r["id"] for r in selected} == {"new", "other-target"}

    def test_targets_differ_by_sex(self):
        runs = [_run(id="all", sex="All"), _run(id="male", sex="Male")]
        assert len(dl.select_latest_runs(runs)) == 2

    def test_missing_created_at_sorts_first(self):
        runs = [_run(id="undated", created=None), _run(id="dated", created="2026-08-10T00:00:00")]
        selected = dl.select_latest_runs(runs)
        assert [r["id"] for r in selected] == ["dated"]

    def test_stable_output_order(self):
        runs = [_run(pheno="ZOSTER", id="z"), _run(pheno="ACNE", id="a"),
                _run(pheno="ACNE", anc="AFR", id="b")]
        selected = dl.select_latest_runs(runs)
        assert [r["id"] for r in selected] == ["b", "a", "z"]


class TestBaseName:
    def test_convention_is_pheno_sex_ancestry(self):
        assert dl.base_name(_run(pheno="ATOPIC_DERM", anc="Combined", sex="All")) \
            == "ATOPIC_DERM_All_Combined"

    def test_unsafe_chars_replaced(self):
        assert dl.base_name(_run(pheno="A/B", anc="E U R", sex="All")) == "A-B_All_E-U-R"


class TestManifestRow:
    def test_row_fields(self):
        run = _run(total_cases=10, total_controls=20, meta_lambda_gc=1.01,
                   n_meta_variants=5, n_genome_wide_sig=1, n_cohorts_used=3)
        row = dl.manifest_row(run)
        assert row["phenotype"] == "PSOR" and row["sex"] == "All" and row["ancestry"] == "EUR"
        assert row["run_id"] == "r1" and row["total_cases"] == 10
        assert row["meta_lambda_gc"] == 1.01 and row["n_genome_wide_sig"] == 1

    def test_missing_optionals_blank(self):
        row = dl.manifest_row(_run())
        assert row["total_cases"] == "" and row["meta_lambda_gc"] == ""
