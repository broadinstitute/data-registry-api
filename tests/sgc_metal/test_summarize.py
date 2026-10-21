import math

import pandas as pd
import pytest

from sgc_metal.summarize import (CAVEAT_FREQ_NONE, CAVEAT_FREQ_PASS1, CAVEAT_FREQ_PASS2, CAVEATS, build_caveats,
                                 build_summary, clump, summarize_tbl, totals_from_per_cohort)
from sgc_metal.tbl import MIN_PVALUE, TBL_COLUMNS, TOP_LOCI_COLUMNS

C1 = {"dataset": "d1", "file_id": "f1", "cohort": "A", "ancestry": "EUR", "sex": "Combined",
      "cases": 100, "controls": 200, "munge_n_in": 1000, "munge_n_out": 900, "munge_n_with_rsid": 800,
      "munge_ref_version": "v1"}
C2 = {"dataset": "d2", "file_id": "f2", "cohort": "B", "ancestry": "EAS", "sex": "Combined",
      "cases": 50, "controls": 70, "munge_n_in": 500, "munge_n_out": 450, "munge_n_with_rsid": 400,
      "munge_ref_version": "v1"}
C3 = {"dataset": "d3", "file_id": "f3", "cohort": "C", "ancestry": "AFR", "sex": "Combined",
      "cases": 10, "controls": 20, "munge_n_in": 100, "munge_n_out": 0, "munge_n_with_rsid": 0,
      "munge_ref_version": None}


def _write_tbl(path, rows):
    path.write_text("\t".join(TBL_COLUMNS) + "\n" + "".join("\t".join(r) + "\n" for r in rows))


def test_summarize_tbl_filters_singletons_and_writes_outputs(tmp_path):
    rows = [["1:100:A:G", "a", "g", "0.3", "0", "0.3", "0.3", "0.5", "0.05", "1e-25", "++", "0", "0", "1", "1", "10", "20"],
            ["1:150:A:G", "a", "g", "0.3", "0", "0.3", "0.3", "0.4", "0.05", "1e-20", "+-", "0", "0", "1", "1", "10", "20"],
            ["2:100:A:G", "a", "g", "0.3", "0", "0.3", "0.3", "0.4", "0.05", "1e-9", "+?", "0", "0", "0", "1", "5", "10"],
            ["3:100:A:G", "a", "g", "NA", "NA", "NA", "NA", "0.1", "0.05", "0.5", "+-", "0", "0", "1", "1", "10", "20"]]
    p = tmp_path / "x.tbl"
    _write_tbl(p, rows)
    st = summarize_tbl(str(p), str(tmp_path), "PSOR Combined")
    assert isinstance(st["meta_lambda_gc"], float) and st["meta_lambda_gc"] > 0
    assert st == {"n_meta_variants": 4, "n_meta_variants_2plus": 3, "meta_lambda_gc": pytest.approx(st["meta_lambda_gc"]),
                  "n_genome_wide_sig": 2, "n_top_loci": 1}
    top = pd.read_csv(tmp_path / "top_loci.tsv", sep="\t")
    assert list(top.columns) == TOP_LOCI_COLUMNS and len(top) == 1
    assert top.iloc[0]["position"] == 100 and top.iloc[0]["n_cohorts"] == 2 and top.iloc[0]["dir_concordance"] == 1.0
    assert (tmp_path / "manhattan.png").exists() and (tmp_path / "qq.png").exists()


def test_summarize_tbl_all_singletons(tmp_path):
    rows = [["1:100:A:G", "a", "g", "0.3", "0", "0.3", "0.3", "0.5", "0.05", "1e-25", "+?", "0", "0", "0", "1", "10", "20"]]
    p = tmp_path / "x.tbl"
    _write_tbl(p, rows)
    st = summarize_tbl(str(p), str(tmp_path), "X")
    assert st == {"n_meta_variants": 1, "n_meta_variants_2plus": 0, "meta_lambda_gc": None,
                  "n_genome_wide_sig": 0, "n_top_loci": 0}
    assert not (tmp_path / "manhattan.png").exists() and not (tmp_path / "qq.png").exists()
    assert (tmp_path / "top_loci.tsv").read_text().strip().split("\t") == TOP_LOCI_COLUMNS


def test_build_summary_contract_keys():
    s = build_summary(cohorts=[C1, C2, C3], used=[C1, C2], skipped=[{**C3, "reason": "not munged"}],
                      ignored=[{"cohort": "Z", "file_id": "z", "reason": "bad λ"}],
                      tbl_stats={"n_meta_variants": 10, "n_meta_variants_2plus": 8, "meta_lambda_gc": 1.01,
                                 "n_genome_wide_sig": 1, "n_top_loci": 1}, engine_version="METAL 2020-05-05")
    assert {"n_cohorts", "n_cohorts_used", "n_meta_variants", "meta_lambda_gc", "n_genome_wide_sig", "total_cases",
            "total_controls", "per_cohort", "caveats", "engine", "scheme", "n_meta_variants_2plus"} <= set(s)
    assert s["n_cohorts"] == 3 and s["n_cohorts_used"] == 2 and s["total_cases"] == C1["cases"] + C2["cases"]
    assert s["total_controls"] == C1["controls"] + C2["controls"]
    assert [c.get("skipped", False) for c in s["per_cohort"]] == [False, False, True, True]
    assert s["per_cohort"][0]["n_variants_in"] == C1["munge_n_in"] and s["per_cohort"][3]["reason"] == "MA ignore-list: bad λ"
    assert s["scheme"] == "STDERR" and s["engine"] == "METAL 2020-05-05" and s["caveats"] == CAVEATS
    assert s["per_cohort"][0]["n_markers_processed"] is None and s["per_cohort"][0]["n_allele_mismatch"] is None


def test_build_summary_merges_extra_and_freq_caveats_exist():
    stats = {"n_meta_variants": 1, "n_meta_variants_2plus": 1, "meta_lambda_gc": 1.0, "n_genome_wide_sig": 0,
             "n_top_loci": 0}
    base = build_summary(cohorts=[C1, C2], used=[C1, C2], skipped=[], ignored=[], tbl_stats=stats,
                         engine_version="METAL x")
    s = build_summary(cohorts=[C1, C2], used=[C1, C2], skipped=[], ignored=[], tbl_stats=stats,
                      engine_version="METAL x", extra={"freq_source": "pass1", "n_cohorts_with_eaf": 2})
    assert s == {**base, "freq_source": "pass1", "n_cohorts_with_eaf": 2}
    assert "freq_source" not in base and base["caveats"] == CAVEATS
    assert "second METAL pass" in CAVEAT_FREQ_PASS2 and "no cohort reports EAF" in CAVEAT_FREQ_NONE


def test_summarize_tbl_underflow_p_counts_and_leads(tmp_path):
    # METAL prints p below double range; float() would make them 0.0 and tie. The lead of a
    # window is the smallest log10p, the underflow rows are genome-wide significant, and the
    # plots/lambda get a finite (clamped) p.
    def row(m, p):
        return [m, "a", "g", "0.3", "0", "0.3", "0.3", "0.5", "0.05", p, "++", "0", "0", "1", "1", "10", "20"]
    rows = [row("6:31000100:A:G", "2.10e-3000"),   # weaker of the two underflow hits, listed first
            row("6:31000000:A:G", "6.41e-4346"),
            row("6:40000000:A:G", "1e-300"),
            row("1:100:A:G", "0.5"), row("2:100:A:G", "0.2")]
    p = tmp_path / "x.tbl"
    _write_tbl(p, rows)
    st = summarize_tbl(str(p), str(tmp_path), "PSOR Combined")
    assert st["n_genome_wide_sig"] == 3 and st["n_top_loci"] == 2 and st["n_meta_variants_2plus"] == 5
    assert math.isfinite(st["meta_lambda_gc"])
    top = pd.read_csv(tmp_path / "top_loci.tsv", sep="\t", dtype={"pvalue_str": str})
    assert list(top.columns) == TOP_LOCI_COLUMNS
    assert list(top["position"]) == [31000000, 40000000]
    assert list(top["pvalue_str"]) == ["6.41e-4346", "1e-300"]
    assert top.iloc[0]["log10p"] == pytest.approx(math.log10(6.41) - 4346)
    assert top.iloc[0]["pvalue"] == MIN_PVALUE
    assert (tmp_path / "manhattan.png").stat().st_size > 0 and (tmp_path / "qq.png").stat().st_size > 0


def test_summarize_tbl_top_loci_beta_oriented_to_alt(tmp_path):
    rows = [["1:100:A:G", "a", "g", "0.3000", "0", "0.3", "0.3", "0.5000", "0.05", "1e-25", "++", "0", "0", "1", "1",
             "10", "20"]]
    p = tmp_path / "x.tbl"
    _write_tbl(p, rows)
    summarize_tbl(str(p), str(tmp_path), "X")
    top = pd.read_csv(tmp_path / "top_loci.tsv", sep="\t")
    assert top.iloc[0]["beta"] == -0.5 and top.iloc[0]["freq1"] == pytest.approx(0.7)
    assert top.iloc[0]["orientation"] == "alt" and top.iloc[0]["allele1"] == "A"


def test_clump_sorts_by_log10p_and_windows():
    df = pd.DataFrame([{"chromosome": "1", "position": 1_000_000, "log10p": -10.0},
                       {"chromosome": "1", "position": 1_400_000, "log10p": -5000.0},
                       {"chromosome": "1", "position": 2_000_000, "log10p": -9.0},
                       {"chromosome": "2", "position": 1_000_000, "log10p": -math.inf}])
    out = clump(df)
    # 1:1.0M is within 500 kb of the stronger 1:1.4M; 1:2.0M is 600 kb away
    assert [(r.chromosome, r.position) for r in out.itertuples()] == [("2", 1_000_000), ("1", 1_400_000),
                                                                      ("1", 2_000_000)]
    assert list(clump(df, window=100_000)["position"]) == [1_000_000, 1_400_000, 1_000_000, 2_000_000]
    assert clump(df.iloc[:0]).empty


def test_totals_from_per_cohort():
    pc = [{"cases": 1, "controls": 2}, {"cases": None, "controls": 3}, {"cases": 10, "controls": 10, "skipped": True}]
    assert totals_from_per_cohort(pc) == (1, 5)
    assert totals_from_per_cohort([{"cases": None, "controls": None}]) == (None, None)


def test_build_caveats_by_freq_source_and_thresholds():
    c = build_caveats(freq_source="pass1", maf_min=0.01, info_min=0.8)
    assert c[:len(CAVEATS)] == CAVEATS
    assert any("MAF>=0.01 and INFO>=0.8" in x for x in c) and not any("0.005" in x for x in c)
    assert c[-1] == CAVEAT_FREQ_PASS1
    assert build_caveats(freq_source="pass2_eaf_cohorts", maf_min=0.005, info_min=0.3)[-1] == CAVEAT_FREQ_PASS2
    none = build_caveats(freq_source="none", maf_min=0.005, info_min=0.3)
    assert none[-1] == CAVEAT_FREQ_NONE and CAVEAT_FREQ_PASS1 not in none
    assert any("MAF>=0.005 and INFO>=0.3" in x for x in none)
    assert not any("allele strings" in x for x in none)
    mm = build_caveats(freq_source="none", maf_min=0.005, info_min=0.3, n_allele_mismatch=7)
    assert mm[-1] == ("7 cohort rows were dropped by METAL because their allele strings did not match the "
                      "marker's alleles (indels reported in a different form); see per_cohort.n_allele_mismatch")


def test_build_summary_per_cohort_metal_counts_and_caveats():
    a = {**C1, "metal_n_markers_processed": 880, "metal_n_allele_mismatch": 2}
    stats = {"n_meta_variants": 1, "n_meta_variants_2plus": 1, "meta_lambda_gc": 1.0, "n_genome_wide_sig": 0,
             "n_top_loci": 0}
    cav = build_caveats(freq_source="pass1", maf_min=0.005, info_min=0.3, n_allele_mismatch=2)
    s = build_summary(cohorts=[a, C2], used=[a, C2], skipped=[], ignored=[], tbl_stats=stats,
                      engine_version="METAL x", caveats=cav)
    assert s["per_cohort"][0]["n_markers_processed"] == 880 and s["per_cohort"][0]["n_allele_mismatch"] == 2
    assert s["caveats"] == cav and s["caveats"] is not cav
