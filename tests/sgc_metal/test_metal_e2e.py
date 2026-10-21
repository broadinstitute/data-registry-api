import os
import shutil

import pytest

from sgc_metal.script import outfile_prefix, render_script, tbl_path
from sgc_metal.tbl import iter_tbl
from tests.sgc_metal.fixtures import write_munged

pytestmark = pytest.mark.skipif(
    shutil.which(os.environ.get("METAL_BIN", "metal")) is None,
    reason="metal binary not installed",
)


def _cohort_a():
    return [
        {"SNP_ID": "1:100:A:G", "Chromosome": "1", "Position": 100, "Effect_allele": "G",
         "Non-effect_allele": "A", "Beta": 0.2, "SE": 0.1, "P-value": 0.05, "Effect_AF": 0.3,
         "N_CAS": 100, "N_CON": 200},
        {"SNP_ID": "1:200:C:T", "Chromosome": "1", "Position": 200, "Effect_allele": "T",
         "Non-effect_allele": "C", "Beta": -0.1, "SE": 0.1, "P-value": 0.3, "Effect_AF": 0.4,
         "N_CAS": 100, "N_CON": 200},
    ]


def _cohort_b(na_extras=False):
    rows = [
        {"SNP_ID": "1:100:A:G", "Chromosome": "1", "Position": 100, "Effect_allele": "A",
         "Non-effect_allele": "G", "Beta": -0.2, "SE": 0.1, "P-value": 0.05, "Effect_AF": 0.7,
         "N_CAS": 50, "N_CON": 80},
        {"SNP_ID": "1:300:G:A", "Chromosome": "1", "Position": 300, "Effect_allele": "A",
         "Non-effect_allele": "G", "Beta": 0.3, "SE": 0.1, "P-value": 0.01, "Effect_AF": 0.2,
         "N_CAS": 50, "N_CON": 80},
    ]
    if na_extras:
        for r in rows:
            for k in ("Effect_AF", "N_CAS", "N_CON"):
                r[k] = "NA"
    return rows


def _meta_cohort(dataset, file_id, cases, controls, **kw):
    return {"dataset": dataset, "file_id": file_id, "cohort": dataset, "cases": cases, "controls": controls,
            "munge_s3_key": f"k_{file_id}", "munge_n_in": 2, "munge_n_out": 2, "munge_has_eaf": True,
            "munge_maf_min": 0.005, "munge_info_min": 0.3, **kw}


def _row(snp_id, effect_allele, other_allele, beta, se, p, eaf):
    chrom, pos = snp_id.split(":")[:2]
    return {"SNP_ID": snp_id, "Chromosome": chrom, "Position": pos, "Effect_allele": effect_allele,
            "Non-effect_allele": other_allele, "Beta": beta, "SE": se, "P-value": p, "Effect_AF": eaf,
            "N_CAS": 100, "N_CON": 200}


def _run(tmp_path, cohort_b):
    from sgc_metal.run_metal import run_metal_script

    work = tmp_path / "work"
    work.mkdir()
    write_munged(str(work / "A.tsv.gz"), _cohort_a())
    write_munged(str(work / "B.tsv.gz"), cohort_b)
    prefix = outfile_prefix("T", "X", "All")
    script = render_script(["work/A.tsv.gz", "work/B.tsv.gz"], prefix)
    run_metal_script(script, str(tmp_path))
    return {f"{r.chromosome}:{r.position}": r
            for r in iter_tbl(tbl_path(str(tmp_path), prefix))}


def test_metal_two_cohorts_direction_and_custom_sums(tmp_path):
    rows = _run(tmp_path, _cohort_b())
    r = rows["1:100"]
    assert r.n_cohorts == 2 and r.dir_concordance == 1.0
    assert r.ncases == 100 + 50 and r.ncontrols == 200 + 80
    effect = -r.effect if r.allele1 == "A" else r.effect
    assert abs(effect - 0.2) < 1e-6
    freq_g = r.freq1 if r.allele1 == "G" else 1 - r.freq1
    assert 0.29 < freq_g < 0.31
    assert rows["1:300"].n_cohorts == 1 and "?" in rows["1:300"].direction


def test_metal_na_frequency_keeps_marker(tmp_path):
    # Cohort B reports no EAF. Alone, METAL would count its NA frequency as 0 (Freq1 0.35 for
    # 1:100); meta_analyze's second, EAF-cohorts-only pass must restore A's 0.30.
    from sgc_metal.run_metal import meta_analyze

    b_rows = _cohort_b()
    for r in b_rows:
        r["Effect_AF"] = "NA"  # counts kept: B still contributes N_CAS/N_CON
    data = {"fa": _cohort_a(), "fb": b_rows}
    cohorts = [_meta_cohort("A", "fa", 100, 200, munge_has_eaf=True),
               _meta_cohort("B", "fb", 50, 80, munge_has_eaf=False)]
    s = meta_analyze(cohorts, lambda co, p: write_munged(p, data[co["file_id"]]), str(tmp_path / "w"),
                     str(tmp_path / "o"), phenotype="T", ancestry="X", sex="All", ignored=[],
                     engine_version="METAL e2e")
    rows = {f"{r.chromosome}:{r.position}": r for r in iter_tbl(str(tmp_path / "o" / "metal.tbl.gz"))}
    r = rows["1:100"]
    freq_g = r.freq1 if r.allele1 == "G" else 1 - r.freq1
    observed = f"freq1={r.freq1!r} (G {freq_g:.4f}) ncases={r.ncases!r} ncontrols={r.ncontrols!r}"
    print(f"METAL NA-handling observed (two-pass): {observed}")
    assert r.n_cohorts == 2, observed
    assert 0.29 < freq_g < 0.31, observed
    assert r.ncases == 150 and r.ncontrols == 280, observed
    assert rows["1:300"].freq1 is None  # B-only marker: no EAF cohort saw it
    assert rows["1:200"].freq1 is not None
    assert s["freq_source"] == "pass2_eaf_cohorts" and s["n_cohorts_with_eaf"] == 1
    # the frequency pass is a plain ANALYZE, which real METAL accepts (Direction width checked)
    assert (tmp_path / "o" / "metal_freq.txt").read_text().endswith(".tbl\nANALYZE\nQUIT\n")
    assert s["metal_runs"]["freq"]["metal_peak_rss_mb"] > 0 and s["metal_runs"]["main"]["metal_elapsed_s"] >= 0


def _no_snp_id(path):
    import gzip
    with gzip.open(path, "wt") as fh:  # munge layout minus the MARKER column
        fh.write("Chromosome\tPosition\tEffect_allele\tNon-effect_allele\tBeta\tSE\tP-value\n")
        fh.write("1\t100\tG\tA\t0.2\t0.1\t0.05\n")


@pytest.mark.parametrize("case", ["missing_marker_column", "missing_file", "unknown_freqlabel"])
def test_metal_exit_zero_dropped_input_is_caught(tmp_path, case):
    # METAL 2020-05-05 exits 0 and still writes .tbl/.tbl.info after skipping an
    # input; check_metal_output must turn that into a MetalError.
    from sgc_metal.run_metal import MetalError, check_metal_output, run_metal_script

    work = tmp_path / "work"
    work.mkdir()
    write_munged(str(work / "A.tsv.gz"), _cohort_a())
    write_munged(str(work / "B.tsv.gz"), _cohort_b())
    paths = ["work/A.tsv.gz", "work/B.tsv.gz", "work/C.tsv.gz"]
    if case == "missing_marker_column":
        _no_snp_id(str(work / "C.tsv.gz"))
    elif case == "unknown_freqlabel":
        # the rejected per-file EAF opt-out: METAL drops the whole file instead
        write_munged(str(work / "C.tsv.gz"), _cohort_a())
    prefix = outfile_prefix("T", "X", "All")
    script = render_script(paths, prefix)
    if case == "unknown_freqlabel":
        script = script.replace("PROCESS work/C.tsv.gz", "FREQLABEL __no_freq__\nPROCESS work/C.tsv.gz")
    log = run_metal_script(script, str(tmp_path))  # exit 0
    assert os.path.exists(tbl_path(str(tmp_path), prefix))
    with pytest.raises(MetalError, match="metal dropped input"):
        check_metal_output(log, tbl_path(str(tmp_path), prefix), len(paths))


def test_metal_all_inputs_used_passes_check(tmp_path):
    from sgc_metal.run_metal import check_metal_output
    _run(tmp_path, _cohort_b())
    prefix = outfile_prefix("T", "X", "All")
    check_metal_output(str(tmp_path / "metal.log"), tbl_path(str(tmp_path), prefix), 2)


def test_metal_top_loci_orientation_underflow_and_allele_mismatch(tmp_path):
    import math

    import pandas as pd

    from sgc_metal.run_metal import meta_analyze

    a = [_row("1:100:A:G", "A", "G", -0.5, 0.05, 1e-20, 0.7),     # first cohort reports REF as effect
         _row("2:200:C:T", "T", "C", 5.0, 0.05, 1e-300, 0.4),     # |z| ~141: p below double range
         _row("2:300:C:T", "T", "C", 4.0, 0.05, 1e-300, 0.4),     # also below range, weaker, same window
         _row("1:300:AT:A", "A", "AT", 0.1, 0.05, 0.05, 0.3),     # deletion in the marker's own form
         _row("3:100:A:G", "G", "A", 0.1, 0.05, 0.05, 0.3)]       # first cohort's effect allele is ALT
    b = [_row("1:100:A:G", "G", "A", 0.5, 0.05, 1e-20, 0.3),
         _row("2:200:C:T", "T", "C", 5.0, 0.05, 1e-300, 0.4),
         _row("2:300:C:T", "T", "C", 4.0, 0.05, 1e-300, 0.4),
         _row("1:300:AT:A", "T", "TA", 0.1, 0.05, 0.05, 0.3),     # same deletion reported as TA/T
         _row("3:100:A:G", "G", "A", 0.1, 0.05, 0.05, 0.3)]
    data = {"fa": a, "fb": b}
    s = meta_analyze([_meta_cohort("A", "fa", 100, 200), _meta_cohort("B", "fb", 100, 200)],
                     lambda co, p: write_munged(p, data[co["file_id"]]), str(tmp_path / "w"), str(tmp_path / "o"),
                     phenotype="T", ancestry="X", sex="All", ignored=[], engine_version="METAL e2e")
    out = tmp_path / "o"
    log = (out / "metal.log").read_text()
    print("METAL log (per-file section):\n" + log[log.index("## Processing file"):log.index("## Set output")])
    print("metal.tbl.info:\n" + (out / "metal.tbl.info").read_text())

    rows = {f"{r.chromosome}:{r.position}": r for r in iter_tbl(str(out / "metal.tbl.gz"))}
    assert rows["1:100"].allele1 == "A"  # REF, as the first cohort reported it
    # METAL sorts each marker's alleles (despite .tbl.info's "first allele ... in the first file"):
    # the first cohort's effect allele was G, METAL's Allele1 is still A = REF
    assert (rows["3:100"].allele1, rows["3:100"].allele2) == ("A", "G") and abs(rows["3:100"].effect + 0.1) < 1e-6
    assert rows["2:200"].pvalue_str.endswith("e-4346") and rows["2:200"].log10p < -4000
    assert rows["1:300"].n_cohorts == 1  # B's TA/T row was dropped by METAL

    top = pd.read_csv(out / "top_loci.tsv", sep="\t", dtype={"chromosome": str, "pvalue_str": str})
    print(top.to_string())
    assert [(c, p) for c, p in zip(top["chromosome"], top["position"])] == [("2", 200), ("1", 100)]
    assert top.iloc[0]["pvalue_str"] == rows["2:200"].pvalue_str and top.iloc[0]["log10p"] < rows["2:300"].log10p
    assert top.iloc[0]["pvalue"] > 0 and math.isfinite(s["meta_lambda_gc"])
    lead = top.iloc[1]
    assert lead["orientation"] == "alt" and abs(lead["beta"] - 0.5) < 1e-6  # effect of ALT G, not of A
    assert 0.29 < lead["freq1"] < 0.31  # frequency of G
    assert s["n_genome_wide_sig"] == 3 and s["n_top_loci"] == 2

    per = {c["dataset"]: c for c in s["per_cohort"]}
    assert (per["A"]["n_markers_processed"], per["A"]["n_allele_mismatch"]) == (5, 0)
    assert (per["B"]["n_markers_processed"], per["B"]["n_allele_mismatch"]) == (4, 1)
    assert s["caveats"][-1].startswith("1 cohort rows were dropped by METAL because their allele strings")
    assert s["freq_source"] == "pass1" and s["metal_runs"]["freq"] is None
