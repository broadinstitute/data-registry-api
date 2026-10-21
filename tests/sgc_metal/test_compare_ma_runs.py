import gzip
import importlib.util
import math
from pathlib import Path

from click.testing import CliRunner

_PATH = Path(__file__).resolve().parents[2] / "scripts" / "sgc" / "compare_ma_runs.py"


def _load():
    spec = importlib.util.spec_from_file_location("compare_ma_runs", _PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


OLD_HDR = "chromosome\tposition\tref\talt\tbeta\tse\tpvalue\tn\tn_cohorts\tdir_concordance\ti2\n"
NEW_HDR = ("MarkerName\tAllele1\tAllele2\tFreq1\tFreqSE\tMinFreq\tMaxFreq\tEffect\tStdErr\tP-value\tDirection\t"
           "HetISq\tHetChiSq\tHetDf\tHetPVal\tNcases\tNcontrols\n")


def _old(ref, alt, beta, p):
    return f"{ref}\t{alt}\t{beta}\t0.1\t{p}\t1000\t2\t1\t0\n"


def _new(marker, a1, a2, eff, p, d="++"):
    return f"{marker}\t{a1}\t{a2}\t0.3\t0.01\t0.3\t0.3\t{eff}\t0.1\t{p}\t{d}\t0\t0\t1\t1\t10\t10\n"


def _write(path, text):
    with gzip.open(path, "wt") as fh:
        fh.write(text)
    return str(path)


def _files(tmp_path):
    old = OLD_HDR
    old += "1\t100\t" + _old("A", "G", 0.5, "1e-10")      # exact, Allele1 == alt
    old += "1\t200\t" + _old("A", "G", 0.4, "1e-9")       # Allele1 == ref -> negated
    old += "1\t300\t" + _old("T", "C", 0.3, "0.2")        # complement of A/G
    old += "1\t400\t" + _old("A", "G", 0.2, "0.5")        # allele mismatch
    old += "1\t500\t" + _old("A", "G", 0.1, "0.5")        # old only
    old += "1\t600\t" + _old("A", "G", 0.6, "1e-12")      # new p 1e-400
    new = NEW_HDR
    new += _new("1:100:A:G", "g", "a", 0.5, "2e-10")
    new += _new("1:200:A:G", "a", "g", -0.4, "3e-9")      # effect of ref allele -> oriented +0.4
    new += _new("1:300:A:G", "a", "g", -0.25, "0.1")      # old T/C alt=C -> comp G; Allele1 a != g -> -(-0.25)
    new += _new("1:400:A:C", "a", "c", 0.2, "0.5")
    new += _new("1:700:A:G", "a", "g", 0.2, "0.5")        # new only
    new += _new("1:800:A:G", "a", "g", 0.2, "0.5", d="+?")  # singleton
    new += _new("1:600:A:G", "g", "a", 0.7, "1e-400")
    return _write(tmp_path / "meta.tsv.gz", old), _write(tmp_path / "metal.tbl.gz", new)


def test_compare_counts_and_orientation(tmp_path):
    m = _load()
    old, new = _files(tmp_path)
    res = m.compare(old, new)
    assert res["counts"] == {"old_variants": 6, "new_rows_total": 7, "new_rows_2plus": 6,
                             "new_singletons": 1, "singletons_included": False}
    o = res["overlap"]
    assert (o["both"], o["old_only"], o["new_only"]) == (5, 1, 1)
    assert (o["allele_match"], o["allele_mismatch"]) == (4, 1)
    assert o["mismatch_examples"][0]["position"] == 400
    a = res["agreement"]
    assert a["n_variants"] == 4 and a["sign_agreement_pct"] == 100.0
    # oriented betas 0.5, 0.4, 0.25, 0.7 vs old 0.5, 0.4, 0.3, 0.6
    assert math.isclose(a["median_abs_dbeta"], 0.025)
    assert a["beta_pearson_r"] > 0.9
    assert a["gws_table"] == {"both": 3, "old_only": 0, "new_only": 0, "neither": 1}
    assert res["top_loci"]["n_old_leads"] == 1 and res["top_loci"]["n_new_leads"] == 1
    assert res["top_loci"]["old_leads_with_new_within_500kb"] == 1
    assert res["lambda_gc"]["new"] is not None


def test_orientation_negation_and_complement(tmp_path):
    m = _load()
    old, new = _files(tmp_path)
    res = m.compare(old, new, sample=None)
    # 1:200 negated effect (-0.4 -> 0.4) and 1:300 complement (-0.25 -> 0.25) keep signs concordant
    assert res["agreement"]["sign_agreement_pct"] == 100.0


def test_include_singletons(tmp_path):
    m = _load()
    old, new = _files(tmp_path)
    res = m.compare(old, new, include_singletons=True)
    assert res["overlap"]["new_only"] == 2
    assert res["counts"]["singletons_included"] is True


def test_sign_disagreement_and_sample(tmp_path):
    m = _load()
    old = _write(tmp_path / "o.gz", OLD_HDR + "1\t1\t" + _old("A", "G", 0.5, "1e-10"))
    new = _write(tmp_path / "n.gz", NEW_HDR + _new("1:1:A:G", "a", "g", 0.5, "1e-10"))  # Allele1=ref -> -0.5
    res = m.compare(old, new, sample=1)
    assert res["agreement"]["sign_agreement_pct"] == 0.0
    assert res["agreement"]["gws_table"]["both"] == 1


def test_cli_markdown_and_json(tmp_path):
    m = _load()
    old, new = _files(tmp_path)
    out = tmp_path / "r.json"
    r = CliRunner().invoke(m.main, ["--old", old, "--new", new, "--json", str(out)])
    assert r.exit_code == 0, r.output
    assert "Lambda GC" in r.output and "per-cohort inputs are not compared" in r.output
    assert out.exists()
