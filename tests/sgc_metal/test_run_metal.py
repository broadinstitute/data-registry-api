import gzip
import json
import os
import re
import signal
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from sgc_metal import run_metal
from sgc_metal.summarize import CAVEAT_FREQ_NONE, CAVEAT_FREQ_PASS1, CAVEAT_FREQ_PASS2
from sgc_metal.tbl import TBL_COLUMNS
from tests.sgc_metal.fixtures import write_munged

# Two variants seen in both cohorts (one genome-wide significant) + one singleton.
ROWS = [["1:100:A:G", "a", "g", "0.3", "0", "0.3", "0.3", "0.5", "0.05", "1e-25", "++", "0", "0", "1", "1", "10", "20"],
        ["2:100:A:G", "a", "g", "0.3", "0", "0.3", "0.3", "0.1", "0.05", "0.2", "+-", "0", "0", "1", "1", "10", "20"],
        ["3:100:A:G", "a", "g", "0.3", "0", "0.3", "0.3", "0.1", "0.05", "0.5", "+?", "0", "0", "0", "1", "5", "10"]]


@pytest.fixture(autouse=True)
def _no_recorded_metal_commit(tmp_path, monkeypatch):
    # hermetic metal_version(): the image's /usr/local/share/metal_commit.txt must not leak in
    monkeypatch.setattr(run_metal, "METAL_COMMIT_FILE", str(tmp_path / "no_metal_commit.txt"))


def _write_tbl(path, rows):
    path.write_text("\t".join(TBL_COLUMNS) + "\n" + "".join("\t".join(r) + "\n" for r in rows))


def _write_munged(path, n_rows):
    rows = [{"SNP_ID": f"1:{100 + i}:A:G", "Chromosome": "1", "Position": 100 + i, "Effect_allele": "G",
             "Non-effect_allele": "A", "Beta": 0.1, "SE": 0.05, "P-value": 0.04, "Effect_AF": 0.3,
             "N_CAS": 10, "N_CON": 20} for i in range(n_rows)]
    write_munged(str(path), rows)


def _cohort(dataset, file_id, **kw):
    return {"dataset": dataset, "file_id": file_id, "cohort": dataset, "cases": 1, "controls": 1,
            "munge_s3_key": f"sgc/munge/{file_id}/standardised.tsv.gz", "munge_n_in": 10, "munge_n_out": 9,
            "munge_n_with_rsid": 5, "munge_has_eaf": True, "munge_maf_min": 0.005, "munge_info_min": 0.3, **kw}


def _fake_run(scripts):
    """A stand-in for run_metal_script: records the script, writes canned METAL outputs into work."""
    def fake_run(script, work):
        scripts.append(script)
        _write_tbl(Path(work, "PSOR_Combined_All_1.tbl"), ROWS)
        Path(work, "PSOR_Combined_All_1.tbl.info").write_text("info")
        Path(work, "metal.txt").write_text(script)
        Path(work, "metal.log").write_text("ok")
        return str(Path(work, "metal.log"))
    return fake_run


def _analyze(tmp_path, cohorts, fetch, **kw):
    args = dict(phenotype="PSOR", ancestry="Combined", sex="All", ignored=[], engine_version="METAL test")
    args.update(kw)
    return run_metal.meta_analyze(cohorts, fetch, str(tmp_path / "w"), str(tmp_path / "o"), **args)


def test_meta_analyze_happy(tmp_path, monkeypatch):
    def fake_run(script, work):  # assert script content and produce outputs
        assert "PROCESS work/A_f1.tsv.gz\nPROCESS work/B_f2.tsv.gz\nOUTFILE PSOR_Combined_All_ .tbl" in script
        assert Path(work, "work", "A_f1.tsv.gz").exists() and Path(work, "work", "B_f2.tsv.gz").exists()
        _write_tbl(Path(work, "PSOR_Combined_All_1.tbl"), ROWS)
        Path(work, "PSOR_Combined_All_1.tbl.info").write_text("info")
        Path(work, "metal.txt").write_text(script)
        Path(work, "metal.log").write_text("ok")
        return str(Path(work, "metal.log"))
    monkeypatch.setattr(run_metal, "run_metal_script", fake_run)
    cohorts = [_cohort("B", "f2", cases=1, controls=1, munge_s3_key="k2", munge_n_out=9),
               _cohort("A", "f1", cases=2, controls=2, munge_s3_key="k1", munge_n_out=8, munge_n_with_rsid=4)]
    fetched = []
    s = run_metal.meta_analyze(cohorts, lambda co, p: (fetched.append(co["munge_s3_key"]), _write_munged(p, 3)),
                               str(tmp_path / "w"), str(tmp_path / "o"),
                               phenotype="PSOR", ancestry="Combined", sex="All", ignored=[],
                               engine_version="METAL test")
    assert fetched == ["k1", "k2"]  # sorted by dataset
    assert s["n_cohorts_used"] == 2 and s["per_cohort"][0]["dataset"] == "A"
    assert s["per_cohort"][0]["n_variants_used"] == 8
    assert s["engine"] == "METAL test" and s["total_cases"] == 3
    assert s["n_meta_variants"] == 3 and s["n_meta_variants_2plus"] == 2 and s["n_genome_wide_sig"] == 1
    for n in run_metal.UPLOAD_NAMES:  # single pass: everything but the frequency-pass artifacts
        assert (tmp_path / "o" / n).exists() == (n not in FREQ_NAMES), n
    assert s["freq_source"] == "pass1" and s["n_cohorts_with_eaf"] == 2
    assert not any("second METAL pass" in c or "no cohort reports EAF" in c for c in s["caveats"])
    assert CAVEAT_FREQ_PASS1 in s["caveats"] and any("MAF>=0.005 and INFO>=0.3" in c for c in s["caveats"])
    assert set(s["metal_runs"]) == {"main", "freq"} and s["metal_runs"]["freq"] is None
    main_run = s["metal_runs"]["main"]
    assert set(main_run) == {"metal_elapsed_s", "metal_peak_rss_mb"} and main_run["metal_elapsed_s"] >= 0
    assert main_run["metal_peak_rss_mb"] > 0
    assert not list((tmp_path / "w" / "work").iterdir())  # staged inputs deleted after the last METAL pass
    assert gzip.open(tmp_path / "o/metal.tbl.gz", "rt").readline().startswith("MarkerName")
    assert (tmp_path / "o/metal.tbl.info").read_text() == "info"
    assert (tmp_path / "o/metal.log").read_text() == "ok"
    assert json.loads((tmp_path / "o/summary.json").read_text()) == s
    tsv = (tmp_path / "o/summary.tsv").read_text().splitlines()
    assert tsv[0].split("\t")[0] == "dataset" and len(tsv) == 3


def test_meta_analyze_sanitises_dataset_in_input_paths(tmp_path, monkeypatch):
    scripts = []
    monkeypatch.setattr(run_metal, "run_metal_script", _fake_run(scripts))
    cohorts = [_cohort("A B/c", "f1"), _cohort("D", "f2")]
    _analyze(tmp_path, cohorts, lambda co, p: _write_munged(p, 2))
    assert "PROCESS work/A_B_c_f1.tsv.gz\nPROCESS work/D_f2.tsv.gz\n" in scripts[0]


def test_run_metal_skips_empty_input(tmp_path, monkeypatch):
    scripts = []
    monkeypatch.setattr(run_metal, "run_metal_script", _fake_run(scripts))
    cohorts = [_cohort("A", "f1"), _cohort("B", "f2"), _cohort("C", "f3")]

    def fetch(co, p):
        _write_munged(p, 0 if co["dataset"] == "B" else 3)  # B: header only

    s = _analyze(tmp_path, cohorts, fetch)
    assert scripts[0].count("PROCESS ") == 2 and "PROCESS work/B_f2.tsv.gz" not in scripts[0]
    assert s["n_cohorts"] == 3 and s["n_cohorts_used"] == 2
    skipped = [c for c in s["per_cohort"] if c.get("skipped")]
    assert [(c["dataset"], c["reason"]) for c in skipped] == [("B", "empty munged file")]


def test_run_metal_fails_below_two_inputs(tmp_path, monkeypatch):
    def no_run(*a, **k):
        raise AssertionError("METAL must not run with < 2 inputs")
    monkeypatch.setattr(run_metal, "run_metal_script", no_run)
    fetched = []
    cohorts = [_cohort("A", "f1"), _cohort("B", "f2", munge_s3_key=None)]
    with pytest.raises(run_metal.MetalError, match="fewer than 2 munged inputs"):
        _analyze(tmp_path, cohorts, lambda co, p: (fetched.append(co["dataset"]), _write_munged(p, 3)))
    assert fetched == ["A"]  # the unmunged cohort is never downloaded


def test_meta_analyze_reports_not_munged_and_ignored(tmp_path, monkeypatch):
    monkeypatch.setattr(run_metal, "run_metal_script", _fake_run([]))
    cohorts = [_cohort("A", "f1"), _cohort("B", "f2"), _cohort("C", "f3", munge_s3_key=None)]
    ignored = [{"cohort": "Z", "dataset": "Z", "file_id": "fz", "reason": "bad lambda"}]
    s = _analyze(tmp_path, cohorts, lambda co, p: _write_munged(p, 1), ignored=ignored)
    reasons = {c["dataset"]: c["reason"] for c in s["per_cohort"] if c.get("skipped")}
    assert reasons == {"C": "not munged", "Z": "MA ignore-list: bad lambda"}
    assert s["n_cohorts"] == 3 and s["n_cohorts_used"] == 2


def test_meta_analyze_missing_tbl_raises(tmp_path, monkeypatch):
    def run_without_output(script, work):
        Path(work, "metal.log").write_text("no markers\n")
        return str(Path(work, "metal.log"))
    monkeypatch.setattr(run_metal, "run_metal_script", run_without_output)
    with pytest.raises(run_metal.MetalError, match="no markers"):
        _analyze(tmp_path, [_cohort("A", "f1"), _cohort("B", "f2")], lambda co, p: _write_munged(p, 1))


def _metal_run_writing(tbl_rows, log_text):
    def fake_run(script, work):
        _write_tbl(Path(work, "PSOR_Combined_All_1.tbl"), tbl_rows)
        Path(work, "PSOR_Combined_All_1.tbl.info").write_text("info")
        Path(work, "metal.txt").write_text(script)
        Path(work, "metal.log").write_text(log_text)
        return str(Path(work, "metal.log"))
    return fake_run


@pytest.mark.parametrize("bad_line", ["## ERROR: No 'SNP_ID' column found",
                                      "## Failed to open file 'work/B_f2.tsv.gz'"])
def test_meta_analyze_raises_when_metal_log_reports_dropped_input(tmp_path, monkeypatch, bad_line):
    # METAL exits 0 after skipping an input; the log is the only signal.
    log = "## Processing file 'work/A_f1.tsv.gz'\n" + bad_line + "\n" + "".join(f"## line {i}\n" for i in range(60))
    monkeypatch.setattr(run_metal, "run_metal_script", _metal_run_writing(ROWS, log))
    with pytest.raises(run_metal.MetalError) as ei:
        _analyze(tmp_path, [_cohort("A", "f1"), _cohort("B", "f2")], lambda co, p: _write_munged(p, 1))
    assert bad_line in str(ei.value)  # the offending line survives even when it is outside the log tail
    assert not (tmp_path / "o" / "summary.json").exists()
    # the logs reach out_dir before the error propagates (main uploads them on FAILED)
    assert (tmp_path / "o" / "metal.log").read_text() == log and (tmp_path / "o" / "metal.txt").exists()


def test_meta_analyze_ignores_non_error_log_lines(tmp_path, monkeypatch):
    log = "# ERROR is mentioned in help text\n## WARNING: Invalid standard errors for 2 markers\n"
    monkeypatch.setattr(run_metal, "run_metal_script", _metal_run_writing(ROWS, log))
    s = _analyze(tmp_path, [_cohort("A", "f1"), _cohort("B", "f2")], lambda co, p: _write_munged(p, 1))
    assert s["n_cohorts_used"] == 2


def test_meta_analyze_raises_on_direction_width_mismatch(tmp_path, monkeypatch):
    narrow = [r[:10] + ["+"] + r[11:] for r in ROWS]  # 1-char Direction for 2 inputs
    monkeypatch.setattr(run_metal, "run_metal_script", _metal_run_writing(narrow, "ok\n"))
    with pytest.raises(run_metal.MetalError, match="Direction width 1 != 2 inputs"):
        _analyze(tmp_path, [_cohort("A", "f1"), _cohort("B", "f2")], lambda co, p: _write_munged(p, 1))


def test_check_metal_output_allows_header_only_tbl(tmp_path):
    tbl = tmp_path / "x1.tbl"
    _write_tbl(tbl, [])
    Path(str(tbl) + ".info").write_text("info")
    (tmp_path / "metal.log").write_text("ok\n")
    run_metal.check_metal_output(str(tmp_path / "metal.log"), str(tbl), 2)


def test_truncate_error_keeps_head_and_tail():
    msg = "H" * 3000 + "T" * 3000
    out = run_metal.truncate_error(msg)
    assert len(out) == 4000 and out.startswith("H") and out.endswith("T" * 100)
    assert "chars omitted" in out
    assert run_metal.truncate_error("short") == "short"


FREQ_NAMES = {"metal_freq.tbl.gz", "metal_freq.tbl.info", "metal_freq.log", "metal_freq.txt"}


def _row(marker, a1, a2, freq1, freqse, minf, maxf, direction="+++", effect="0.1"):
    return [marker, a1, a2, freq1, freqse, minf, maxf, effect, "0.05", "0.01", direction, "0", "0", "2", "1",
            "30", "60"]


def _fake_metal(calls, pass1_rows, pass2_rows=(), pass2_log="ok\n"):
    """A two-pass-aware stand-in for run_metal_script: picks rows by the script's OUTFILE prefix."""
    def fake_run(script, work):
        calls.append((script, work))
        prefix = re.search(r"^OUTFILE (\S+) \.tbl$", script, re.M).group(1)
        freq_pass = prefix.endswith("freq_")
        os.makedirs(work, exist_ok=True)
        _write_tbl(Path(work, prefix + "1.tbl"), list(pass2_rows if freq_pass else pass1_rows))
        Path(work, prefix + "1.tbl.info").write_text("freq info" if freq_pass else "info")
        Path(work, "metal.txt").write_text(script)
        Path(work, "metal.log").write_text(pass2_log if freq_pass else "ok\n")
        return str(Path(work, "metal.log"))
    return fake_run


def _final_rows(tmp_path):
    with gzip.open(tmp_path / "o" / "metal.tbl.gz", "rt") as fh:
        lines = fh.read().splitlines()
    assert lines[0].split("\t") == TBL_COLUMNS
    return {ln.split("\t")[0]: ln.split("\t") for ln in lines[1:]}


def test_meta_analyze_mixed_eaf_runs_frequency_pass_and_joins(tmp_path, monkeypatch):
    calls = []
    pass1 = [_row("2:50:A:C", "a", "c", "0.1100", "0.1100", "0.0000", "0.2200"),   # in pass 2, same orientation
             _row("1:100:A:G", "a", "g", "0.3500", "0.3500", "0.0000", "0.7000"),  # in pass 2, alleles swapped
             _row("10:5:C:T", "c", "t", "0.4000", "0.4000", "0.0000", "0.8000", direction="?+?")]  # B only
    pass2 = [_row("1:100:A:G", "G", "A", "0.2500", "0.0200", "0.2000", "0.4000", direction="++"),
             _row("2:50:A:C", "a", "c", "0.2200", "0.0100", "0.2100", "0.2300", direction="++"),
             _row("3:7:A:T", "a", "t", "0.5000", "0.0000", "0.5000", "0.5000", direction="+?")]
    inputs_at_pass2 = []
    fake = _fake_metal(calls, pass1, pass2)

    def recording(script, work):
        if work.endswith("freq"):
            inputs_at_pass2.extend(sorted(os.listdir(os.path.join(os.path.dirname(work), "work"))))
        return fake(script, work)
    monkeypatch.setattr(run_metal, "run_metal_script", recording)
    cohorts = [_cohort("C", "f3"), _cohort("B", "f2", munge_has_eaf=False), _cohort("A", "f1")]
    s = _analyze(tmp_path, cohorts, lambda co, p: _write_munged(p, 2))

    assert len(calls) == 2
    (script1, work1), (script2, work2) = calls
    assert work1 == str(tmp_path / "w") and work2 == str(tmp_path / "w" / "freq")
    assert script1.count("PROCESS ") == 3
    assert script1.endswith("ANALYZE HETEROGENEITY\nQUIT\n") and script2.endswith(".tbl\nANALYZE\nQUIT\n")
    # only the EAF cohorts, same sorted order, paths resolving from work_dir/freq
    assert "PROCESS ../work/A_f1.tsv.gz\nPROCESS ../work/C_f3.tsv.gz\nOUTFILE PSOR_Combined_All_freq_ .tbl" in script2
    assert "B_f2" not in script2
    # pass 2's PROCESS paths resolved from work_dir/freq while it ran; B (no EAF) was already deleted
    assert inputs_at_pass2 == ["A_f1.tsv.gz", "C_f3.tsv.gz"]
    assert not list((tmp_path / "w" / "work").iterdir())
    # raw pass-1/pass-2 tables deleted once joined; the joined table is what was summarised
    assert not (tmp_path / "w" / "PSOR_Combined_All_1.tbl").exists()
    assert not (tmp_path / "w" / "freq" / "PSOR_Combined_All_freq_1.tbl").exists()

    rows = _final_rows(tmp_path)
    assert set(rows) == {"2:50:A:C", "1:100:A:G", "10:5:C:T"}  # pass-2-only marker not added
    assert rows["2:50:A:C"][3:7] == ["0.2200", "0.0100", "0.2100", "0.2300"]
    assert rows["1:100:A:G"][3:7] == ["0.7500", "0.0200", "0.6000", "0.8000"]  # flipped; FreqSE unchanged
    assert rows["10:5:C:T"][3:7] == ["NA", "NA", "NA", "NA"]
    by_marker = {r[0]: r for r in pass1}
    for marker, r in rows.items():  # everything but the four frequency columns comes from pass 1
        assert r[:3] + r[7:] == by_marker[marker][:3] + by_marker[marker][7:], marker

    for n in run_metal.UPLOAD_NAMES:
        assert (tmp_path / "o" / n).exists(), n
    assert (tmp_path / "o" / "metal_freq.txt").read_text() == script2
    assert (tmp_path / "o" / "metal_freq.tbl.info").read_text() == "freq info"
    assert (tmp_path / "o" / "metal.tbl.info").read_text() == "info"
    with gzip.open(tmp_path / "o" / "metal_freq.tbl.gz", "rt") as fh:
        assert len(fh.read().splitlines()) == 1 + len(pass2)  # raw pass-2 output kept as is
    assert s["freq_source"] == "pass2_eaf_cohorts" and s["n_cohorts_with_eaf"] == 2
    assert s["caveats"][-1] == CAVEAT_FREQ_PASS2 and CAVEAT_FREQ_PASS1 not in s["caveats"]
    assert set(s["metal_runs"]["freq"]) == {"metal_elapsed_s", "metal_peak_rss_mb"}
    assert json.loads((tmp_path / "o" / "summary.json").read_text())["freq_source"] == "pass2_eaf_cohorts"
    assert s["n_meta_variants"] == 3  # summarised from the joined table


def test_meta_analyze_frequency_pass_errors_raise(tmp_path, monkeypatch):
    pass1 = [_row("1:100:A:G", "a", "g", "0.35", "0.35", "0", "0.7", direction="++")]
    pass2 = [_row("1:100:A:G", "a", "g", "0.3", "0", "0.3", "0.3", direction="+")]
    monkeypatch.setattr(run_metal, "run_metal_script",
                        _fake_metal([], pass1, pass2, pass2_log="## Failed to open file '../work/A_f1.tsv.gz'\n"))
    with pytest.raises(run_metal.MetalError, match="Failed to open"):
        _analyze(tmp_path, [_cohort("A", "f1"), _cohort("B", "f2", munge_has_eaf=False)],
                 lambda co, p: _write_munged(p, 1))
    assert "Failed to open" in (tmp_path / "o" / "metal_freq.log").read_text()
    assert (tmp_path / "o" / "metal.log").exists() and (tmp_path / "o" / "metal_freq.txt").exists()


def test_meta_analyze_frequency_pass_direction_width_checked(tmp_path, monkeypatch):
    pass1 = [_row("1:100:A:G", "a", "g", "0.35", "0.35", "0", "0.7", direction="+++")]
    pass2 = [_row("1:100:A:G", "a", "g", "0.3", "0", "0.3", "0.3", direction="+")]  # 1 char, 2 EAF inputs
    monkeypatch.setattr(run_metal, "run_metal_script", _fake_metal([], pass1, pass2))
    with pytest.raises(run_metal.MetalError, match="Direction width 1 != 2 inputs"):
        _analyze(tmp_path, [_cohort("A", "f1"), _cohort("B", "f2", munge_has_eaf=None), _cohort("C", "f3")],
                 lambda co, p: _write_munged(p, 1))


def test_meta_analyze_no_eaf_blanks_frequencies(tmp_path, monkeypatch):
    calls = []
    pass1 = [_row("1:100:A:G", "a", "g", "0.0000", "0.0000", "0.0000", "1.0000", direction="++"),
             _row("2:50:A:C", "a", "c", "1.0000", "0.0000", "1.0000", "1.0000", direction="+-")]
    monkeypatch.setattr(run_metal, "run_metal_script", _fake_metal(calls, pass1))
    cohorts = [_cohort("A", "f1", munge_has_eaf=False), _cohort("B", "f2", munge_has_eaf=0)]
    s = _analyze(tmp_path, cohorts, lambda co, p: _write_munged(p, 1))
    assert len(calls) == 1
    rows = _final_rows(tmp_path)
    by_marker = {r[0]: r for r in pass1}
    for marker, r in rows.items():
        assert r[3:7] == ["NA"] * 4 and r[:3] + r[7:] == by_marker[marker][:3] + by_marker[marker][7:]
    assert s["freq_source"] == "none" and s["n_cohorts_with_eaf"] == 0
    assert s["caveats"][-1] == CAVEAT_FREQ_NONE and CAVEAT_FREQ_PASS1 not in s["caveats"]
    assert not (tmp_path / "w" / "PSOR_Combined_All_1.tbl").exists()  # raw table deleted after blanking
    assert not any((tmp_path / "o" / n).exists() for n in FREQ_NAMES)
    top = (tmp_path / "o" / "top_loci.tsv").read_text().splitlines()
    assert len(top) == 1  # no genome-wide hits here; NA frequencies parse fine in summarize


def test_meta_analyze_all_eaf_is_single_pass(tmp_path, monkeypatch):
    calls = []
    pass1 = [_row("1:100:A:G", "a", "g", "0.3000", "0.0100", "0.2900", "0.3100", direction="++")]
    monkeypatch.setattr(run_metal, "run_metal_script", _fake_metal(calls, pass1))
    s = _analyze(tmp_path, [_cohort("A", "f1"), _cohort("B", "f2")], lambda co, p: _write_munged(p, 1))
    assert len(calls) == 1 and s["freq_source"] == "pass1"
    assert _final_rows(tmp_path)["1:100:A:G"] == pass1[0]  # untouched
    assert not any((tmp_path / "o" / n).exists() for n in FREQ_NAMES)


def test_run_metal_script_nonzero_exit_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(run_metal, "METAL_BIN", "false")
    with pytest.raises(run_metal.MetalError, match="exited 1"):
        run_metal.run_metal_script("QUIT\n", str(tmp_path))


def _fake_bin(tmp_path, body):
    p = tmp_path / "fake_metal"
    p.write_text("#!/bin/sh\n" + body)
    p.chmod(0o755)
    return str(p)


def test_run_metal_script_runs_in_work_dir_and_logs(tmp_path, monkeypatch):
    work = tmp_path / "w"
    work.mkdir()
    monkeypatch.setattr(run_metal, "METAL_BIN", _fake_bin(tmp_path, 'pwd\ncat "$1"\necho oops >&2\n'))
    log = run_metal.run_metal_script("PROCESS x\nQUIT\n", str(work))
    assert log == str(work / "metal.log")
    assert (work / "metal.txt").read_text() == "PROCESS x\nQUIT\n"
    text = (work / "metal.log").read_text()
    assert str(work.resolve()) in text and "PROCESS x" in text and "oops" in text


def test_run_metal_script_error_carries_log_tail(tmp_path, monkeypatch):
    body = 'i=1; while [ $i -le 50 ]; do echo "line $i"; i=$((i+1)); done; exit 3\n'
    monkeypatch.setattr(run_metal, "METAL_BIN", _fake_bin(tmp_path, body))
    with pytest.raises(run_metal.MetalError, match="exited 3") as ei:
        run_metal.run_metal_script("QUIT\n", str(tmp_path))
    msg = str(ei.value)
    assert "line 11\n" in msg and "line 50" in msg and "line 10\n" not in msg


def test_run_metal_script_missing_binary_raises_metal_error(tmp_path, monkeypatch):
    monkeypatch.setattr(run_metal, "METAL_BIN", str(tmp_path / "nope"))
    with pytest.raises(run_metal.MetalError):
        run_metal.run_metal_script("QUIT\n", str(tmp_path))


def test_metal_version_parses_banner(tmp_path, monkeypatch):
    monkeypatch.setattr(run_metal, "METAL_BIN",
                        _fake_bin(tmp_path, 'echo "MetaAnalysis Helper"\necho "  METAL 2020-05-05  "\n'))
    assert run_metal.metal_version() == "METAL 2020-05-05"


def test_metal_version_uses_release_date_when_banner_lacks_metal(tmp_path, monkeypatch):
    # The real METAL 2020-05-05 banner, verbatim: no line contains "METAL".
    banner = ('echo "MetaAnalysis Helper - (c) 2007 - 2009 Goncalo Abecasis"\n'
              'echo "This version released on 2020-05-05"\necho\necho "# This program faciliates meta-analysis"\n')
    monkeypatch.setattr(run_metal, "METAL_BIN", _fake_bin(tmp_path, banner))
    assert run_metal.metal_version() == "METAL 2020-05-05"


def test_metal_version_appends_commit_when_recorded(tmp_path, monkeypatch):
    banner = 'echo "This version released on 2020-05-05"\n'
    monkeypatch.setattr(run_metal, "METAL_BIN", _fake_bin(tmp_path, banner))
    sha = tmp_path / "metal_commit.txt"
    sha.write_text("e2253cc3901df8403a331bd725d4d9fe1edfb19f\n")
    monkeypatch.setattr(run_metal, "METAL_COMMIT_FILE", str(sha))
    assert run_metal.metal_version() == "METAL 2020-05-05 (commit e2253cc3901df8403a331bd725d4d9fe1edfb19f)"
    monkeypatch.setattr(run_metal, "METAL_COMMIT_FILE", str(tmp_path / "absent.txt"))
    assert run_metal.metal_version() == "METAL 2020-05-05"


def test_metal_version_fallbacks(tmp_path, monkeypatch):
    monkeypatch.setattr(run_metal, "METAL_COMMIT_FILE", str(tmp_path / "absent.txt"))
    monkeypatch.setattr(run_metal, "METAL_BIN", str(tmp_path / "nope"))
    assert run_metal.metal_version() == "METAL (unknown version)"
    monkeypatch.setattr(run_metal, "METAL_BIN", _fake_bin(tmp_path, 'echo "no banner here"\n'))
    assert run_metal.metal_version() == "METAL (unknown version)"


def _setup_main(monkeypatch, run=None):
    import dataregistry.api.db as db
    from dataregistry.api import sgc_ma_results as ma_db

    updates = []
    monkeypatch.setattr(ma_db, "get_sgc_ma_run", lambda *a, **k: run or {
        "id": "run-xyz", "phenotype": "PH", "ancestry": "Combined", "sex": "Male",
        "dataset_file_ids": ["f1", "f2"]})
    monkeypatch.setattr(ma_db, "update_sgc_ma_result", lambda eng, rid, **k: updates.append((rid, k)))

    class _DummyDB:
        def get_engine(self):
            return "ENG"
    monkeypatch.setattr(db, "DataRegistryReadWriteDB", _DummyDB)
    selected = {}
    cohorts = [_cohort("A", "f1"), _cohort("B", "f2")]

    def fake_select(engine, file_ids):
        selected["file_ids"] = file_ids
        return cohorts

    def fake_ignored(engine, phenotype, ancestry, sex="All"):
        selected["bucket"] = (phenotype, ancestry, sex)
        return []
    monkeypatch.setattr(run_metal.sel, "select_cohorts_by_file_ids", fake_select)
    monkeypatch.setattr(run_metal.sel, "ignored_cohorts", fake_ignored)
    monkeypatch.setattr(run_metal, "metal_version", lambda: "METAL vX")
    s3 = MagicMock()
    monkeypatch.setattr(run_metal.boto3, "client", lambda *a, **k: s3)
    return updates, selected, s3


def _invoke(tmp_path):
    return CliRunner().invoke(run_metal.main, ["--run-id", "run-xyz", "--bucket", "bkt",
                                               "--local-out", str(tmp_path / "out")])


def test_main_updates_status_and_uploads(monkeypatch, tmp_path):
    updates, selected, s3 = _setup_main(monkeypatch)
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-xyz")
    seen = {}

    def fake_meta(cohorts, fetch_fn, work_dir, out_dir, **kw):
        seen.update(kw, work_dir=work_dir, out_dir=out_dir)
        fetch_fn(cohorts[0], os.path.join(work_dir, "in.tsv.gz"))
        os.makedirs(out_dir, exist_ok=True)
        for n in run_metal.UPLOAD_NAMES:
            Path(out_dir, n).write_bytes(b"x")
        return {"meta_lambda_gc": 1.02, "n_meta_variants": 100, "n_genome_wide_sig": 3, "n_cohorts": 2,
                "n_cohorts_used": 2, "total_cases": 15, "total_controls": 27, "per_cohort": []}
    monkeypatch.setattr(run_metal, "meta_analyze", fake_meta)

    res = _invoke(tmp_path)
    assert res.exit_code == 0, res.output
    assert selected == {"file_ids": ["f1", "f2"], "bucket": ("PH", "Combined", "Male")}
    assert seen["phenotype"] == "PH" and seen["ancestry"] == "Combined" and seen["sex"] == "Male"
    assert seen["engine_version"] == "METAL vX" and seen["ignored"] == []
    assert seen["maf_min"] == 0.005 and seen["info_min"] == 0.3  # run row has none: defaults
    assert seen["out_dir"] == str(tmp_path / "out")
    s3.download_file.assert_called_once_with("bkt", "sgc/munge/f1/standardised.tsv.gz",
                                             os.path.join(seen["work_dir"], "in.tsv.gz"))

    assert [u[0] for u in updates] == ["run-xyz", "run-xyz"]
    assert updates[0][1] == {"status": "RUNNING", "batch_job_id": "job-xyz"}
    prefix = "sgc/ma/PH/Combined/run-xyz"
    assert updates[1][1] == {
        "status": "SUCCEEDED", "meta_lambda_gc": 1.02, "n_meta_variants": 100, "n_genome_wide_sig": 3,
        "n_cohorts": 2, "n_cohorts_used": 2, "total_cases": 15, "total_controls": 27,
        "manhattan_s3_key": f"{prefix}/manhattan.png", "qq_s3_key": f"{prefix}/qq.png",
        "meta_s3_key": f"{prefix}/metal.tbl.gz", "summary_json_s3_key": f"{prefix}/summary.json",
        "summary_tsv_s3_key": f"{prefix}/summary.tsv", "top_loci_s3_key": f"{prefix}/top_loci.tsv"}

    calls = s3.upload_file.call_args_list
    assert len(calls) == len(run_metal.UPLOAD_NAMES) == 13
    ct = {c.args[2]: c.kwargs["ExtraArgs"]["ContentType"] for c in calls}
    assert set(ct) == {f"{prefix}/{n}" for n in run_metal.UPLOAD_NAMES}
    assert all(c.args[1] == "bkt" for c in calls)
    assert ct[f"{prefix}/metal.log"] == "text/plain"
    assert ct[f"{prefix}/metal.txt"] == "text/plain"
    assert ct[f"{prefix}/metal.tbl.info"] == "text/plain"
    assert ct[f"{prefix}/metal.tbl.gz"] == "application/gzip"
    assert ct[f"{prefix}/metal_freq.tbl.gz"] == "application/gzip"
    assert ct[f"{prefix}/metal_freq.tbl.info"] == "text/plain"
    assert ct[f"{prefix}/metal_freq.log"] == "text/plain" and ct[f"{prefix}/metal_freq.txt"] == "text/plain"
    assert ct[f"{prefix}/manhattan.png"] == "image/png" and ct[f"{prefix}/qq.png"] == "image/png"
    assert ct[f"{prefix}/summary.json"] == "application/json"
    assert ct[f"{prefix}/summary.tsv"] == "text/tab-separated-values"
    assert ct[f"{prefix}/top_loci.tsv"] == "text/tab-separated-values"


def test_main_skips_absent_artifacts_and_defaults_sex(monkeypatch, tmp_path):
    updates, selected, s3 = _setup_main(monkeypatch, run={
        "id": "run-xyz", "phenotype": "PH", "ancestry": "EUR", "dataset_file_ids": None})
    monkeypatch.delenv("AWS_BATCH_JOB_ID", raising=False)

    def fake_meta(cohorts, fetch_fn, work_dir, out_dir, **kw):
        os.makedirs(out_dir, exist_ok=True)
        for n in ("metal.tbl.gz", "summary.json"):
            Path(out_dir, n).write_bytes(b"x")
        return {"meta_lambda_gc": None, "n_meta_variants": 1, "n_genome_wide_sig": 0, "n_cohorts": 2,
                "n_cohorts_used": 2, "total_cases": None, "total_controls": None}
    monkeypatch.setattr(run_metal, "meta_analyze", fake_meta)
    res = _invoke(tmp_path)
    assert res.exit_code == 0, res.output
    assert selected == {"file_ids": [], "bucket": ("PH", "EUR", "All")}
    assert updates[0][1] == {"status": "RUNNING", "batch_job_id": None}
    assert {c.args[2] for c in s3.upload_file.call_args_list} == {
        "sgc/ma/PH/EUR/run-xyz/metal.tbl.gz", "sgc/ma/PH/EUR/run-xyz/summary.json"}


def test_main_marks_failed_on_error(monkeypatch, tmp_path):
    updates, _, s3 = _setup_main(monkeypatch)

    def boom(*a, **k):
        raise run_metal.MetalError("fewer than 2 munged inputs" + "x" * 5000)
    monkeypatch.setattr(run_metal, "meta_analyze", boom)
    res = _invoke(tmp_path)
    assert res.exit_code != 0
    assert isinstance(res.exception, run_metal.MetalError)
    assert [u[1]["status"] for u in updates] == ["RUNNING", "FAILED"]
    err = updates[-1][1]["error_message"]
    assert err.startswith("fewer than 2 munged inputs") and len(err) == 4000 and err.endswith("x")
    s3.upload_file.assert_not_called()


def test_main_missing_run_exits(monkeypatch, tmp_path):
    from dataregistry.api import sgc_ma_results as ma_db
    updates, _, _ = _setup_main(monkeypatch)
    monkeypatch.setattr(ma_db, "get_sgc_ma_run", lambda *a, **k: None)
    res = _invoke(tmp_path)
    assert res.exit_code != 0 and "no MA run run-xyz" in res.output
    assert updates == []


def test_worker_import_is_light():
    code = ("import sys, sgc_metal.run_metal; "
            "bad = [m for m in ('fastapi', 'dataregistry.api.query', 'pegasus', 'sgc_ma.run_ma') "
            "if m in sys.modules]; "
            "print(bad)")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True,
                         cwd=str(Path(__file__).resolve().parents[2]))
    assert out.stdout.strip() == "[]"


def test_main_cli_options_match_run_ma():
    from sgc_ma import run_ma
    opts = lambda cmd: {(p.name, p.required, p.default) for p in cmd.params}  # noqa: E731
    assert {(n, r) for n, r, _ in opts(run_metal.main)} == {(n, r) for n, r, _ in opts(run_ma.main)}
    assert dict((n, d) for n, _, d in opts(run_metal.main))["local_out"] == "metal_out"


# ---- run thresholds vs munged inputs ------------------------------------------------------

def test_meta_analyze_threshold_mismatch_fails_before_download(tmp_path, monkeypatch):
    monkeypatch.setattr(run_metal, "run_metal_script", _fake_run([]))
    fetched = []
    cohorts = [_cohort("A", "f1"), _cohort("B", "f2", munge_maf_min=0.01)]
    with pytest.raises(run_metal.MetalError) as ei:
        _analyze(tmp_path, cohorts, lambda co, p: (fetched.append(co["dataset"]), _write_munged(p, 1)),
                 maf_min=0.005, info_min=0.3)
    assert str(ei.value) == ("run thresholds maf_min=0.005 info_min=0.3 differ from munged inputs "
                             "(B: maf_min=0.01 info_min=0.3); re-munge with --force or launch with matching "
                             "thresholds")
    assert fetched == []


def test_meta_analyze_run_threshold_differs_from_every_munge_row(tmp_path, monkeypatch):
    monkeypatch.setattr(run_metal, "run_metal_script", _fake_run([]))
    with pytest.raises(run_metal.MetalError, match=r"maf_min=0.005 info_min=0.8 differ .*\(A: maf_min=0.005 "
                                                   r"info_min=0.3 and 1 more\); re-munge"):
        _analyze(tmp_path, [_cohort("A", "f1"), _cohort("B", "f2")], lambda co, p: _write_munged(p, 1),
                 maf_min=0.005, info_min=0.8)


def test_meta_analyze_unknown_munge_thresholds_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(run_metal, "run_metal_script", _fake_run([]))
    with pytest.raises(run_metal.MetalError, match=r"\(A: maf_min=None info_min=0.3\)"):
        _analyze(tmp_path, [_cohort("A", "f1", munge_maf_min=None), _cohort("B", "f2")],
                 lambda co, p: _write_munged(p, 1))


def test_meta_analyze_matching_custom_thresholds_drive_caveat(tmp_path, monkeypatch):
    monkeypatch.setattr(run_metal, "run_metal_script", _fake_run([]))
    cohorts = [_cohort("A", "f1", munge_maf_min=0.01, munge_info_min=0.8),
               _cohort("B", "f2", munge_maf_min=0.01, munge_info_min=0.8),
               _cohort("C", "f3", munge_s3_key=None, munge_maf_min=None, munge_info_min=None)]  # not munged: skipped
    s = _analyze(tmp_path, cohorts, lambda co, p: _write_munged(p, 1), maf_min=0.01, info_min=0.8)
    assert any("MAF>=0.01 and INFO>=0.8" in c for c in s["caveats"])
    assert not any("0.005" in c for c in s["caveats"])


# ---- metal.log: per-file processed counts and allele mismatches ----------------------------

CANNED_LOG = """# Processing commands in metal.txt ...
## Set frequency header to Effect_AF ...
###########################################################################
## Processing file 'work/A_f1.tsv.gz'
## Processed 4 markers ...

###########################################################################
## Processing file 'work/B_f2.tsv.gz'
## WARNING: Bad alleles for marker '1:300:AT:A', expecting 'a/at' found 'ta/t'
## WARNING: Bad alleles for marker '1:900:CT:C', expecting 'c/ct' found 'g/tg'
## WARNING: Allele names don't match previous occurences at 5 additional markers
## Processed 3 markers ...

###########################################################################
## Processing file 'work/C_f3.tsv.gz'
## Processed 2 markers ...

## Set output file prefix and suffix to PSOR_Combined_All_ and .tbl ...
###########################################################################
## Running second pass analysis to evaluate heterogeneity...
## Processing file 'work/C_f3.tsv.gz'
## WARNING: Bad alleles for marker '1:1:A:G', expecting 'a/g' found 'a/c'
## Processing file 'work/B_f2.tsv.gz'
## Processing file 'work/A_f1.tsv.gz'

###########################################################################
## Executing meta-analysis ...
## Completed meta-analysis for 5 markers!
"""


def test_parse_process_log(tmp_path):
    p = tmp_path / "metal.log"
    p.write_text(CANNED_LOG)
    assert run_metal.parse_process_log(str(p)) == {
        "work/A_f1.tsv.gz": {"n_markers_processed": 4, "n_allele_mismatch": 0},
        "work/B_f2.tsv.gz": {"n_markers_processed": 3, "n_allele_mismatch": 7},
        "work/C_f3.tsv.gz": {"n_markers_processed": 2, "n_allele_mismatch": 0},  # heterogeneity re-read ignored
    }


def test_parse_process_log_without_counts(tmp_path):
    p = tmp_path / "metal.log"
    p.write_text("ok\n")
    assert run_metal.parse_process_log(str(p)) == {}


def test_meta_analyze_reports_metal_counts_and_mismatch_caveat(tmp_path, monkeypatch):
    monkeypatch.setattr(run_metal, "run_metal_script", _metal_run_writing([r[:10] + ["+++"] + r[11:] for r in ROWS],
                                                                          CANNED_LOG))
    s = _analyze(tmp_path, [_cohort("A", "f1"), _cohort("B", "f2"), _cohort("C", "f3")],
                 lambda co, p: _write_munged(p, 1))
    used = {c["dataset"]: c for c in s["per_cohort"] if not c.get("skipped")}
    assert {k: (v["n_markers_processed"], v["n_allele_mismatch"]) for k, v in used.items()} == {
        "A": (4, 0), "B": (3, 7), "C": (2, 0)}
    assert s["caveats"][-1].startswith("7 cohort rows were dropped by METAL because their allele strings")
    tsv = (tmp_path / "o" / "summary.tsv").read_text().splitlines()[0].split("\t")
    assert "n_markers_processed" in tsv and "n_allele_mismatch" in tsv


def test_meta_analyze_no_mismatch_no_caveat(tmp_path, monkeypatch):
    log = CANNED_LOG.replace("## WARNING: Bad alleles for marker", "## NOTE").replace(
        "## WARNING: Allele names don't match previous occurences at 5 additional markers\n", "")
    monkeypatch.setattr(run_metal, "run_metal_script", _metal_run_writing([r[:10] + ["+++"] + r[11:] for r in ROWS],
                                                                          log))
    s = _analyze(tmp_path, [_cohort("A", "f1"), _cohort("B", "f2"), _cohort("C", "f3")],
                 lambda co, p: _write_munged(p, 1))
    assert not any("allele strings" in c for c in s["caveats"])
    assert [c["n_allele_mismatch"] for c in s["per_cohort"]] == [0, 0, 0]


# ---- helpers -------------------------------------------------------------------------------

def test_sorted_copy_failure_is_metal_error_with_stderr(tmp_path):
    with pytest.raises(run_metal.MetalError, match=r"sort .*No such file"):
        run_metal._sorted_copy(str(tmp_path / "absent.tbl"), str(tmp_path))


# ---- main: thresholds, failure logs, SIGTERM -----------------------------------------------

def test_main_passes_run_thresholds(monkeypatch, tmp_path):
    _setup_main(monkeypatch, run={"id": "run-xyz", "phenotype": "PH", "ancestry": "EUR",
                                  "dataset_file_ids": ["f1"], "maf_min": 0.01, "info_min": 0.8})
    seen = {}

    def fake_meta(cohorts, fetch_fn, work_dir, out_dir, **kw):
        seen.update(kw)
        return {"meta_lambda_gc": None, "n_meta_variants": 1, "n_genome_wide_sig": 0, "n_cohorts": 2,
                "n_cohorts_used": 2, "total_cases": None, "total_controls": None}
    monkeypatch.setattr(run_metal, "meta_analyze", fake_meta)
    assert _invoke(tmp_path).exit_code == 0
    assert seen["maf_min"] == 0.01 and seen["info_min"] == 0.8


def test_main_uploads_logs_on_failure(monkeypatch, tmp_path):
    updates, _, s3 = _setup_main(monkeypatch)

    def fail_with_logs(cohorts, fetch_fn, work_dir, out_dir, **kw):
        os.makedirs(out_dir, exist_ok=True)
        for n in ("metal.log", "metal.txt", "metal_freq.log"):
            Path(out_dir, n).write_text(n)
        raise run_metal.MetalError("metal dropped input(s)")
    monkeypatch.setattr(run_metal, "meta_analyze", fail_with_logs)
    res = _invoke(tmp_path)
    assert res.exit_code != 0 and isinstance(res.exception, run_metal.MetalError)
    assert [u[1]["status"] for u in updates] == ["RUNNING", "FAILED"]
    uploaded = {c.args[2]: c.kwargs["ExtraArgs"]["ContentType"] for c in s3.upload_file.call_args_list}
    prefix = "sgc/ma/PH/Combined/run-xyz"
    assert uploaded == {f"{prefix}/metal.log": "text/plain", f"{prefix}/metal.txt": "text/plain",
                        f"{prefix}/metal_freq.log": "text/plain"}


def test_main_failure_log_upload_error_keeps_original_error(monkeypatch, tmp_path):
    updates, _, s3 = _setup_main(monkeypatch)
    s3.upload_file.side_effect = RuntimeError("s3 down")

    def fail_with_logs(cohorts, fetch_fn, work_dir, out_dir, **kw):
        os.makedirs(out_dir, exist_ok=True)
        Path(out_dir, "metal.log").write_text("x")
        raise run_metal.MetalError("boom")
    monkeypatch.setattr(run_metal, "meta_analyze", fail_with_logs)
    res = _invoke(tmp_path)
    assert isinstance(res.exception, run_metal.MetalError) and str(res.exception) == "boom"
    assert updates[-1][1] == {"status": "FAILED", "error_message": "boom"}


def test_main_does_not_upload_stale_logs(monkeypatch, tmp_path):
    updates, _, s3 = _setup_main(monkeypatch)
    out = tmp_path / "out"
    out.mkdir()
    (out / "metal.log").write_text("from an earlier local run")
    monkeypatch.setattr(run_metal.sel, "select_cohorts_by_file_ids",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db down")))
    res = _invoke(tmp_path)
    assert res.exit_code != 0 and updates[-1][1]["status"] == "FAILED"
    s3.upload_file.assert_not_called()


def test_main_sigterm_marks_failed_and_restores_handler(monkeypatch, tmp_path):
    updates, _, s3 = _setup_main(monkeypatch)
    before = signal.getsignal(signal.SIGTERM)

    def killed(cohorts, fetch_fn, work_dir, out_dir, **kw):
        os.makedirs(out_dir, exist_ok=True)
        Path(out_dir, "metal.log").write_text("partial")
        os.kill(os.getpid(), signal.SIGTERM)  # Batch timeout / Spot reclaim
        raise AssertionError("SIGTERM handler did not raise")
    monkeypatch.setattr(run_metal, "meta_analyze", killed)
    res = _invoke(tmp_path)
    assert isinstance(res.exception, RuntimeError)
    assert updates[-1][1] == {"status": "FAILED",
                              "error_message": "terminated by SIGTERM (Batch timeout or Spot reclaim)"}
    assert [c.args[2].rsplit("/", 1)[1] for c in s3.upload_file.call_args_list] == ["metal.log"]
    assert signal.getsignal(signal.SIGTERM) is before
