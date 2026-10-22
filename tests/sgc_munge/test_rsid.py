import pytest
from sgc_munge import rsid
from tests.sgc_munge.fixtures import build_reference, requires_bcftools


def test_bcftools_path_default_and_env(monkeypatch):
    monkeypatch.delenv("SGC_BCFTOOLS", raising=False)
    assert rsid.bcftools_path() == "bcftools"
    monkeypatch.setenv("SGC_BCFTOOLS", "/opt/bin/bcftools")
    assert rsid.bcftools_path() == "/opt/bin/bcftools"


def test_check_bcftools_missing_raises(monkeypatch, tmp_path):
    monkeypatch.setenv("SGC_BCFTOOLS", str(tmp_path / "nope"))
    with pytest.raises(RuntimeError, match="bcftools not found at .*nope.*set SGC_BCFTOOLS"):
        rsid.check_bcftools()


@requires_bcftools
def test_check_bcftools_reports_version(monkeypatch):
    monkeypatch.delenv("SGC_BCFTOOLS", raising=False)
    assert rsid.check_bcftools().startswith("bcftools 1.")


@requires_bcftools
def test_sort_and_query_dbsnp_write_per_contig_hits(tmp_path):
    _, db = build_reference(tmp_path)
    targets = tmp_path / "targets.tsv"
    targets.write_text("X\t50\n1\t300\n1\t120\n1\t120\n1\t999\n1\t161\n")
    sorted_ = tmp_path / "targets.sorted.tsv"
    assert rsid.sort_targets(str(targets), str(sorted_), str(tmp_path)) == 5
    assert sorted_.read_text() == "1\t120\n1\t161\n1\t300\n1\t999\nX\t50\n"
    hits_dir = tmp_path / "hits"
    hits_dir.mkdir()
    assert rsid.query_dbsnp(db, str(sorted_), str(hits_dir)) == {"1": 4, "X": 1}
    assert rsid.load_hits(str(hits_dir / "1.tsv")) == {
        120: [("A", ("G",), "rs100"), ("A", ("C",), "rs101")],
        161: [("CA", ("C",), "rs106")],
        300: [("A", ("G", "T"), "rs105")]}
    assert rsid.load_hits(str(hits_dir / "X.tsv")) == {50: [("G", ("T",), "rs104")]}
    assert rsid.load_hits(str(hits_dir / "Y.tsv")) == {}          # never written
    assert (hits_dir / "bcftools.stderr").exists()


def test_sort_targets_empty(tmp_path):
    (tmp_path / "t.tsv").write_text("")
    assert rsid.sort_targets(str(tmp_path / "t.tsv"), str(tmp_path / "s.tsv"), str(tmp_path)) == 0
    assert (tmp_path / "s.tsv").read_text() == ""


def test_query_dbsnp_empty_targets_skips_bcftools(tmp_path, monkeypatch):
    monkeypatch.setenv("SGC_BCFTOOLS", str(tmp_path / "nope"))   # would fail if invoked
    (tmp_path / "t.tsv").write_text("")
    assert rsid.query_dbsnp("unused.vcf.gz", str(tmp_path / "t.tsv"), str(tmp_path)) == {}


@requires_bcftools
def test_query_dbsnp_failure_raises_with_stderr(tmp_path):
    (tmp_path / "t.tsv").write_text("1\t120\n")
    with pytest.raises(RuntimeError, match=r"bcftools query failed \(exit [1-9]\d*\)") as ei:
        rsid.query_dbsnp(str(tmp_path / "missing.vcf.gz"), str(tmp_path / "t.tsv"), str(tmp_path))
    assert "missing.vcf.gz" in str(ei.value)          # the stderr tail names the file


def test_load_hits_parses_missing_id_and_multi_alt(tmp_path):
    p = tmp_path / "1.tsv"
    p.write_text("1\t10\t.\tA\tG\n1\t10\trs2\tA\tC,T\n1\t11\trs3\tTA\tT\n")
    assert rsid.load_hits(str(p)) == {10: [("A", ("G",), None), ("A", ("C", "T"), "rs2")],
                                      11: [("TA", ("T",), "rs3")]}
