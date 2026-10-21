import gzip
import math

import pytest

from sgc_metal.tbl import (TBL_COLUMNS, TOP_LOCI_COLUMNS, TblRow, direction_stats, iter_tbl, parse_marker,
                           parse_pvalue, row_to_locus)

GOOD = ["1:100:A:G", "g", "a", "0.3", "0.01", "0.25", "0.35", "0.12", "0.02", "1e-6", "++?", "45.0", "1.8",
        "1", "0.18", "300", "900"]


def _write(p, rows, header=None):
    p.write_text("\t".join(header or TBL_COLUMNS) + "\n" + "".join("\t".join(r) + "\n" for r in rows))
    return str(p)


def test_parse_marker_snv():
    assert parse_marker("1:12345:A:G") == ("1", 12345, "A", "G")


def test_parse_marker_indel_and_x():
    assert parse_marker("X:50:G:GT") == ("X", 50, "G", "GT")


def test_direction_stats():
    assert direction_stats("++-?") == (3, 2 / 3)
    assert direction_stats("?+") == (1, 1.0)
    assert direction_stats("??") == (0, None)


def test_direction_stats_zero_counts_as_present():
    assert direction_stats("+0-") == (3, 1 / 3)


def test_iter_tbl_parses_and_uppercases(tmp_path):
    rows = list(iter_tbl(_write(tmp_path / "x.tbl", [GOOD])))
    r = rows[0]
    assert r.allele1 == "G" and r.allele2 == "A" and r.chromosome == "1" and r.position == 100
    assert r.n_cohorts == 2 and r.dir_concordance == 1.0 and r.i2 == 45.0 and r.ncases == 300


def test_iter_tbl_gz(tmp_path):
    p = tmp_path / "x.tbl.gz"
    with gzip.open(p, "wt") as fh:
        fh.write("\t".join(TBL_COLUMNS) + "\n" + "\t".join(GOOD) + "\n")
    assert len(list(iter_tbl(str(p)))) == 1


def test_iter_tbl_na_optional_fields(tmp_path):
    g = list(GOOD)
    g[3], g[11], g[15] = "NA", "NA", "nan"
    r = list(iter_tbl(_write(tmp_path / "x.tbl", [g])))[0]
    assert r.freq1 is None and r.i2 is None and r.ncases is None and r.effect == 0.12


def test_iter_tbl_na_required_raises(tmp_path):
    g = list(GOOD)
    g[7] = "NA"
    with pytest.raises(ValueError):
        list(iter_tbl(_write(tmp_path / "x.tbl", [g])))


def test_iter_tbl_rejects_bad_header(tmp_path):
    with pytest.raises(ValueError):
        list(iter_tbl(_write(tmp_path / "x.tbl", [GOOD], header=TBL_COLUMNS[:-1] + ["Foo"])))


def _r(marker, a1, a2, effect=0.12, freq1=0.3, pvalue_str="1e-06"):
    chrom, pos, ref, alt = parse_marker(marker)
    return TblRow(chrom, pos, ref, alt, a1, a2, freq1, effect, 0.02, float(pvalue_str), "++?", 2, 1.0, 45.0,
                  300.0, 900.0, pvalue_str=pvalue_str, log10p=math.log10(float(pvalue_str)))


def test_row_to_locus_keys():
    r = _r("1:100:A:G", "G", "A")
    d = row_to_locus(r)
    assert list(d) == TOP_LOCI_COLUMNS and d["beta"] == r.effect and d["se"] == r.se
    assert TOP_LOCI_COLUMNS[-3:] == ["orientation", "pvalue_str", "log10p"]
    assert d["pvalue_str"] == "1e-06" and d["log10p"] == pytest.approx(-6)


def test_row_to_locus_allele1_is_alt_keeps_orientation():
    d = row_to_locus(_r("1:100:A:G", "G", "A", effect=0.12, freq1=0.3))
    assert d["beta"] == 0.12 and d["freq1"] == 0.3 and d["orientation"] == "alt"
    assert (d["allele1"], d["allele2"]) == ("G", "A")  # METAL's alleles as printed


def test_row_to_locus_allele1_is_ref_flips_to_alt():
    # METAL sorts a marker's alleles, so for an A/G SNP Allele1 is 'a' = REF
    d = row_to_locus(_r("1:100:A:G", "A", "G", effect=0.12, freq1=0.3))
    assert d["beta"] == -0.12 and d["freq1"] == pytest.approx(0.7) and d["orientation"] == "alt"


def test_row_to_locus_flip_keeps_missing_freq():
    d = row_to_locus(_r("1:100:A:G", "A", "G", effect=0.12, freq1=None))
    assert d["beta"] == -0.12 and d["freq1"] is None


def test_row_to_locus_indel_other_form_oriented_by_length():
    # marker AT>A (normalised), first cohort reported the deletion as TA/T: the longer
    # reported allele is the longer of (ref, alt) = REF, the shorter one is ALT
    d = row_to_locus(_r("1:300:AT:A", "TA", "T", effect=0.12, freq1=0.3))
    assert d["beta"] == -0.12 and d["freq1"] == pytest.approx(0.7) and d["orientation"] == "alt"
    d = row_to_locus(_r("1:300:AT:A", "T", "TA", effect=0.12, freq1=0.3))
    assert d["beta"] == 0.12 and d["freq1"] == 0.3 and d["orientation"] == "alt"
    d = row_to_locus(_r("2:50:C:CTT", "CTT", "C", effect=0.12, freq1=0.3))  # insertion, plain match
    assert d["beta"] == 0.12 and d["orientation"] == "alt"
    d = row_to_locus(_r("2:50:C:CTT", "G", "GTT", effect=0.12, freq1=0.3))  # insertion, other form
    assert d["beta"] == -0.12 and d["freq1"] == pytest.approx(0.7) and d["orientation"] == "alt"


def test_row_to_locus_unresolved_orientation_left_as_is():
    # equal lengths and no allele match (e.g. an MNP reported differently): cannot tell
    d = row_to_locus(_r("1:300:AT:GC", "TA", "CG", effect=0.12, freq1=0.3))
    assert d["beta"] == 0.12 and d["freq1"] == 0.3 and d["orientation"] == "unresolved"
    d = row_to_locus(_r("1:300:AT:A", "TA", "TC", effect=0.12, freq1=0.3))  # reported alleles same length
    assert d["orientation"] == "unresolved"


@pytest.mark.parametrize("s, pvalue, log10p", [
    ("0.000532", 0.000532, math.log10(0.000532)),
    ("1e-300", 1e-300, -300.0),
    ("6.41e-4346", math.nextafter(0.0, 1.0), math.log10(6.41) - 4346),   # below double range
    ("1.00e-400", math.nextafter(0.0, 1.0), -400.0),
    ("0", math.nextafter(0.0, 1.0), -math.inf),
    ("1", 1.0, 0.0),
])
def test_parse_pvalue(s, pvalue, log10p):
    p, lp = parse_pvalue(s)
    assert p == pytest.approx(pvalue, rel=1e-12) and p > 0
    assert lp == log10p if math.isinf(log10p) else lp == pytest.approx(log10p, abs=1e-9)


def test_iter_tbl_keeps_metal_pvalue_string_and_log10p(tmp_path):
    g = list(GOOD)
    g[9] = "6.41e-4346"
    r = list(iter_tbl(_write(tmp_path / "x.tbl", [g])))[0]
    assert r.pvalue_str == "6.41e-4346" and r.pvalue == math.nextafter(0.0, 1.0)
    assert r.log10p == pytest.approx(-4345.19314, abs=1e-5)


@pytest.mark.parametrize("bad", ["NA", "", "nan"])
def test_iter_tbl_missing_pvalue_raises(tmp_path, bad):
    g = list(GOOD)
    g[9] = bad
    with pytest.raises(ValueError):
        list(iter_tbl(_write(tmp_path / "x.tbl", [g])))
