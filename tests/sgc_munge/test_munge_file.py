import json
import os
import random

import pandas as pd
import pytest

from sgc_munge import munge, staging
from sgc_munge.munge import ENGINE, MISMATCH_COLUMNS, OUTPUT_COLUMNS, munge_file
from sgc_munge.reference import Reference, normalize_indel
from sgc_munge.rows import ColumnIndex
from tests.sgc_munge.fixtures import (BASE_AT, HDR, MAP, SCENARIO, SCENARIO_ROWS, _gwas_row,
                                      build_reference, requires_bcftools, write_dbsnp,
                                      write_fasta, write_gwas)

pytestmark = requires_bcftools

_OPTIONAL = {"A1FREQ": "col_effect_allele_freq", "INFO": "col_imputation_quality"}


def _run(tmp_path, gwas_path, mapping=MAP, cases=100, controls=200, fasta=None, **kw):
    fa, db = build_reference(tmp_path) if fasta is None else (fasta, None)
    with Reference(fa, db) as ref:
        res = munge_file(gwas_path, mapping, ref, str(tmp_path / "out"),
                         cases=cases, controls=controls, **kw)
    out = pd.read_csv(tmp_path / "out/standardised.tsv.gz", sep="\t", dtype=str,
                      keep_default_na=False)
    return res, out


def test_munge_file_scenario(tmp_path):
    gw = write_gwas(tmp_path, SCENARIO_ROWS)
    res, out = _run(tmp_path, gw, ref_version="test-v1", file_id="f1")
    assert list(out.columns) == OUTPUT_COLUMNS
    assert res["n_in"] == 14 and res["n_out"] == 7
    assert res["n_dropped"] == {"chrom": 1, "invalid": 1, "maf": 1, "info": 1,
                                "ref_mismatch": 1, "dup": 2}
    assert res["n_in"] == res["n_out"] + sum(res["n_dropped"].values())
    assert res["n_with_rsid"] == 4 and res["n_swapped"] == 1
    # SNVs that parse: 7 report oa == REF, 2 (snv_swapped, dup_flipped) ea == REF, 1 mismatch
    assert res["orientation_prescan"] == {"n_snv": 10, "n_oa_is_ref": 7, "n_ea_is_ref": 2,
                                          "prefer": "oa", "purity": 0.2}
    assert res["n_indel_ambiguous"] == 0 and res["n_indel_norm_exhausted"] == 0
    assert res["n_indel_tie_dbsnp_resolved"] == 0 and res["n_indel_tie_unresolved"] == 0
    assert res["rsid_lookup"] == {"n_targets": 7, "n_hit_positions": 5,
                                  "bcftools": res["rsid_lookup"]["bcftools"]}
    assert res["rsid_lookup"]["bcftools"].startswith("bcftools 1.")
    assert set(res["timings"]) == {"pass1_s", "sort_s", "bcftools_s", "pass2_s"}
    assert all(t >= 0 for t in res["timings"].values())
    by = out.set_index("SNP_ID")
    assert sorted(by.index) == sorted(["1:120:A:G", "1:150:T:A", "1:200:A:T", "1:160:C:CA",
                                       "1:101:G:GA", "1:300:A:C", "X:50:G:T"])
    assert by.loc["1:120:A:G", ["Chromosome", "Position", "Effect_allele", "Non-effect_allele",
                                "Beta", "SE", "P-value", "Effect_AF", "Imp_Quality",
                                "RSID"]].tolist() == \
        ["1", "120", "G", "A", "0.1", "0.05", "0.01", "0.3", "0.9", "rs100"]
    assert by.loc["1:150:T:A", ["Effect_allele", "Non-effect_allele", "Beta", "Effect_AF",
                                "RSID"]].tolist() == ["T", "A", "0.4", "0.7", "rs102"]
    assert by.loc["1:101:G:GA", ["Position", "Effect_allele", "Non-effect_allele",
                                 "RSID"]].tolist() == ["105", "AA", "A", "NA"]
    assert by.loc["1:160:C:CA", "RSID"] == "rs103" and by.loc["X:50:G:T", "RSID"] == "rs104"
    assert by.loc["1:200:A:T", "RSID"] == "NA" and by.loc["1:300:A:C", "RSID"] == "NA"
    assert (out["N_CAS"] == "100").all() and (out["N_CON"] == "200").all()
    mm = pd.read_csv(tmp_path / "out/ref_mismatch.tsv.gz", sep="\t", dtype=str)
    assert list(mm.columns) == MISMATCH_COLUMNS and len(mm) == 1
    assert mm.iloc[0][["Chromosome", "Position", "Effect_allele", "Non-effect_allele",
                       "Ref_seq"]].tolist() == ["1", "250", "G", "C", "A"]
    j = json.loads((tmp_path / "out/munge.json").read_text())
    assert j == res
    assert j["engine"] == ENGINE and j["ref_version"] == "test-v1" and j["file_id"] == "f1"
    assert j["has_eaf"] and j["has_info"] and j["elapsed_s"] >= 0 and j["peak_rss_mb"] > 0
    assert j["cases"] == 100 and j["controls"] == 200 and j["column_mapping"] == MAP
    assert j["maf_min"] == 0.005 and j["info_min"] == 0.3


def test_munge_file_without_optional_columns(tmp_path):
    keep = [i for i, c in enumerate(HDR) if c not in _OPTIONAL]
    hdr = [HDR[i] for i in keep]
    rows = [[r[i] for i in keep] for r in SCENARIO_ROWS]
    mapping = {k: v for k, v in MAP.items() if k not in _OPTIONAL.values()}
    res, out = _run(tmp_path, write_gwas(tmp_path, rows, header=hdr), mapping=mapping)
    assert (out["Effect_AF"] == "NA").all() and (out["Imp_Quality"] == "NA").all()
    assert res["has_eaf"] is False and res["has_info"] is False and res["n_dropped"]["maf"] == 0
    # without the gates, maf_fail and info_fail become further duplicates of snv_fwd
    assert res["n_dropped"]["info"] == 0 and res["n_dropped"]["dup"] == 4 and res["n_out"] == 7


def test_munge_file_all_na_optional_values_mean_has_eaf_false(tmp_path):
    rows = [list(r) for r in (SCENARIO["snv_fwd"], SCENARIO["chrX"])]
    for r in rows:
        r[HDR.index("A1FREQ")] = "NA"
    res, out = _run(tmp_path, write_gwas(tmp_path, rows))
    assert res["has_eaf"] is False and res["has_info"] is True
    assert (out["Effect_AF"] == "NA").all() and (out["Imp_Quality"] == "0.9").all()


def test_munge_file_null_counts_write_NA(tmp_path):
    res, out = _run(tmp_path, write_gwas(tmp_path, SCENARIO_ROWS), cases=None, controls=None)
    assert (out["N_CAS"] == "NA").all() and (out["N_CON"] == "NA").all()
    assert res["cases"] is None and res["controls"] is None and res["n_out"] == 7


def test_munge_file_no_mismatch_sidecar_when_clean(tmp_path):
    res, out = _run(tmp_path, write_gwas(tmp_path, [SCENARIO["snv_fwd"]]))
    assert res["n_out"] == 1 and out["SNP_ID"].tolist() == ["1:120:A:G"]
    assert res["n_dropped"]["ref_mismatch"] == 0
    assert not (tmp_path / "out/ref_mismatch.tsv.gz").exists()
    assert (tmp_path / "out/munge.json").exists()


def test_munge_file_plain_text_input(tmp_path):
    gw = write_gwas(tmp_path, SCENARIO_ROWS, gzip=False)
    assert gw.endswith(".tsv")
    res, out = _run(tmp_path, gw)
    assert res["n_in"] == 14 and res["n_out"] == 7 and len(out) == 7


def test_munge_file_writes_position_as_integer(tmp_path):
    row = list(SCENARIO["snv_fwd"])
    row[HDR.index("GENPOS")] = "1.2e+02"
    mismatch = list(SCENARIO["mismatch"])
    mismatch[HDR.index("GENPOS")] = "250.0"
    _, out = _run(tmp_path, write_gwas(tmp_path, [row, mismatch]))
    assert out.iloc[0][["SNP_ID", "Position"]].tolist() == ["1:120:A:G", "120"]
    mm = pd.read_csv(tmp_path / "out/ref_mismatch.tsv.gz", sep="\t", dtype=str)
    assert mm.iloc[0]["Position"] == "250"


def test_munge_file_writes_numeric_fields_as_reported(tmp_path):
    row = list(SCENARIO["snv_fwd"])
    for col, raw in {"BETA": "1.50E-01", "SE": "5e-2", "P": "1.0e-08", "A1FREQ": "0.30",
                     "INFO": "0.900"}.items():
        row[HDR.index(col)] = raw
    _, out = _run(tmp_path, write_gwas(tmp_path, [row]))
    assert out.iloc[0][["Beta", "SE", "P-value", "Effect_AF", "Imp_Quality"]].tolist() == \
        ["1.50E-01", "5e-2", "1.0e-08", "0.30", "0.900"]


def test_munge_file_output_grouped_by_contig_in_canonical_order(tmp_path):
    rows = [SCENARIO["chrX"], SCENARIO["snv_swapped"], SCENARIO["snv_fwd"]]
    _, out = _run(tmp_path, write_gwas(tmp_path, rows))
    assert out["SNP_ID"].tolist() == ["1:150:T:A", "1:120:A:G", "X:50:G:T"]


def test_munge_file_shuffled_input_same_rows_and_dup_count(tmp_path):
    rows = list(SCENARIO_ROWS)
    random.Random(3).shuffle(rows)
    res, out = _run(tmp_path, write_gwas(tmp_path, rows))
    assert res["n_out"] == 7 and res["n_dropped"]["dup"] == 2
    assert sorted(out["SNP_ID"]) == sorted(["1:120:A:G", "1:150:T:A", "1:200:A:T", "1:160:C:CA",
                                            "1:101:G:GA", "1:300:A:C", "X:50:G:T"])
    assert out["Chromosome"].tolist() == sorted(out["Chromosome"], key=staging.CANONICAL_CONTIGS.index)


def test_munge_file_empty_input_skips_bcftools(tmp_path, monkeypatch):
    monkeypatch.setattr(munge.rsid, "query_dbsnp",
                        lambda *a, **k: pytest.fail("bcftools must not run without targets"))
    res, out = _run(tmp_path, write_gwas(tmp_path, []))
    assert res["n_in"] == 0 and res["n_out"] == 0 and len(out) == 0
    assert list(out.columns) == OUTPUT_COLUMNS
    assert res["rsid_lookup"]["n_targets"] == 0 and res["rsid_lookup"]["n_hit_positions"] == 0


def test_munge_file_missing_bcftools_fails_before_pass1(tmp_path, monkeypatch):
    monkeypatch.setenv("SGC_BCFTOOLS", str(tmp_path / "nope"))
    with pytest.raises(RuntimeError, match="bcftools not found"):
        _run(tmp_path, write_gwas(tmp_path, SCENARIO_ROWS))
    assert not (tmp_path / "out/standardised.tsv.gz").exists()
    assert not (tmp_path / "out/.work").exists()


def test_munge_file_contig_absent_from_dbsnp_gets_NA_rsid(tmp_path):
    fa, _ = build_reference(tmp_path)
    db = write_dbsnp(tmp_path, [("1", 120, "rs100", "A", "G")], contigs=["1"], name="only1.vcf")
    with Reference(fa, db) as ref:
        res = munge_file(write_gwas(tmp_path, [SCENARIO["snv_fwd"], SCENARIO["chrX"]]), MAP, ref,
                         str(tmp_path / "out"), cases=None, controls=None)
    out = pd.read_csv(tmp_path / "out/standardised.tsv.gz", sep="\t", dtype=str, keep_default_na=False)
    assert out.set_index("SNP_ID")["RSID"].to_dict() == {"1:120:A:G": "rs100", "X:50:G:T": "NA"}
    assert res["rsid_lookup"]["n_targets"] == 1 and res["n_with_rsid"] == 1


def test_munge_file_cleans_up_work_dir(tmp_path):
    _run(tmp_path, write_gwas(tmp_path, SCENARIO_ROWS))
    assert sorted(os.listdir(tmp_path / "out")) == ["munge.json", "ref_mismatch.tsv.gz",
                                                     "standardised.tsv.gz"]


def test_munge_file_removes_work_dir_when_bcftools_fails(tmp_path, monkeypatch):
    def failing_query(*args, **kwargs):
        raise RuntimeError("bcftools query failed (exit 1): boom")
    monkeypatch.setattr(munge.rsid, "query_dbsnp", failing_query)
    with pytest.raises(RuntimeError, match="bcftools query failed"):
        _run(tmp_path, write_gwas(tmp_path, SCENARIO_ROWS))
    assert not (tmp_path / "out/.work").exists()
    assert not (tmp_path / "out/standardised.tsv.gz").exists()


def test_munge_file_unknown_contig_raises(tmp_path):
    # chromosome 2 is a kept chromosome but absent from the FASTA: configuration error
    row = ["2", "10", "A", "G", "0.3", "0.9", "300", "0.1", "0.05", "0.01"]
    with pytest.raises(KeyError):
        _run(tmp_path, write_gwas(tmp_path, [row]))


def test_munge_file_ignores_blank_lines(tmp_path):
    gw = write_gwas(tmp_path, [SCENARIO["snv_fwd"]], gzip=False)
    with open(gw, "a") as fh:
        fh.write("\n\n")
    res, _ = _run(tmp_path, gw)
    assert res["n_in"] == 1 and res["n_out"] == 1 and res["n_dropped"]["invalid"] == 0


# Reference bases the indel cases below rely on: 1:160 C, 1:161 C, 1:162 A.
_DEL_AT_161 = {"oa_majority": _gwas_row("1", 161, "CA", "C"),   # oa = REF "CA"
               "ea_majority": _gwas_row("1", 161, "C", "CA")}   # ea = REF "CA" (swapped)


def test_munge_file_indel_windows_in_oa_majority_file(tmp_path):
    assert BASE_AT["1"][159:162] == "CCA"
    rows = [SCENARIO["snv_fwd"],
            _gwas_row("1", 160, "CA", "C"),      # swapped insertion: ea C is REF, oa CA is not
            _DEL_AT_161["oa_majority"]]          # deletion: both CA and C match -> ambiguous
    res, out = _run(tmp_path, write_gwas(tmp_path, rows))
    assert res["orientation_prescan"]["prefer"] == "oa"
    by = out.set_index("SNP_ID")
    assert by.loc["1:160:C:CA", ["Effect_allele", "Non-effect_allele"]].tolist() == ["C", "CA"]
    assert by.loc["1:161:CA:C", ["Effect_allele", "Non-effect_allele"]].tolist() == ["C", "CA"]
    assert res["n_out"] == 3 and res["n_swapped"] == 1 and res["n_indel_ambiguous"] == 1


def test_munge_file_ea_majority_file_orients_ambiguous_indels_by_ea(tmp_path):
    snvs_ea_is_ref = [_gwas_row("1", 150, "A", "T"), _gwas_row("1", 120, "G", "A"),
                      _gwas_row("1", 200, "T", "A"), _gwas_row("X", 50, "T", "G"),
                      _gwas_row("1", 300, "C", "A")]
    res, out = _run(tmp_path, write_gwas(tmp_path, snvs_ea_is_ref + [_DEL_AT_161["ea_majority"]]))
    assert res["orientation_prescan"] == {"n_snv": 5, "n_oa_is_ref": 0, "n_ea_is_ref": 5,
                                          "prefer": "ea", "purity": 0.0}
    by = out.set_index("SNP_ID")
    assert by.loc["1:161:CA:C", ["Effect_allele", "Non-effect_allele"]].tolist() == ["CA", "C"]
    assert res["n_out"] == 6 and res["n_swapped"] == 6 and res["n_indel_ambiguous"] == 1
    # a perfectly consistent file (purity 0) never consults dbSNP for the tie
    assert res["n_indel_tie_dbsnp_resolved"] == 0 and res["n_indel_tie_unresolved"] == 0


def test_prescan_orientation_caps_at_max_rows(tmp_path):
    fa, db = build_reference(tmp_path)
    with Reference(fa, db) as ref:
        scan = munge._prescan_orientation(write_gwas(tmp_path, SCENARIO_ROWS),
                                          ColumnIndex.from_header(HDR, MAP), ref, max_rows=3)
    assert scan == {"n_snv": 3, "n_oa_is_ref": 2, "n_ea_is_ref": 1, "prefer": "oa",
                    "purity": 1 / 3}


def test_prescan_orientation_purity_is_zero_without_snvs(tmp_path):
    fa, db = build_reference(tmp_path)
    with Reference(fa, db) as ref:
        scan = munge._prescan_orientation(write_gwas(tmp_path, [_DEL_AT_161["oa_majority"]]),
                                          ColumnIndex.from_header(HDR, MAP), ref)
    assert scan == {"n_snv": 0, "n_oa_is_ref": 0, "n_ea_is_ref": 0, "prefer": "oa",
                    "purity": 0.0}


# contig 1: G at 1:51 then a 150 bp A-run (1:52-201); contig 2: the A-run starts the contig.
_HOMOPOLYMER = {"1": "C" * 50 + "G" + "A" * 150 + "T" + "C" * 50,
                "2": "A" * 150 + "T" + "C" * 50}
_RUN_INSERTIONS = [_gwas_row("1", 201, "A", "AA"),   # reported at the right end of the run
                   _gwas_row("2", 150, "A", "AA")]   # no anchor base left of the run


def test_munge_file_left_aligns_indel_beyond_default_padding(tmp_path):
    fasta = write_fasta(tmp_path, _HOMOPOLYMER)
    res, out = _run(tmp_path, write_gwas(tmp_path, _RUN_INSERTIONS), fasta=fasta)
    # contig 1 needs a 200 bp left window (one doubling) to reach the G anchor;
    # contig 2 can never be anchored, so it keeps the reported (un-normalised) ID.
    assert out["SNP_ID"].tolist() == ["1:51:G:GA", "2:150:A:AA"]
    assert out["Position"].tolist() == ["201", "150"]
    assert res["n_indel_norm_exhausted"] == 1 and res["n_out"] == 2


def test_munge_file_indel_norm_exhausted_falls_back_to_reported_id(tmp_path, monkeypatch):
    monkeypatch.setattr(munge, "_MAX_PAD_DOUBLINGS", 0)
    fasta = write_fasta(tmp_path, _HOMOPOLYMER)
    res, out = _run(tmp_path, write_gwas(tmp_path, _RUN_INSERTIONS), fasta=fasta)
    assert out["SNP_ID"].tolist() == ["1:201:A:AA", "2:150:A:AA"]
    assert res["n_indel_norm_exhausted"] == 2 and res["n_out"] == 2
    assert res["n_in"] == res["n_out"] + sum(res["n_dropped"].values())
    assert all(allele for sid in out["SNP_ID"] for allele in sid.split(":"))


def test_munge_file_mismatch_beyond_contig_end_writes_NA_ref_seq(tmp_path):
    fasta = write_fasta(tmp_path, _HOMOPOLYMER)
    res, _ = _run(tmp_path, write_gwas(tmp_path, [_gwas_row("2", 999, "A", "G")]), fasta=fasta)
    mm = pd.read_csv(tmp_path / "out/ref_mismatch.tsv.gz", sep="\t", dtype=str,
                     keep_default_na=False)
    assert res["n_dropped"]["ref_mismatch"] == 1 and mm.iloc[0]["Ref_seq"] == "NA"


# --- mixed-orientation files: ambiguous indels (C1) -------------------------------------

def _snvs(n_oa_is_ref: int, n_ea_is_ref: int) -> list[list[str]]:
    """SNVs on contig 1 (positions 1..400, wrapping), the first ``n_oa_is_ref`` coded oa = REF."""
    rows = []
    for i in range(n_oa_is_ref + n_ea_is_ref):
        pos = 1 + i % len(BASE_AT["1"])
        b = BASE_AT["1"][pos - 1]
        other = "ACGT".replace(b, "")[0]
        rows.append(_gwas_row("1", pos, b, other) if i < n_oa_is_ref else
                    _gwas_row("1", pos, other, b))
    return rows


def _tie_result(tmp_path, rows, dbsnp=True):
    fa, db = build_reference(tmp_path)
    with Reference(fa, db if dbsnp else None) as ref:
        res = munge_file(write_gwas(tmp_path, rows), MAP, ref, str(tmp_path / "out"),
                         cases=None, controls=None)
    out = pd.read_csv(tmp_path / "out/standardised.tsv.gz", sep="\t", dtype=str,
                      keep_default_na=False)
    return res, out.set_index("SNP_ID")


def test_mixed_orientation_file_breaks_indel_tie_with_dbsnp(tmp_path):
    # 85/15 oa/ea = REF SNVs (purity 0.15, prefer oa); the 1:161 CA>C deletion is reported
    # effect = REF (ea CA, oa C). Both readings match the FASTA; prefer would call it the
    # insertion C>CA, but only the deletion reading is in dbSNP (rs106).
    res, by = _tie_result(tmp_path, _snvs(85, 15) + [_DEL_AT_161["ea_majority"]])
    scan = res["orientation_prescan"]
    assert scan["prefer"] == "oa" and scan["purity"] == 0.15
    assert "1:161:C:CA" not in by.index
    assert by.loc["1:161:CA:C", ["Effect_allele", "Non-effect_allele", "RSID"]].tolist() == \
        ["CA", "C", "rs106"]                                   # alleles as reported
    assert res["n_indel_ambiguous"] == 1
    assert res["n_indel_tie_dbsnp_resolved"] == 1 and res["n_indel_tie_unresolved"] == 0
    assert res["n_swapped"] == 15 + 1


def test_mixed_orientation_tie_without_dbsnp_match_falls_back_to_prefer(tmp_path):
    res, by = _tie_result(tmp_path, _snvs(85, 15) + [_DEL_AT_161["ea_majority"]], dbsnp=False)
    assert by.loc["1:161:C:CA", ["Effect_allele", "Non-effect_allele"]].tolist() == ["CA", "C"]
    assert res["n_indel_tie_dbsnp_resolved"] == 0 and res["n_indel_tie_unresolved"] == 1
    assert res["n_indel_ambiguous"] == 1 and res["n_swapped"] == 15
    assert res["rsid_lookup"] == {"n_targets": 0, "n_hit_positions": 0, "bcftools": None}


def test_low_purity_file_still_breaks_indel_ties(tmp_path):
    # SGC_GNH-like: 1 of 125 SNVs coded effect = REF (purity 0.008 > 0.001)
    assert munge._TIE_BREAK_MIN_PURITY == 0.001
    res, by = _tie_result(tmp_path, _snvs(124, 1) + [_DEL_AT_161["ea_majority"]])
    assert res["orientation_prescan"]["purity"] == pytest.approx(0.008)
    assert by.loc["1:161:CA:C", "RSID"] == "rs106" and "1:161:C:CA" not in by.index
    assert res["n_indel_tie_dbsnp_resolved"] == 1 and res["n_indel_tie_unresolved"] == 0


def test_purity_at_threshold_keeps_prefer_for_indel_ties(tmp_path):
    # 1 of 1000 SNVs coded effect = REF: purity 0.001 is not above the threshold
    res, by = _tie_result(tmp_path, _snvs(999, 1) + [_DEL_AT_161["ea_majority"]])
    assert res["orientation_prescan"]["purity"] == 0.001
    assert "1:161:C:CA" in by.index and "1:161:CA:C" not in by.index
    assert res["n_indel_ambiguous"] == 1
    assert res["n_indel_tie_dbsnp_resolved"] == 0 and res["n_indel_tie_unresolved"] == 0


def _repeat_rich_contig(rng, length: int) -> str:
    """Random sequence with embedded homopolymers and dinucleotide repeats."""
    parts, total = [], 0
    while total < length:
        r = rng.random()
        if r < 0.03:
            part = rng.choice("ACGT") * rng.randint(3, 9)
        elif r < 0.05:
            part = (rng.choice("ACGT") + rng.choice("ACGT")) * rng.randint(3, 6)
        else:
            part = "".join(rng.choice("ACGT") for _ in range(rng.randint(5, 30)))
        parts.append(part)
        total += len(part)
    return "".join(parts)


def _truth_variants(rng, seq: str, n: int) -> list[tuple[int, str, str]]:
    """(pos1, REF, ALT) as a cohort might report them: SNVs, insertions copying the next
    base (often inside a repeat, so not left-aligned) and 1-2 bp deletions."""
    truth, used = [], set()
    for _ in range(n):
        p = rng.randint(50, len(seq) - 50)
        if p in used:
            continue
        used.add(p)
        b = seq[p - 1]
        k = rng.random()
        if k < 0.7:
            truth.append((p, b, rng.choice("ACGT".replace(b, ""))))
        elif k < 0.85:
            truth.append((p, b, b + seq[p]))
        else:
            d = rng.choice((1, 2))
            truth.append((p, seq[p - 1:p + d], b))
    return truth


def _left_normalised_id(seq: str, pos1: int, ref: str, alt: str) -> str:
    if len(ref) == len(alt) == 1:
        return f"1:{pos1}:{ref}:{alt}"
    p0, r, a = normalize_indel(seq, pos1 - 1, ref, alt)
    return f"1:{p0 + 1}:{r}:{a}"


def _munge_oriented(tmp_path, name, fasta, dbsnp, rows):
    path = tmp_path / f"{name}.tsv.gz"
    os.replace(write_gwas(tmp_path, rows), path)
    with Reference(fasta, dbsnp) as ref:
        res = munge_file(str(path), MAP, ref, str(tmp_path / name), cases=None, controls=None)
    out = pd.read_csv(tmp_path / name / "standardised.tsv.gz", sep="\t", dtype=str,
                      keep_default_na=False)
    return res, out


def test_same_variants_coded_effect_alt_or_effect_ref_get_identical_ids(tmp_path):
    # Reviewer probe: one variant set written once with effect = ALT and once with
    # effect = REF must produce the same SNP_IDs (the left-normalised truth), while the
    # alleles stay exactly as reported.
    rng = random.Random(7)
    seq = _repeat_rich_contig(rng, 60_000)
    truth = _truth_variants(rng, seq, 1500)
    expected = [_left_normalised_id(seq, *t) for t in truth]
    fasta = write_fasta(tmp_path, {"1": seq})
    res_a, a = _munge_oriented(tmp_path, "effect_alt", fasta, None,
                               [_gwas_row("1", p, r, x) for p, r, x in truth])
    res_b, b = _munge_oriented(tmp_path, "effect_ref", fasta, None,
                               [_gwas_row("1", p, x, r) for p, r, x in truth])
    assert res_a["orientation_prescan"]["prefer"] == "oa"
    assert res_b["orientation_prescan"]["prefer"] == "ea"
    assert res_a["n_indel_ambiguous"] == res_b["n_indel_ambiguous"] > 100
    assert res_a["n_out"] == res_b["n_out"] == len(truth)
    assert a["SNP_ID"].tolist() == b["SNP_ID"].tolist() == expected
    assert b["Effect_allele"].tolist() == [r for _, r, _ in truth]       # as reported
    assert b["Non-effect_allele"].tolist() == [x for _, _, x in truth]


def test_minor_allele_coded_file_gets_truth_ids_when_dbsnp_knows_the_indels(tmp_path):
    # Reviewer probe: ~12% of variants reported effect = REF (minor-allele coding). With
    # only the file-level prefer, every flipped ambiguous indel took the opposite
    # variant's ID; with dbSNP holding the true alleles the tie is broken per row.
    rng = random.Random(7)
    seq = _repeat_rich_contig(rng, 60_000)
    truth = _truth_variants(rng, seq, 1500)
    expected = [_left_normalised_id(seq, *t) for t in truth]
    flip = [rng.random() < 0.12 for _ in truth]
    rows = [_gwas_row("1", p, x if f else r, r if f else x) for (p, r, x), f in zip(truth, flip)]
    records = sorted({(int(e.split(":")[1]), e.split(":")[2], e.split(":")[3])
                      for e in expected})
    dbsnp = write_dbsnp(tmp_path, [("1", p, f"rs{i + 1}", r, x)
                                   for i, (p, r, x) in enumerate(records)], contigs=["1"])
    fasta = write_fasta(tmp_path, {"1": seq})

    res, out = _munge_oriented(tmp_path, "mixed", fasta, dbsnp, rows)
    assert res["orientation_prescan"]["prefer"] == "oa"
    assert res["orientation_prescan"]["purity"] > munge._TIE_BREAK_MIN_PURITY
    assert res["n_out"] == len(truth) and res["n_with_rsid"] == len(truth)
    assert out["SNP_ID"].tolist() == expected
    assert res["n_indel_tie_dbsnp_resolved"] == res["n_indel_ambiguous"] > 100
    assert res["n_indel_tie_unresolved"] == 0

    # the same file without dbSNP: flipped ambiguous indels keep the prefer (wrong) reading
    res0, out0 = _munge_oriented(tmp_path, "mixed_no_dbsnp", fasta, None, rows)
    wrong = sum(e != s for e, s in zip(expected, out0["SNP_ID"]))
    assert wrong > 0 and res0["n_indel_tie_unresolved"] == res0["n_indel_ambiguous"]


# --- wrong-build guard (I3) -------------------------------------------------------------

def _build_rows(n_good: int, n_bad: int, rng) -> list[list[str]]:
    """SNVs on contig 1: ``n_good`` with oa = REF, ``n_bad`` with neither allele REF."""
    rows = []
    for i in range(n_good + n_bad):
        pos = 1 + i % len(BASE_AT["1"])
        b = BASE_AT["1"][pos - 1]
        if i < n_good:
            rows.append(_gwas_row("1", pos, b, rng.choice("ACGT".replace(b, ""))))
        else:
            oa, ea = rng.sample("ACGT".replace(b, ""), 2)
            rows.append(_gwas_row("1", pos, oa, ea))
    return rows


def test_random_alleles_fail_the_wrong_build_guard(tmp_path):
    rng = random.Random(5)
    rows = []
    for i in range(1200):
        pos = 1 + i % len(BASE_AT["1"])
        rows.append(_gwas_row("1", pos, *rng.sample("ACGT", 2)))
    with pytest.raises(ValueError, match=r"reference match rate 0\.\d{3} over 1200 SNVs "
                                         r"suggests the file is not GRCh38 \(expected >= 0\.95\)"):
        _run(tmp_path, write_gwas(tmp_path, rows))
    assert not (tmp_path / "out/standardised.tsv.gz").exists()


def test_wrong_build_guard_threshold(tmp_path):
    rng = random.Random(6)
    res, _ = _run(tmp_path, write_gwas(tmp_path, _build_rows(960, 40, rng)))   # rate 0.96
    assert res["n_dropped"]["ref_mismatch"] == 40
    with pytest.raises(ValueError, match="reference match rate 0.940 over 1000 SNVs"):
        _run(tmp_path, write_gwas(tmp_path, _build_rows(940, 60, rng)))


def test_wrong_build_guard_needs_1000_snvs(tmp_path):
    res, _ = _run(tmp_path, write_gwas(tmp_path, _build_rows(0, 999, random.Random(7))))
    assert res["orientation_prescan"]["n_snv"] == 999 and res["n_out"] == 0
