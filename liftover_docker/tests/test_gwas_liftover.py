"""
Pure-Python unit tests for gwas_liftover.py.

These tests do NOT require the UCSC liftOver binary or AWS credentials, so
they run in CI without Docker. Tested functions:
  - standardize_chromosome
  - reverse_complement
  - pick_chain_file
  - tsv_to_bed
  - apply_lifted_positions
"""

import sys
import os

import pandas as pd
import pytest

# Allow importing from the parent liftover_docker/ directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from gwas_liftover import (
    standardize_chromosome,
    reverse_complement,
    pick_chain_file,
    tsv_to_bed,
    apply_lifted_positions,
    validate_column_mapping,
)


# ---------------------------------------------------------------------------
# standardize_chromosome
# ---------------------------------------------------------------------------

class TestStandardizeChromosome:
    """Truth table for standardize_chromosome."""

    def test_plain_number(self):
        assert standardize_chromosome("1") == "chr1"

    def test_chr_prefix_stripped_and_readded(self):
        assert standardize_chromosome("chr1") == "chr1"

    def test_large_autosome(self):
        assert standardize_chromosome("22") == "chr22"

    def test_chr_prefix_on_large_autosome(self):
        assert standardize_chromosome("chr22") == "chr22"

    def test_numeric_alias_23_to_X(self):
        assert standardize_chromosome("23") == "chrX"

    def test_X_passthrough(self):
        assert standardize_chromosome("X") == "chrX"

    def test_chrX_passthrough(self):
        assert standardize_chromosome("chrX") == "chrX"

    def test_numeric_alias_24_to_Y(self):
        assert standardize_chromosome("24") == "chrY"

    def test_Y_passthrough(self):
        assert standardize_chromosome("Y") == "chrY"

    def test_chrY_passthrough(self):
        assert standardize_chromosome("chrY") == "chrY"

    def test_numeric_alias_25_to_M(self):
        assert standardize_chromosome("25") == "chrM"

    def test_M_passthrough(self):
        assert standardize_chromosome("M") == "chrM"

    def test_chrM_passthrough(self):
        assert standardize_chromosome("chrM") == "chrM"

    def test_mixed_case_chr_prefix(self):
        # 'Chr1' should also be stripped
        assert standardize_chromosome("Chr1") == "chr1"

    def test_chr10(self):
        assert standardize_chromosome("chr10") == "chr10"

    def test_numeric_10(self):
        assert standardize_chromosome("10") == "chr10"


# ---------------------------------------------------------------------------
# reverse_complement
# ---------------------------------------------------------------------------

class TestReverseComplement:
    """Tests for reverse_complement allele handling."""

    def test_A_to_T(self):
        assert reverse_complement("A") == "T"

    def test_T_to_A(self):
        assert reverse_complement("T") == "A"

    def test_C_to_G(self):
        assert reverse_complement("C") == "G"

    def test_G_to_C(self):
        assert reverse_complement("G") == "C"

    def test_lowercase_a(self):
        assert reverse_complement("a") == "T"

    def test_lowercase_c(self):
        assert reverse_complement("c") == "G"

    def test_multi_base_AC(self):
        # AC -> reverse complement of CA = GT (complement C->G, A->T; reversed)
        # complement: A->T, C->G  => TC; reversed => CT... wait:
        # AC: complement each = TG; reverse = GT
        assert reverse_complement("AC") == "GT"

    def test_multi_base_ACGT(self):
        # Complement: A->T, C->G, G->C, T->A => TGCA; reversed => ACGT
        assert reverse_complement("ACGT") == "ACGT"

    def test_multi_base_indel_insertion(self):
        # 'TA' complement: T->A, A->T => AT; reversed => TA
        assert reverse_complement("TA") == "TA"

    def test_multi_base_ATG(self):
        # A->T, T->A, G->C => TAC; reversed => CAT
        assert reverse_complement("ATG") == "CAT"

    def test_N_passes_through(self):
        # N has no complement in the table — should pass through
        assert reverse_complement("N") == "N"

    def test_dash_passes_through(self):
        assert reverse_complement("-") == "-"

    def test_dot_passes_through(self):
        assert reverse_complement(".") == "."

    def test_mixed_with_unknown(self):
        # 'AN' -> complement A->T, N->N; reversed => NT
        assert reverse_complement("AN") == "NT"


# ---------------------------------------------------------------------------
# pick_chain_file
# ---------------------------------------------------------------------------

class TestPickChainFile:
    def test_hg19_to_hg38(self):
        chain = pick_chain_file("hg19", "hg38")
        assert "hg19ToHg38" in chain

    def test_hg38_to_hg19(self):
        chain = pick_chain_file("hg38", "hg19")
        assert "hg38ToHg19" in chain

    def test_case_insensitive(self):
        chain = pick_chain_file("HG19", "HG38")
        assert "hg19ToHg38" in chain

    def test_invalid_combination_exits(self):
        with pytest.raises(SystemExit):
            pick_chain_file("hg38", "hg38")

    def test_unknown_build_exits(self):
        with pytest.raises(SystemExit):
            pick_chain_file("hg17", "hg18")


# ---------------------------------------------------------------------------
# tsv_to_bed
# ---------------------------------------------------------------------------

COLUMN_MAPPING = {
    "chromosome": "chr",
    "position": "pos",
    "ref": "ref",
    "alt": "alt",
}

def make_small_df(rows=None) -> pd.DataFrame:
    """Build a tiny GWAS DataFrame for testing."""
    if rows is None:
        rows = [
            {"chr": "chr1", "pos": "10177", "ref": "A", "alt": "AC", "pval": "0.05"},
            {"chr": "1",    "pos": "10352", "ref": "T", "alt": "TA", "pval": "0.12"},
            {"chr": "chrX", "pos": "100000","ref": "C", "alt": "G",  "pval": "0.01"},
        ]
    return pd.DataFrame(rows)


class TestTsvToBed:
    def test_returns_list_of_strings(self):
        df = make_small_df()
        lines = tsv_to_bed(df, COLUMN_MAPPING)
        assert isinstance(lines, list)
        assert len(lines) == 3

    def test_tab_separated(self):
        df = make_small_df()
        lines = tsv_to_bed(df, COLUMN_MAPPING)
        for line in lines:
            parts = line.split("\t")
            assert len(parts) == 6, f"Expected 6 BED columns, got {len(parts)}: {line}"

    def test_chr_prefix_present(self):
        df = make_small_df()
        lines = tsv_to_bed(df, COLUMN_MAPPING)
        # Row with "1" should become "chr1"
        assert lines[1].startswith("chr1\t")

    def test_zero_based_start(self):
        df = make_small_df()
        lines = tsv_to_bed(df, COLUMN_MAPPING)
        # Row 0: pos=10177 -> start=10176, end=10177
        parts = lines[0].split("\t")
        assert int(parts[1]) == 10176  # 0-based start
        assert int(parts[2]) == 10177  # 1-based end

    def test_varid_format(self):
        df = make_small_df()
        lines = tsv_to_bed(df, COLUMN_MAPPING)
        # varid = {std_chr}_{pos}_{ref}_{alt}
        parts = lines[0].split("\t")
        assert parts[3] == "chr1_10177_A_AC"

    def test_strand_plus(self):
        df = make_small_df()
        lines = tsv_to_bed(df, COLUMN_MAPPING)
        for line in lines:
            parts = line.split("\t")
            assert parts[5] == "+"

    def test_score_dot(self):
        df = make_small_df()
        lines = tsv_to_bed(df, COLUMN_MAPPING)
        for line in lines:
            parts = line.split("\t")
            assert parts[4] == "."

    def test_duplicate_varid_skipped(self):
        rows = [
            {"chr": "chr1", "pos": "10177", "ref": "A", "alt": "AC", "pval": "0.05"},
            {"chr": "chr1", "pos": "10177", "ref": "A", "alt": "AC", "pval": "0.08"},  # duplicate
        ]
        df = pd.DataFrame(rows)
        lines = tsv_to_bed(df, COLUMN_MAPPING)
        assert len(lines) == 1  # second duplicate dropped

    def test_numeric_chr_alias(self):
        rows = [{"chr": "23", "pos": "100", "ref": "A", "alt": "T", "pval": "0.5"}]
        df = pd.DataFrame(rows)
        lines = tsv_to_bed(df, COLUMN_MAPPING)
        assert lines[0].startswith("chrX\t")
        assert "chrX_100_A_T" in lines[0]


# ---------------------------------------------------------------------------
# apply_lifted_positions
# ---------------------------------------------------------------------------

class TestApplyLiftedPositions:
    """Test join-back logic including strand-flip allele complementation."""

    def _make_orig_df(self):
        return pd.DataFrame([
            {"chr": "chr1", "pos": "10177", "ref": "A",  "alt": "AC", "beta": "0.01"},
            {"chr": "chr2", "pos": "20000", "ref": "G",  "alt": "A",  "beta": "0.02"},
            {"chr": "chr3", "pos": "30000", "ref": "C",  "alt": "T",  "beta": "0.03"},
        ])

    def _make_lifted_bed_stable(self) -> pd.DataFrame:
        """Lifted BED with no strand flips and same chromosomes."""
        return pd.DataFrame([
            {"chr": "chr1", "start": 10200, "end": 10201, "varid": "chr1_10177_A_AC",  "score": ".", "strand": "+"},
            {"chr": "chr2", "start": 20500, "end": 20501, "varid": "chr2_20000_G_A",   "score": ".", "strand": "+"},
            {"chr": "chr3", "start": 30500, "end": 30501, "varid": "chr3_30000_C_T",   "score": ".", "strand": "+"},
        ])

    def _make_lifted_bed_with_flip(self) -> pd.DataFrame:
        """Lifted BED where chr2 variant has a strand flip."""
        return pd.DataFrame([
            {"chr": "chr1", "start": 10200, "end": 10201, "varid": "chr1_10177_A_AC",  "score": ".", "strand": "+"},
            {"chr": "chr2", "start": 20500, "end": 20501, "varid": "chr2_20000_G_A",   "score": ".", "strand": "-"},
            {"chr": "chr3", "start": 30500, "end": 30501, "varid": "chr3_30000_C_T",   "score": ".", "strand": "+"},
        ])

    def test_stable_lift_preserves_alleles(self):
        orig = self._make_orig_df()
        lifted_bed = self._make_lifted_bed_stable()
        lifted_df, mismatch = apply_lifted_positions(orig, lifted_bed, COLUMN_MAPPING)

        assert len(lifted_df) == 3
        assert len(mismatch) == 0

        # chr1 row: ref/alt unchanged
        row1 = lifted_df[lifted_df["chr"] == "chr1"].iloc[0]
        assert row1["ref"] == "A"
        assert row1["alt"] == "AC"

    def test_stable_lift_updates_position(self):
        orig = self._make_orig_df()
        lifted_bed = self._make_lifted_bed_stable()
        lifted_df, _ = apply_lifted_positions(orig, lifted_bed, COLUMN_MAPPING)

        row1 = lifted_df[lifted_df["chr"] == "chr1"].iloc[0]
        assert int(row1["pos"]) == 10201  # end column from BED = new 1-based pos

    def test_strand_flip_complements_ref_and_alt(self):
        """Strand-flipped variant: both ref AND alt must be reverse-complemented."""
        orig = self._make_orig_df()
        lifted_bed = self._make_lifted_bed_with_flip()
        lifted_df, mismatch = apply_lifted_positions(orig, lifted_bed, COLUMN_MAPPING)

        assert len(mismatch) == 0
        # chr2 row had G/A; strand flip -> reverse_complement(G)=C, reverse_complement(A)=T
        row2 = lifted_df[lifted_df["chr"] == "chr2"].iloc[0]
        assert row2["ref"] == "C", f"Expected C, got {row2['ref']}"
        assert row2["alt"] == "T", f"Expected T, got {row2['alt']}"

    def test_strand_flip_does_not_affect_non_flipped(self):
        """Non-flipped variants must retain original alleles even when a flip exists elsewhere."""
        orig = self._make_orig_df()
        lifted_bed = self._make_lifted_bed_with_flip()
        lifted_df, _ = apply_lifted_positions(orig, lifted_bed, COLUMN_MAPPING)

        row3 = lifted_df[lifted_df["chr"] == "chr3"].iloc[0]
        assert row3["ref"] == "C"
        assert row3["alt"] == "T"

    def test_chr_mismatch_drops_variant(self):
        """If lifted chromosome differs from original, variant is excluded."""
        orig = pd.DataFrame([
            {"chr": "chr1", "pos": "10177", "ref": "A", "alt": "AC", "beta": "0.01"},
        ])
        # Lifted to a different chromosome (chr2 instead of chr1)
        lifted_bed = pd.DataFrame([
            {"chr": "chr2", "start": 10200, "end": 10201, "varid": "chr1_10177_A_AC", "score": ".", "strand": "+"},
        ])
        lifted_df, mismatch = apply_lifted_positions(orig, lifted_bed, COLUMN_MAPPING)
        assert len(lifted_df) == 0
        assert "chr1_10177_A_AC" in mismatch

    def test_unmapped_variant_excluded(self):
        """Variants with no entry in lifted BED are excluded from lifted output."""
        orig = self._make_orig_df()
        # Only lift chr1 and chr3; chr2 is absent (simulates liftOver couldn't map it)
        lifted_bed = pd.DataFrame([
            {"chr": "chr1", "start": 10200, "end": 10201, "varid": "chr1_10177_A_AC", "score": ".", "strand": "+"},
            {"chr": "chr3", "start": 30500, "end": 30501, "varid": "chr3_30000_C_T",  "score": ".", "strand": "+"},
        ])
        lifted_df, mismatch = apply_lifted_positions(orig, lifted_bed, COLUMN_MAPPING)
        assert len(lifted_df) == 2
        # chr2 row is absent — not in mismatch (it's handled by unmapped BED parsing)
        assert "chr2_20000_G_A" not in mismatch

    def test_empty_lifted_bed_produces_empty_output(self):
        orig = self._make_orig_df()
        empty_bed = pd.DataFrame(columns=["chr", "start", "end", "varid", "score", "strand"])
        lifted_df, mismatch = apply_lifted_positions(orig, empty_bed, COLUMN_MAPPING)
        assert len(lifted_df) == 0
        assert len(mismatch) == 0

    def test_multibase_allele_strand_flip(self):
        """Multi-base allele (indel) is correctly reverse-complemented on strand flip."""
        orig = pd.DataFrame([
            {"chr": "chr1", "pos": "500000", "ref": "AC", "alt": "ACGT", "beta": "0.0"},
        ])
        # AC -> complement A->T, C->G => TG; reversed => GT
        # ACGT -> complement A->T, C->G, G->C, T->A => TGCA; reversed => ACGT
        lifted_bed = pd.DataFrame([
            {"chr": "chr1", "start": 500100, "end": 500101,
             "varid": "chr1_500000_AC_ACGT", "score": ".", "strand": "-"},
        ])
        lifted_df, _ = apply_lifted_positions(orig, lifted_bed, COLUMN_MAPPING)
        assert len(lifted_df) == 1
        row = lifted_df.iloc[0]
        assert row["ref"] == "GT",   f"Expected GT, got {row['ref']}"
        assert row["alt"] == "ACGT", f"Expected ACGT, got {row['alt']}"

    def test_original_columns_preserved(self):
        """Extra columns (e.g. beta, se, pval) are preserved in lifted output."""
        orig = pd.DataFrame([
            {"chr": "chr1", "pos": "10177", "ref": "A", "alt": "AC",
             "beta": "0.01", "se": "0.001", "pval": "0.05"},
        ])
        lifted_bed = pd.DataFrame([
            {"chr": "chr1", "start": 10200, "end": 10201, "varid": "chr1_10177_A_AC",
             "score": ".", "strand": "+"},
        ])
        lifted_df, _ = apply_lifted_positions(orig, lifted_bed, COLUMN_MAPPING)
        assert "beta" in lifted_df.columns
        assert "se" in lifted_df.columns
        assert "pval" in lifted_df.columns
        assert lifted_df.iloc[0]["beta"] == "0.01"

    def test_duplicate_variants_deduplicated_in_output(self):
        """
        Two rows with identical chr/pos/ref/alt must produce exactly one row
        in the lifted output — 'keep first' semantics.
        """
        orig = pd.DataFrame([
            {"chr": "chr1", "pos": "10177", "ref": "A", "alt": "AC", "beta": "0.01"},
            {"chr": "chr1", "pos": "10177", "ref": "A", "alt": "AC", "beta": "0.99"},  # duplicate
        ])
        lifted_bed = pd.DataFrame([
            {"chr": "chr1", "start": 10200, "end": 10201,
             "varid": "chr1_10177_A_AC", "score": ".", "strand": "+"},
        ])
        lifted_df, mismatch = apply_lifted_positions(orig, lifted_bed, COLUMN_MAPPING)
        assert len(lifted_df) == 1, f"Expected 1 row, got {len(lifted_df)}"
        assert len(mismatch) == 0
        # The kept row should be the first one (beta=0.01)
        assert lifted_df.iloc[0]["beta"] == "0.01"


# ---------------------------------------------------------------------------
# validate_column_mapping
# ---------------------------------------------------------------------------

class TestValidateColumnMapping:
    """Tests for the column-mapping validation helper."""

    def test_valid_mapping_does_not_exit(self):
        df = pd.DataFrame(columns=["chr", "pos", "ref", "alt", "pval"])
        # Should not raise
        validate_column_mapping(df, COLUMN_MAPPING)

    def test_missing_column_exits(self):
        df = pd.DataFrame(columns=["chr", "pos", "ref"])  # 'alt' missing
        with pytest.raises(SystemExit):
            validate_column_mapping(df, COLUMN_MAPPING)

    def test_all_columns_missing_exits(self):
        df = pd.DataFrame(columns=["snp_id", "p_value"])
        with pytest.raises(SystemExit):
            validate_column_mapping(df, COLUMN_MAPPING)

    def test_extra_columns_in_df_are_fine(self):
        df = pd.DataFrame(columns=["chr", "pos", "ref", "alt", "beta", "se", "pval"])
        # All mapping values present — should not raise
        validate_column_mapping(df, COLUMN_MAPPING)
