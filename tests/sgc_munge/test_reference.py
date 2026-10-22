import pytest
from sgc_munge.reference import align_to_reference, normalize_indel, snp_id, pick_rsid

def test_align_oa_is_ref():
    assert align_to_reference("G", "A", "A") == ("A", "G", False)

def test_align_ea_is_ref_swaps():
    assert align_to_reference("A", "G", "A") == ("A", "G", True)

def test_align_palindromic_resolved_by_reference():
    assert align_to_reference("T", "A", "A") == ("A", "T", False)
    assert align_to_reference("A", "T", "A") == ("A", "T", True)

def test_align_neither_matches():
    assert align_to_reference("C", "G", "A") is None

def test_align_short_fetch_is_mismatch():
    assert align_to_reference("AT", "A", "") is None

def test_normalize_indel_snv_unchanged():
    assert normalize_indel("ACGTACGT", 3, "T", "A") == (3, "T", "A")

def test_normalize_indel_trims_common_prefix():
    # ref window ...CAG...; reported ref=CAG alt=CA at pos0=2 -> AG/A at pos0=3
    assert normalize_indel("TTCAGTT", 2, "CAG", "CA") == (3, "AG", "A")

def test_normalize_indel_left_aligns_in_homopolymer():
    # window GAAAAT, deletion of one A reported at the right end (pos0=4 'A'->'' as AT/T)
    assert normalize_indel("GAAAAT", 4, "AT", "T") == (0, "GA", "G")

def test_snp_id():
    assert snp_id("1", 12345, "A", "G") == "1:12345:A:G"

def test_pick_rsid_allele_match_multiallelic():
    recs = [("A", ("C",), "rs1"), ("A", ("G", "T"), "rs2")]
    assert pick_rsid(recs, "A", "G") == "rs2"

def test_pick_rsid_position_only_is_none():
    assert pick_rsid([("A", ("C",), "rs1")], "A", "G") is None

def test_pick_rsid_trims_padded_multiallelic_record():
    # dbSNP pads multi-ALT records to a shared REF: TA>T,TAA is the deletion TA>T plus the
    # insertion T>TA, which is how our normalised alleles name it.
    recs = [("TA", ("T", "TAA"), "rs7")]
    assert pick_rsid(recs, "T", "TA") == "rs7"
    assert pick_rsid(recs, "TA", "T") == "rs7"
    assert pick_rsid(recs, "T", "TAA") is None

def test_pick_rsid_trims_padded_snv_allele():
    assert pick_rsid([("AT", ("GT", "A"), "rs8")], "A", "G") == "rs8"

def test_pick_rsid_missing_id_is_none():
    assert pick_rsid([("A", ("G",), None)], "A", "G") is None

def test_pick_rsid_empty():
    assert pick_rsid([], "A", "G") is None

def test_reference_fetch_and_dbsnp_contigs(tmp_path):
    from sgc_munge.reference import Reference
    from tests.sgc_munge.fixtures import build_reference, BASE_AT
    fa, db = build_reference(tmp_path)
    with Reference(fa, db) as ref:
        assert ref.fetch("1", 119, 120) == BASE_AT["1"][119]
        assert ref.dbsnp_path == db and ref.dbsnp_contigs == frozenset({"1", "X"})
        assert not hasattr(ref, "rsid_at")

def test_fetch_unknown_contig_raises(tmp_path):
    from sgc_munge.reference import Reference
    from tests.sgc_munge.fixtures import build_reference
    fa, db = build_reference(tmp_path)
    with Reference(fa, db) as ref, pytest.raises(KeyError, match="contig 7 not in FASTA"):
        ref.fetch("7", 0, 1)

def test_reference_without_dbsnp(tmp_path):
    from sgc_munge.reference import Reference
    from tests.sgc_munge.fixtures import build_reference
    fa, _ = build_reference(tmp_path)
    with Reference(fa, None) as ref:
        assert ref.dbsnp_path is None and ref.dbsnp_contigs == frozenset()

def test_fixture_documented_bases(tmp_path):
    from tests.sgc_munge.fixtures import BASE_AT
    one = BASE_AT["1"]
    assert one[119] == "A" and one[149] == "T" and one[159] == "C"
    assert one[199] == "A" and one[249] == "A" and one[299] == "A"
    assert one[100:106] == "GAAAAT" and BASE_AT["X"][49] == "G"

def test_reference_rejects_unindexed_dbsnp(tmp_path):
    import pysam
    from sgc_munge.reference import Reference
    from tests.sgc_munge.fixtures import build_reference
    fa, _ = build_reference(tmp_path)
    plain = tmp_path / "noindex.vcf"
    plain.write_text("##fileformat=VCFv4.2\n##contig=<ID=1>\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
                     "1\t120\trs1\tA\tG\t.\t.\t.\n")
    gz = str(plain) + ".gz"
    pysam.tabix_compress(str(plain), gz, force=True)
    with pytest.raises(ValueError, match="dbSNP VCF has no tabix index"):
        Reference(fa, gz)

def test_reference_rejects_dbsnp_without_canonical_contigs(tmp_path):
    import pysam
    from sgc_munge.reference import Reference
    from tests.sgc_munge.fixtures import build_reference
    fa, _ = build_reference(tmp_path)
    plain = tmp_path / "refseq.vcf"
    plain.write_text("##fileformat=VCFv4.2\n##contig=<ID=NC_000001.11>\n"
                     "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
                     "NC_000001.11\t120\trs1\tA\tG\t.\t.\t.\n")
    gz = str(plain) + ".gz"
    pysam.tabix_compress(str(plain), gz, force=True)
    pysam.tabix_index(gz, preset="vcf", force=True)
    with pytest.raises(ValueError, match="none of 1..22, X, Y"):
        Reference(fa, gz)

