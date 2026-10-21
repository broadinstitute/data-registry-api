"""Synthetic GRCh38-like reference (FASTA + dbSNP VCF) for sgc_munge tests."""
import gzip as _gzip
import os
import random
import shutil
from pathlib import Path

import pysam
import pytest

HDR = ["CHROM", "GENPOS", "ALLELE0", "ALLELE1", "A1FREQ", "INFO", "N", "BETA", "SE", "P"]
MAP = {"col_chromosome": "CHROM", "col_position": "GENPOS", "col_non_effect_allele": "ALLELE0",
       "col_effect_allele": "ALLELE1", "col_effect_allele_freq": "A1FREQ",
       "col_imputation_quality": "INFO", "col_beta": "BETA", "col_se": "SE", "col_pvalue": "P"}

requires_bcftools = pytest.mark.skipif(
    shutil.which(os.environ.get("SGC_BCFTOOLS", "bcftools")) is None, reason="bcftools not on PATH")

_LENGTHS = {"1": 400, "X": 200}
# (contig, 1-based pos, base) forced into the otherwise pseudo-random sequence.
# Contig 1 bases 101-106 are the 'GAAAAT' homopolymer motif used for left-alignment tests.
_FORCED = [("1", 120, "A"), ("1", 150, "T"), ("1", 160, "C"), ("1", 200, "A"),
           ("1", 250, "A"), ("1", 300, "A"), ("X", 50, "G")]
_MOTIF = ("1", 101, "GAAAAT")


def _make_sequences() -> dict[str, str]:
    rng = random.Random(20261020)
    seqs = {c: [rng.choice("ACGT") for _ in range(n)] for c, n in _LENGTHS.items()}
    contig, start, motif = _MOTIF
    seqs[contig][start - 1:start - 1 + len(motif)] = list(motif)
    for c, pos, base in _FORCED:
        seqs[c][pos - 1] = base
    return {c: "".join(s) for c, s in seqs.items()}


# contig -> full sequence string (0-based indexing: BASE_AT["1"][119] is 1:120)
BASE_AT = _make_sequences()

# (contig, pos, id, ref, alt) -- sorted by contig then position
_DBSNP = [
    ("1", 120, "rs100", "A", "G"),
    ("1", 120, "rs101", "A", "C"),
    ("1", 150, "rs102", "T", "A"),
    ("1", 160, "rs103", "C", "CA"),
    ("1", 161, "rs106", "CA", "C"),   # deletion of the A at 1:162 (1:160-162 is CCA)
    ("1", 300, "rs105", "A", "G,T"),
    ("X", 50, "rs104", "G", "T"),
]


def build_reference(tmp_path) -> tuple[str, str]:
    """Write bgzipped+indexed FASTA and dbSNP VCF under tmp_path; return (fasta_path, dbsnp_path)."""
    tmp_path = Path(tmp_path)
    plain = str(tmp_path / "ref.fa")
    with open(plain, "w") as fh:
        for contig, seq in BASE_AT.items():
            fh.write(f">{contig}\n")
            for i in range(0, len(seq), 60):
                fh.write(seq[i:i + 60] + "\n")
    fa_gz = plain + ".gz"
    pysam.tabix_compress(plain, fa_gz, force=True)
    pysam.faidx(fa_gz)

    return fa_gz, write_dbsnp(tmp_path, _DBSNP, contigs=list(BASE_AT))


def write_dbsnp(tmp_path, records, contigs, name: str = "dbsnp.vcf") -> str:
    """Write a bgzipped + tabix-indexed sites VCF of ``(contig, pos, id, ref, alt)`` records
    (already sorted by contig, then position); return its path."""
    vcf = str(Path(tmp_path) / name)
    with open(vcf, "w") as fh:
        fh.write("##fileformat=VCFv4.2\n")
        for contig in contigs:
            fh.write(f"##contig=<ID={contig}>\n")
        fh.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
        for contig, pos, rid, ref, alt in records:
            fh.write(f"{contig}\t{pos}\t{rid}\t{ref}\t{alt}\t.\t.\t.\n")
    vcf_gz = vcf + ".gz"
    pysam.tabix_compress(vcf, vcf_gz, force=True)
    pysam.tabix_index(vcf_gz, preset="vcf", force=True)
    return vcf_gz


def _base(contig: str, pos: int) -> str:
    return BASE_AT[contig][pos - 1]


def _gwas_row(chrom, pos, oa, ea, eaf="0.3", info="0.9", beta="0.1", se="0.05", p="0.01", n="300"):
    """One GWAS line in HDR order (ALLELE0 = non-effect, ALLELE1 = effect)."""
    return [chrom, str(pos), oa, ea, eaf, info, n, beta, se, p]


_SNV_FWD = ("1", 120, _base("1", 120), "G")  # oa is the reference base A; rs100 A>G

# Canonical munge scenario (name -> row); order matters (duplicates follow snv_fwd).
SCENARIO = {
    "snv_fwd": _gwas_row(*_SNV_FWD),
    "snv_swapped": _gwas_row("1", 150, "A", _base("1", 150), beta="0.4", eaf="0.7"),
    "palindromic": _gwas_row("1", 200, _base("1", 200), "T"),
    "indel_norm": _gwas_row("1", 160, _base("1", 160), _base("1", 160) + "A"),
    # insertion of an A reported at the right end of the GAAAAT motif (1:101-106)
    "ins_shift": _gwas_row("1", 105, _base("1", 105), _base("1", 105) * 2),
    "mismatch": _gwas_row("1", 250, "C", "G"),  # reference base is A
    "no_rsid": _gwas_row("1", 300, _base("1", 300), "C"),  # rs105 is A>G,T
    "chrX": _gwas_row("X", 50, _base("X", 50), "T"),
    "mt": _gwas_row("MT", 10, "A", "G"),
    "maf_fail": _gwas_row(*_SNV_FWD, eaf="0.001"),
    "info_fail": _gwas_row(*_SNV_FWD, info="0.1"),
    "dup": _gwas_row(*_SNV_FWD),
    "dup_flipped": _gwas_row("1", 120, "G", _base("1", 120), eaf="0.7"),
    "invalid": _gwas_row(*_SNV_FWD, se="0"),
}
SCENARIO_ROWS = list(SCENARIO.values())


def write_gwas(tmp_path, rows, header=HDR, gzip=True) -> str:
    """Write a tab-delimited GWAS file (gzipped unless gzip=False); return its path."""
    path = Path(tmp_path) / ("gwas.tsv.gz" if gzip else "gwas.tsv")
    opener = _gzip.open if gzip else open
    with opener(path, "wt") as fh:
        for fields in [header, *rows]:
            fh.write("\t".join(fields) + "\n")
    return str(path)


def write_fasta(tmp_path, seqs: dict[str, str], name: str = "custom.fa") -> str:
    """Write a bgzipped + faidx-indexed FASTA of ``seqs`` (contig -> sequence); return its path."""
    plain = Path(tmp_path) / name
    with open(plain, "w") as fh:
        for contig, seq in seqs.items():
            fh.write(f">{contig}\n{seq}\n")
    gz = str(plain) + ".gz"
    pysam.tabix_compress(str(plain), gz, force=True)
    pysam.faidx(gz)
    return gz
