"""Reference alignment, indel normalisation and rsID matching (gwas2vcf algorithm).

rsIDs are not looked up row by row here: the munge queries dbSNP once per file with
``bcftools query --targets-file`` (see ``rsid.py``), and ``pick_rsid`` matches a row's
alleles against the records returned for its position. ``Reference`` holds the FASTA,
validates the dbSNP VCF and exposes its path and header contigs for that bulk query.
"""
import pysam

PADDING = 100  # bases fetched each side of a variant for normalize_indel

_CANONICAL_CONTIGS = frozenset([str(i) for i in range(1, 23)] + ["X", "Y"])


def align_to_reference(ea: str, oa: str, ref_seq: str) -> tuple[str, str, bool] | None:
    """Return (REF, ALT, swapped) given the reference sequence for the tested allele."""
    if oa == ref_seq:
        return oa, ea, False
    if ea == ref_seq:
        return ea, oa, True
    return None


def normalize_indel(seq: str, pos0: int, ref: str, alt: str) -> tuple[int, str, str]:
    """Left-align and trim against ``seq``; ``pos0`` is the 0-based index of ``ref`` in ``seq``."""
    if len(ref) == 1 and len(alt) == 1:
        return pos0, ref, alt
    while ref and alt and ref[-1] == alt[-1]:
        ref, alt = ref[:-1], alt[:-1]
        if not ref or not alt:
            if pos0 == 0:  # window exhausted; cannot left-extend further
                break
            pos0 -= 1
            base = seq[pos0]
            ref, alt = base + ref, base + alt
    while len(ref) > 1 and len(alt) > 1 and ref[0] == alt[0]:
        ref, alt = ref[1:], alt[1:]
        pos0 += 1
    return pos0, ref, alt


def snp_id(chrom: str, pos: int, ref: str, alt: str) -> str:
    return f"{chrom}:{pos}:{ref}:{alt}"


def _trim_common_suffix(ref: str, alt: str) -> tuple[str, str]:
    while len(ref) > 1 and len(alt) > 1 and ref[-1] == alt[-1]:
        ref, alt = ref[:-1], alt[:-1]
    return ref, alt


def pick_rsid(records, ref: str, alt: str) -> str | None:
    """First rsID among ``records`` -- ``(REF, ALTs, ID)`` tuples at one position -- with an
    allele matching (ref, alt).

    Each (REF, ALT) pair is compared after trimming its common suffix: dbSNP pads a
    multi-ALT record to one shared REF (``TA>T,TAA``), so its ``TAA`` allele is the
    insertion that our normalised alleles call ``T>TA``.
    """
    want = _trim_common_suffix(ref, alt)
    for rec_ref, rec_alts, rec_id in records:
        if rec_id is None:
            continue
        for rec_alt in rec_alts:
            if _trim_common_suffix(rec_ref, rec_alt) == want:
                return rec_id
    return None


class Reference:
    def __init__(self, fasta_path: str, dbsnp_path: str | None):
        self.fasta = pysam.FastaFile(fasta_path)
        self.dbsnp_path: str | None = dbsnp_path or None
        # contigs in the dbSNP header: only rows on these are sent to bcftools
        self.dbsnp_contigs: frozenset[str] = frozenset()
        if self.dbsnp_path:
            try:
                self.dbsnp_contigs = self._check_dbsnp(self.dbsnp_path)
            except Exception:
                self.fasta.close()
                raise

    @staticmethod
    def _check_dbsnp(dbsnp_path: str) -> frozenset[str]:
        """Reject an unindexed dbSNP VCF or one without 1..22, X, Y; return its header contigs."""
        with pysam.VariantFile(dbsnp_path) as vcf:
            if vcf.index is None:
                raise ValueError(f"dbSNP VCF has no tabix index: {dbsnp_path}")
            contigs = frozenset(vcf.header.contigs)
        if not contigs & _CANONICAL_CONTIGS:
            sample = sorted(contigs)[:5]
            raise ValueError(f"dbSNP VCF header contigs include none of 1..22, X, Y "
                             f"(e.g. {sample}); were the chromosomes renamed? {dbsnp_path}")
        return contigs

    def fetch(self, chrom: str, start0: int, end0: int) -> str:
        if chrom not in self.fasta.references:
            raise KeyError(f"contig {chrom} not in FASTA")
        return self.fasta.fetch(chrom, start0, end0).upper()

    def close(self):
        self.fasta.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
