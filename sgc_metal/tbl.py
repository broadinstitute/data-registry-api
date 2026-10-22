import gzip
import math
import re
from dataclasses import dataclass, field
from typing import Iterator, Optional

TBL_COLUMNS = ["MarkerName", "Allele1", "Allele2", "Freq1", "FreqSE", "MinFreq", "MaxFreq", "Effect",
               "StdErr", "P-value", "Direction", "HetISq", "HetChiSq", "HetDf", "HetPVal", "Ncases",
               "Ncontrols"]
# beta/freq1 are for `alt` (from MarkerName) unless orientation == "unresolved", when they are
# METAL's, for allele1. pvalue is clamped to the smallest positive double; pvalue_str is METAL's.
TOP_LOCI_COLUMNS = ["chromosome", "position", "ref", "alt", "allele1", "allele2", "freq1", "beta", "se",
                    "pvalue", "n_cohorts", "dir_concordance", "i2", "orientation", "pvalue_str", "log10p"]

# Smallest positive double. METAL prints p below double range for |z| >= ~38 (e.g. "6.41e-4346"),
# which float() turns into 0.0; plots and lambda get this instead, ordering uses log10p.
MIN_PVALUE = math.nextafter(0.0, 1.0)
_PVALUE_RE = re.compile(r"^(\d+(?:\.\d*)?|\.\d+)(?:[eE]([+-]?\d+))?$")


@dataclass
class TblRow:
    chromosome: str
    position: int
    ref: str
    alt: str
    allele1: str
    allele2: str
    freq1: Optional[float]
    effect: float
    se: float
    pvalue: float
    direction: str
    n_cohorts: int
    dir_concordance: Optional[float]
    i2: Optional[float]
    ncases: Optional[float]
    ncontrols: Optional[float]
    pvalue_str: str = ""
    log10p: float = field(default=0.0)


def parse_marker(marker: str) -> tuple[str, int, str, str]:
    chrom, pos, ref, alt = marker.split(":", 3)
    return chrom, int(pos), ref, alt


def direction_stats(direction: str) -> tuple[int, Optional[float]]:
    n_plus, n_minus = direction.count("+"), direction.count("-")
    n = n_plus + n_minus + direction.count("0")
    return n, (max(n_plus, n_minus) / n if n else None)


def _opt(s: str) -> Optional[float]:
    s = s.strip()
    if s == "" or s.upper() in ("NA", "NAN"):
        return None
    return float(s)


def _req(s: str, name: str) -> float:
    v = _opt(s)
    if v is None:
        raise ValueError(f"missing required value for {name}: {s!r}")
    return v


def parse_pvalue(s: str) -> tuple[float, float]:
    """(pvalue, log10p) from METAL's p-value string. log10p comes from the mantissa and
    exponent, so it stays exact below double range; pvalue is clamped to MIN_PVALUE when
    the string does not fit a positive double (and for "0"). NA raises ValueError."""
    v = _req(s, "P-value")
    m = _PVALUE_RE.match(s.strip())
    if m:
        mantissa = float(m.group(1))
        log10p = math.log10(mantissa) + int(m.group(2) or 0) if mantissa > 0 else -math.inf
    else:  # e.g. "inf": not something METAL prints for p
        log10p = math.log10(v) if v > 0 else -math.inf
    return (v if v > 0 else MIN_PVALUE), log10p


def iter_tbl(path: str) -> Iterator[TblRow]:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt") as fh:
        header = fh.readline().rstrip("\r\n").split("\t")
        if header != TBL_COLUMNS:
            raise ValueError(f"unexpected .tbl header: {header}")
        for line in fh:
            line = line.rstrip("\r\n")
            if not line:
                continue
            f = line.split("\t")
            chrom, pos, ref, alt = parse_marker(f[0])
            n, conc = direction_stats(f[10])
            pvalue, log10p = parse_pvalue(f[9])
            yield TblRow(
                chromosome=chrom, position=pos, ref=ref, alt=alt,
                allele1=f[1].upper(), allele2=f[2].upper(), freq1=_opt(f[3]),
                effect=_req(f[7], "Effect"), se=_req(f[8], "StdErr"), pvalue=pvalue,
                direction=f[10], n_cohorts=n, dir_concordance=conc, i2=_opt(f[11]),
                ncases=_opt(f[15]), ncontrols=_opt(f[16]), pvalue_str=f[9].strip(), log10p=log10p,
            )


def allele1_is_alt(r: TblRow) -> Optional[bool]:
    """Whether METAL's Allele1 is the marker's ALT (True), its REF (False), or unknown (None).
    METAL sorts each marker's alleles, so Allele1 is REF for about half of the SNPs. Munge keeps
    reported allele strings and normalises only SNP_ID, so an indel may be reported in another
    form (TA/T for AT>A): then the longer reported allele stands for the longer of (ref, alt)."""
    a1, a2, ref, alt = r.allele1.upper(), r.allele2.upper(), r.ref.upper(), r.alt.upper()
    if a1 == alt:
        return True
    if a1 == ref:
        return False
    if len(a1) == len(a2) or len(ref) == len(alt):
        return None
    a1_longer, alt_longer = len(a1) > len(a2), len(alt) > len(ref)
    return a1_longer == alt_longer


def row_to_locus(r: TblRow) -> dict:
    is_alt = allele1_is_alt(r)
    beta, freq1 = r.effect, r.freq1
    if is_alt is False:
        beta = -beta
        freq1 = None if freq1 is None else 1.0 - freq1
    return {
        "chromosome": r.chromosome, "position": r.position, "ref": r.ref, "alt": r.alt,
        "allele1": r.allele1, "allele2": r.allele2, "freq1": freq1, "beta": beta, "se": r.se,
        "pvalue": r.pvalue, "n_cohorts": r.n_cohorts, "dir_concordance": r.dir_concordance, "i2": r.i2,
        "orientation": "unresolved" if is_alt is None else "alt",
        "pvalue_str": r.pvalue_str, "log10p": r.log10p,
    }
