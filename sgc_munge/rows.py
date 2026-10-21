"""Parse and validate one GWAS summary-statistics row (no I/O)."""
import math
import re
from dataclasses import dataclass

from sgc_ma.reader import REQUIRED_KEYS
from sgc_qc_plots.computations import CHROM_ALIASES, VALID_CHROMS

_ALLELE_RE = re.compile(r"^[ACGT]+$")


class RowError(Exception):
    """Row dropped; ``kind`` is "chrom" (unkept chromosome) or "invalid"."""

    def __init__(self, kind: str, message: str = ""):
        super().__init__(message or kind)
        self.kind = kind


@dataclass(frozen=True)
class ColumnIndex:
    chrom: int
    pos: int
    ea: int
    oa: int
    beta: int
    se: int
    pvalue: int
    eaf: int | None
    info: int | None

    @classmethod
    def from_header(cls, header: list[str], column_mapping: dict) -> "ColumnIndex":
        missing_keys = [k for k in REQUIRED_KEYS if not column_mapping.get(k)]
        if missing_keys:
            raise ValueError(f"column_mapping missing required keys: {missing_keys}")
        required_cols = [column_mapping[k] for k in REQUIRED_KEYS]
        missing_cols = [c for c in required_cols if c not in header]
        if missing_cols:
            raise ValueError(f"file missing required columns: {missing_cols}")

        def optional(key):
            col = column_mapping.get(key)
            return header.index(col) if col and col in header else None

        return cls(
            chrom=header.index(column_mapping["col_chromosome"]),
            pos=header.index(column_mapping["col_position"]),
            ea=header.index(column_mapping["col_effect_allele"]),
            oa=header.index(column_mapping["col_non_effect_allele"]),
            beta=header.index(column_mapping["col_beta"]),
            se=header.index(column_mapping["col_se"]),
            pvalue=header.index(column_mapping["col_pvalue"]),
            eaf=optional("col_effect_allele_freq"),
            info=optional("col_imputation_quality"),
        )


@dataclass
class ParsedRow:
    chrom: str
    pos: int
    ea: str
    oa: str
    beta: float
    se: float
    pvalue: float
    eaf: float
    info: float


def normalize_chrom(raw: str) -> str | None:
    """Map to canonical {1..22, X, Y}; anything else (MT, contigs) -> None."""
    v = raw.strip()
    if v[:3].lower() == "chr":
        v = v[3:]
    v = CHROM_ALIASES.get(v, v)
    return v if v in VALID_CHROMS else None


def _optional_float(fields: list[str], i: int | None) -> float:
    if i is None or i >= len(fields):
        return math.nan
    try:
        return float(fields[i])
    except ValueError:
        return math.nan


def parse_row(fields: list[str], idx: ColumnIndex) -> ParsedRow:
    needed = max(idx.chrom, idx.pos, idx.ea, idx.oa, idx.beta, idx.se, idx.pvalue)
    if len(fields) <= needed:
        raise RowError("invalid", "short line")
    chrom = normalize_chrom(fields[idx.chrom])
    if chrom is None:
        raise RowError("chrom")
    try:
        pos_f = float(fields[idx.pos])
        beta = float(fields[idx.beta])
        se = float(fields[idx.se])
        p = float(fields[idx.pvalue])
        pos = int(pos_f)
    except (ValueError, OverflowError):
        raise RowError("invalid", "non-numeric field") from None
    if pos <= 0:
        raise RowError("invalid", "pos <= 0")
    if not (math.isfinite(beta) and math.isfinite(se)):
        raise RowError("invalid", "non-finite beta/se")
    if se <= 0:
        raise RowError("invalid", "se <= 0")
    if not (0 < p <= 1):  # also rejects nan
        raise RowError("invalid", "p out of (0, 1]")
    ea = fields[idx.ea].strip().upper()
    oa = fields[idx.oa].strip().upper()
    if not (_ALLELE_RE.match(ea) and _ALLELE_RE.match(oa)):
        raise RowError("invalid", "bad allele")
    if ea == oa:
        raise RowError("invalid", "ea == oa")
    return ParsedRow(chrom, pos, ea, oa, beta, se, p,
                     _optional_float(fields, idx.eaf), _optional_float(fields, idx.info))
