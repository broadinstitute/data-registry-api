"""Munge an SGC GWAS file into aligned (rs_id, Z, N) records.

Ported from dig-ldsc-methods/src/ldsc/sumstats/main.py. Differences:
  * SGC column_mapping uses col_* keys (translated by build_col_map()).
  * Effect allele (EA) is ALT, other allele (OA) is REF -> var_id chr:pos:OA:EA.
  * N is the study-level effective sample size 4/(1/cases+1/controls), applied as
    a SCALAR to every variant. SGC case/control files carry cases/controls at the
    study level (sgc_gwas_files); `col_variant_n`, when present, is a single
    total-N column (not a usable per-variant *effective* N, since we have no
    per-variant case/control split), so we don't use it. The LDSC intercept is
    invariant to a constant N scaling, so this is safe for the headline metric;
    h2 then uses the correct case/control effective-N scale.
"""
from typing import Dict, List, Tuple

import numpy as np
from scipy.stats import chi2


def build_col_map(column_mapping: dict) -> dict:
    """Translate SGC col_* mapping -> the short keys this module uses."""
    return {
        "chrom": column_mapping["col_chromosome"],
        "pos": column_mapping["col_position"],
        "ea": column_mapping["col_effect_allele"],
        "oa": column_mapping["col_non_effect_allele"],
        "p": column_mapping["col_pvalue"],
        "beta": column_mapping.get("col_beta"),
    }


def sgc_var_id(chrom: str, pos: str, oa: str, ea: str) -> str:
    return f"{chrom}:{pos}:{oa.upper()}:{ea.upper()}"


def p_to_z(p: float, beta: float) -> float:
    return float(np.sqrt(chi2.isf(p, 1)) * (-1) ** (beta < 0))


def effective_n(ncase: float, ncontrol: float) -> float:
    """Effective sample size for a case/control study: 4 / (1/Ncase + 1/Ncontrol)."""
    return 4.0 / (1.0 / ncase + 1.0 / ncontrol)


def _valid(line: Dict, cm: Dict) -> bool:
    for k in ("chrom", "pos", "ea", "oa", "p"):
        if not line.get(cm[k]):
            return False
    try:
        return 0 < float(line[cm["p"]]) <= 1
    except ValueError:
        return False


def munge_records(rows, cm, snpmap, snpmap_flipped, effective_n) -> List[Tuple[str, float, float]]:
    """Map rows to (rs_id, Z, N). N is the study-level effective sample size
    (the scalar `effective_n`), identical for every variant — see module docstring."""
    out = []
    for line in rows:
        if not _valid(line, cm):
            continue
        var_id = sgc_var_id(line[cm["chrom"]], line[cm["pos"]], line[cm["oa"]], line[cm["ea"]])
        flipped = var_id in snpmap_flipped
        if not flipped and var_id not in snpmap:
            continue
        rs_id = snpmap_flipped[var_id] if flipped else snpmap[var_id]
        try:
            p = float(line[cm["p"]])
            beta = float(line[cm["beta"]]) * (1 - 2 * flipped)
            out.append((rs_id, p_to_z(p, beta), effective_n))
        except (ValueError, KeyError):
            continue
    return out


def n90_filter(records: List[Tuple[str, float, float]]) -> Dict[str, Tuple[float, float]]:
    """Collapse to {rs_id: (Z, N)} (last wins), dropping SNPs with N < N90/1.5.

    With a scalar effective N this filter is a no-op on N (all equal), matching the
    upstream behaviour when a single effective_n is supplied; it still de-dupes by rs.
    """
    if not records:
        return {}
    n90 = float(np.quantile([r[2] for r in records], 0.9))
    return {r[0]: (r[1], r[2]) for r in records if r[2] >= n90 / 1.5}
