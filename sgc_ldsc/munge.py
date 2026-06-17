"""Munge an SGC GWAS file into aligned (rs_id, Z, N) records.

Ported from dig-ldsc-methods/src/ldsc/sumstats/main.py. Differences:
  * SGC column_mapping uses col_* keys (translated by build_col_map()).
  * Effect allele (EA) is ALT, other allele (OA) is REF -> var_id chr:pos:OA:EA.
  * N is per-variant effective N from N_case/N_control (case/control GWAS).
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
        # col_variant_n is the literal string "N_case, N_control"
        "ncase": (column_mapping.get("col_variant_n", "").split(",")[0].strip() or None),
        "ncontrol": ((column_mapping.get("col_variant_n", "").split(",")[1].strip()
                      if "," in column_mapping.get("col_variant_n", "") else None)),
    }


def sgc_var_id(chrom: str, pos: str, oa: str, ea: str) -> str:
    return f"{chrom}:{pos}:{oa.upper()}:{ea.upper()}"


def p_to_z(p: float, beta: float) -> float:
    return float(np.sqrt(chi2.isf(p, 1)) * (-1) ** (beta < 0))


def effective_n(ncase: float, ncontrol: float) -> float:
    return 4.0 / (1.0 / ncase + 1.0 / ncontrol)


def _valid(line: Dict, cm: Dict) -> bool:
    for k in ("chrom", "pos", "ea", "oa", "p"):
        if not line.get(cm[k]):
            return False
    try:
        return 0 < float(line[cm["p"]]) <= 1
    except ValueError:
        return False


def munge_records(rows, cm, snpmap, snpmap_flipped) -> List[Tuple[str, float, float]]:
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
            n = effective_n(float(line[cm["ncase"]]), float(line[cm["ncontrol"]]))
            out.append((rs_id, p_to_z(p, beta), n))
        except (ValueError, KeyError, ZeroDivisionError):
            continue
    return out


def n90_filter(records: List[Tuple[str, float, float]]) -> Dict[str, Tuple[float, float]]:
    """Drop SNPs with N < N90/1.5; collapse to {rs_id: (Z, N)} (last wins)."""
    if not records:
        return {}
    n90 = float(np.quantile([r[2] for r in records], 0.9))
    return {r[0]: (r[1], r[2]) for r in records if r[2] >= n90 / 1.5}
