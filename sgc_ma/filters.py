"""Pure per-variant filter predicates for the SGC meta-analysis.

Both predicates treat a missing value (None/NaN) as "filter not applicable"
and return True, so a cohort lacking the column is never dropped for it.
"""


def maf_ok(eaf, maf_min: float = 0.005) -> bool:
    """True if eaf is absent/NaN (not applicable) or min(eaf, 1-eaf) >= maf_min.
    An out-of-range eaf (outside [0, 1]) fails naturally: min(eaf, 1-eaf) goes
    negative, which is < maf_min."""
    if eaf is None or eaf != eaf:   # None or NaN
        return True
    return min(eaf, 1.0 - eaf) >= maf_min


def info_ok(info, info_min: float = 0.3) -> bool:
    """True if info is absent/NaN (not applicable) or info >= info_min.
    No upper bound (some imputation methods report INFO > 1)."""
    if info is None or info != info:   # None or NaN
        return True
    return info >= info_min
