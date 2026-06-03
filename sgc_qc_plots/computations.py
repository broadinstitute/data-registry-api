"""Pure computation helpers for SGC GWAS QC plots. No I/O."""
import numpy as np
import pandas as pd
from scipy.stats import chi2

VALID_CHROMS = {str(i) for i in range(1, 23)} | {"X", "Y"}
CHROM_ALIASES = {"23": "X", "24": "Y"}

_CHI2_MEDIAN = float(chi2.ppf(0.5, df=1))  # ~0.4549, exact median of chi^2_1


def lambda_gc(pvalues: pd.Series) -> float:
    """Genomic inflation factor: median(chi^2_1(p)) / median(chi^2_1)."""
    return float(np.median(chi2.isf(pvalues, df=1)) / _CHI2_MEDIAN)


def lambda_1000(lam: float, n_cases: int, n_controls: int) -> float:
    """Sample-size-adjusted lambda: what lambda would be at 1000 cases vs 1000 controls.

    Used to distinguish inflation from polygenicity vs systematic bias in
    case/control studies. Larger studies are more sensitive to small effects,
    so naive lambda inflates with N even when there is no confounding.
    """
    return 1.0 + (lam - 1.0) * 500.0 * (1.0 / n_cases + 1.0 / n_controls)


def normalize_chromosome(s: pd.Series) -> pd.Series:
    """Map chromosome labels to canonical {1..22, X, Y}; unknown -> None.

    Accepts "chrN" prefix and "23"/"24" aliases for X/Y.
    """
    cleaned = s.astype(str).str.strip().str.removeprefix("chr")
    mapped = cleaned.map(lambda v: CHROM_ALIASES.get(v, v))
    result = [v if v in VALID_CHROMS else None for v in mapped]
    return pd.Series(result, index=s.index, dtype=object)


def filter_valid_pvalues(df: pd.DataFrame, pcol: str) -> pd.DataFrame:
    """Keep rows where 0 < p <= 1; coerce pcol to float."""
    p = pd.to_numeric(df[pcol], errors="coerce")
    return df.assign(**{pcol: p}).loc[(p > 0) & (p <= 1)]


def filter_by_eaf(df: pd.DataFrame, eaf_col: str, lo: float = 0.01, hi: float = 0.99) -> pd.DataFrame:
    """Keep common variants: rows where lo <= effect AF <= hi (default MAF >= 1%).

    Coerces eaf_col to numeric; blank/non-numeric values become NaN and are
    dropped (NaN comparisons are False). EAF and 1-EAF describe the same
    variant, so the symmetric [lo, hi] window is exactly MAF >= lo.
    """
    eaf = pd.to_numeric(df[eaf_col], errors="coerce")
    return df.loc[(eaf >= lo) & (eaf <= hi)]


def count_significant(pvalues: pd.Series, threshold: float) -> int:
    return int((pvalues <= threshold).sum())
