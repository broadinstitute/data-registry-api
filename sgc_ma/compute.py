"""Fixed-effects IVW meta-analysis via streaming sufficient statistics.
Cochran's Q = Σ w_i (β_i - β)^2 = Σ w_i β_i^2 - (Σ w_i β_i)^2 / Σ w_i, so only
running sums are needed — never the per-cohort betas."""
import math

import numpy as np
import pandas as pd
from scipy.stats import norm

_SUMS = ["w", "wb", "wb2", "n", "pos_sign", "k"]


def cohort_terms(df: pd.DataFrame) -> pd.DataFrame:
    w = 1.0 / (df["se"].astype(float) ** 2)
    b = df["beta"].astype(float)
    return df.assign(w=w, wb=w * b, wb2=w * b * b,
                     pos_sign=(b > 0).astype(int), k=1)


def accumulate(acc, cohort: pd.DataFrame) -> pd.DataFrame:
    keys = ["chromosome", "position", "ref", "alt"]
    part = cohort.groupby(keys, as_index=False)[_SUMS].sum()
    if acc is None:
        return part
    merged = pd.concat([acc, part], ignore_index=True)
    return merged.groupby(keys, as_index=False)[_SUMS].sum()


def finalize(acc: pd.DataFrame, min_cohorts: int = 2) -> pd.DataFrame:
    df = acc[acc["k"] >= min_cohorts].copy()
    if df.empty:
        return df.assign(beta=[], se=[], pvalue=[], n=[], n_cohorts=[],
                         dir_concordance=[], i2=[])[
            ["chromosome", "position", "ref", "alt", "beta", "se", "pvalue",
             "n", "n_cohorts", "dir_concordance", "i2"]]
    beta = df["wb"] / df["w"]
    se = 1.0 / np.sqrt(df["w"])
    p = 2.0 * norm.cdf(-np.abs(beta / se))
    p = np.maximum(p, np.nextafter(0, 1))
    q = df["wb2"] - (df["wb"] ** 2) / df["w"]
    dof = df["k"] - 1
    i2 = np.where((df["k"] >= 2) & (q > 0), np.maximum(0.0, (q - dof) / q), 0.0)
    conc = np.maximum(df["pos_sign"], df["k"] - df["pos_sign"]) / df["k"]
    return pd.DataFrame({
        "chromosome": df["chromosome"], "position": df["position"],
        "ref": df["ref"], "alt": df["alt"],
        "beta": beta, "se": se, "pvalue": p, "n": df["n"].astype(int),
        "n_cohorts": df["k"].astype(int), "dir_concordance": conc, "i2": i2,
    }).reset_index(drop=True)


def finalize_one(w, wb, wb2, n, pos_sign, k, min_cohorts=2):
    if k < min_cohorts:
        return None
    beta = wb / w
    se = 1.0 / math.sqrt(w)
    p = max(2.0 * norm.cdf(-abs(beta / se)), np.nextafter(0, 1))
    q = wb2 - (wb * wb) / w
    i2 = max(0.0, (q - (k - 1)) / q) if (k >= 2 and q > 0) else 0.0
    conc = max(pos_sign, k - pos_sign) / k
    return {"beta": beta, "se": se, "pvalue": p, "n": int(n),
            "n_cohorts": int(k), "dir_concordance": conc, "i2": i2}
