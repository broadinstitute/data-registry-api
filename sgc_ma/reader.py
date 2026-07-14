"""Read a per-cohort GWAS file and normalize to MA input columns."""
from typing import Iterator

import numpy as np
import pandas as pd

from sgc_qc_plots.computations import normalize_chromosome


def _effective_n(cases, controls):
    if cases and controls:
        return 4.0 / (1.0 / cases + 1.0 / controls)
    return np.nan


def normalize_frame(raw: pd.DataFrame, column_mapping: dict, cases, controls) -> pd.DataFrame:
    m = column_mapping
    df = pd.DataFrame({
        "chromosome": normalize_chromosome(raw[m["col_chromosome"]]),
        "position": pd.to_numeric(raw[m["col_position"]], errors="coerce"),
        "ea": raw[m["col_effect_allele"]].astype(str).str.upper(),
        "oa": raw[m["col_non_effect_allele"]].astype(str).str.upper(),
        "beta": pd.to_numeric(raw[m["col_beta"]], errors="coerce"),
        "se": pd.to_numeric(raw[m["col_se"]], errors="coerce"),
        "eaf": pd.to_numeric(raw.get(m.get("col_effect_allele_freq")), errors="coerce")
               if m.get("col_effect_allele_freq") in raw else np.nan,
        "pvalue": pd.to_numeric(raw[m["col_pvalue"]], errors="coerce"),
    })
    n_col = m.get("col_variant_n")
    if n_col and n_col in raw:
        df["n"] = pd.to_numeric(raw[n_col], errors="coerce")
    else:
        df["n"] = _effective_n(cases, controls)
    df = df.dropna(subset=["chromosome", "position", "beta", "se", "pvalue"])
    df = df[np.isfinite(df["beta"]) & np.isfinite(df["se"]) & np.isfinite(df["position"])]
    df = df[(df["se"] > 0) & (df["pvalue"] > 0) & (df["pvalue"] <= 1)]
    df["position"] = df["position"].astype("int64")
    df["n"] = df["n"].fillna(0)
    return df.reset_index(drop=True)


REQUIRED_KEYS = ["col_chromosome", "col_position", "col_effect_allele",
                 "col_non_effect_allele", "col_beta", "col_se", "col_pvalue"]
OPTIONAL_KEYS = ["col_effect_allele_freq", "col_variant_n"]


def _validate_and_usecols(path: str, column_mapping: dict) -> list:
    """Shared validation for read_cohort/read_cohort_chunks: required mapping
    keys present, required columns present in the file, and the resulting
    usecols list (required + any optional columns that exist)."""
    missing_keys = [k for k in REQUIRED_KEYS if not column_mapping.get(k)]
    if missing_keys:
        raise ValueError(f"column_mapping missing required keys: {missing_keys}")
    available = set(pd.read_csv(path, sep="\t", nrows=0).columns)
    required_cols = [column_mapping[k] for k in REQUIRED_KEYS]
    missing_cols = [c for c in required_cols if c not in available]
    if missing_cols:
        raise ValueError(f"file missing required columns: {missing_cols}")
    usecols = list(required_cols)
    for k in OPTIONAL_KEYS:
        c = column_mapping.get(k)
        if c and c in available:
            usecols.append(c)
    return usecols


def read_cohort(path: str, column_mapping: dict, cases, controls) -> pd.DataFrame:
    usecols = _validate_and_usecols(path, column_mapping)
    raw = pd.read_csv(path, sep="\t", usecols=usecols, low_memory=False)
    return normalize_frame(raw, column_mapping, cases, controls)


def read_cohort_chunks(path: str, column_mapping: dict, cases, controls,
                        chunksize: int = 1_000_000) -> Iterator[pd.DataFrame]:
    """Same validation/usecols as read_cohort, but reads via chunked
    pd.read_csv and yields normalized chunks so a whole cohort file is never
    materialized in memory at once. Validation runs eagerly (before any
    chunk is read) so callers see the same errors read_cohort raises,
    regardless of whether the returned iterator is ever consumed."""
    usecols = _validate_and_usecols(path, column_mapping)

    def _chunks():
        for chunk in pd.read_csv(path, sep="\t", usecols=usecols, low_memory=False,
                                  chunksize=chunksize):
            yield normalize_frame(chunk, column_mapping, cases, controls)

    return _chunks()
