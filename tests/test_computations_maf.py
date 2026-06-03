import numpy as np
import pandas as pd

from sgc_qc_plots.computations import filter_by_eaf, lambda_gc


def test_filter_by_eaf_keeps_common_inclusive_bounds():
    df = pd.DataFrame({"eaf": [0.005, 0.01, 0.5, 0.99, 0.995],
                       "pvalue": [0.1, 0.2, 0.3, 0.4, 0.5]})
    out = filter_by_eaf(df, "eaf")
    assert list(out["eaf"]) == [0.01, 0.5, 0.99]


def test_filter_by_eaf_drops_blank_and_nonnumeric():
    df = pd.DataFrame({"eaf": ["", "abc", "0.2", 0.5],
                       "pvalue": [0.1, 0.2, 0.3, 0.4]})
    out = filter_by_eaf(df, "eaf")
    assert list(out["pvalue"]) == [0.3, 0.4]


def test_filter_by_eaf_empty_result_is_empty_not_error():
    df = pd.DataFrame({"eaf": [0.001, 0.999], "pvalue": [0.1, 0.2]})
    out = filter_by_eaf(df, "eaf")
    assert len(out) == 0


def test_lambda_drops_when_rare_variant_inflation_removed():
    # Mirrors the real observation: rare variants carry artificial small-p
    # inflation; restricting to common variants brings lambda back toward 1.
    rng = np.random.default_rng(1)
    common = pd.DataFrame({"eaf": rng.uniform(0.1, 0.9, 500),
                           "pvalue": rng.uniform(0, 1, 500)})
    rare = pd.DataFrame({"eaf": rng.uniform(0.0001, 0.005, 500),
                         "pvalue": rng.uniform(0, 1e-6, 500)})
    df = pd.concat([common, rare], ignore_index=True)
    lam_all = lambda_gc(df["pvalue"])
    lam_common = lambda_gc(filter_by_eaf(df, "eaf")["pvalue"])
    assert lam_common < lam_all
