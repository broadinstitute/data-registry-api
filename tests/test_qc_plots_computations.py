import numpy as np
import pandas as pd

from sgc_qc_plots.computations import (
    lambda_gc, normalize_chromosome, filter_valid_pvalues, count_significant,
)


def test_lambda_gc_uniform_is_one():
    rng = np.random.default_rng(42)
    p = pd.Series(rng.uniform(1e-10, 1.0, size=100_000))
    assert abs(lambda_gc(p) - 1.0) < 0.05


def test_lambda_gc_inflated():
    rng = np.random.default_rng(42)
    p = pd.Series(rng.uniform(1e-10, 1.0, size=100_000)) ** 2
    assert lambda_gc(p) > 1.5


def test_normalize_chromosome_basic():
    s = pd.Series(["1", "2", "X", "Y", "23", "24", "chr5", "MT", "foo"])
    out = normalize_chromosome(s)
    # 23 -> X, 24 -> Y, chr5 -> 5, MT/foo dropped (None)
    assert list(out) == ["1", "2", "X", "Y", "X", "Y", "5", None, None]


def test_filter_valid_pvalues():
    df = pd.DataFrame({"p": [0.5, 0.0, 1.0, 1.1, -0.1, float("nan")]})
    out = filter_valid_pvalues(df, "p")
    # keep 0 < p <= 1
    assert list(out["p"]) == [0.5, 1.0]


def test_count_significant():
    p = pd.Series([1e-9, 5e-9, 1e-7, 1e-6, 0.5])
    assert count_significant(p, 5e-8) == 2
    assert count_significant(p, 1e-5) == 4


def test_normalize_chromosome_preserves_index():
    s = pd.Series(["1", "X", "MT", "chr5"], index=[10, 20, 30, 40])
    out = normalize_chromosome(s)
    assert list(out.index) == [10, 20, 30, 40]
    assert out.loc[10] == "1"
    assert out.loc[20] == "X"
    assert out.loc[30] is None
    assert out.loc[40] == "5"
