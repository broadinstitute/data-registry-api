import math
import pandas as pd
from sgc_ma.compute import cohort_terms, accumulate, finalize, finalize_one

def _cohort(var, beta, se, n):
    ch, pos, ref, alt = var.split(":")
    return pd.DataFrame([dict(varId=var, chromosome=ch, position=int(pos),
                              ref=ref, alt=alt, beta=beta, se=se, n=n)])

def test_ivw_two_equal_cohorts():
    acc = None
    for beta in (0.2, 0.4):
        acc = accumulate(acc, cohort_terms(_cohort("1:100:A:G", beta, 0.1, 1000)))
    out = finalize(acc)
    row = out.iloc[0]
    # equal SE -> mean beta; SE = 0.1/sqrt(2)
    assert abs(row.beta - 0.3) < 1e-9
    assert abs(row.se - 0.1/math.sqrt(2)) < 1e-9
    assert row.n == 2000 and row.n_cohorts == 2
    assert abs(row.dir_concordance - 1.0) < 1e-9

def test_min_cohorts_filters_singletons():
    acc = accumulate(None, cohort_terms(_cohort("2:5:A:C", 0.5, 0.2, 100)))
    assert finalize(acc, min_cohorts=2).empty

def test_heterogeneity_zero_when_identical():
    acc = None
    for _ in range(3):
        acc = accumulate(acc, cohort_terms(_cohort("1:1:A:G", 0.3, 0.1, 100)))
    assert abs(finalize(acc).iloc[0].i2) < 1e-9

def test_pvalue_matches_normal():
    from scipy.stats import norm
    acc = None
    for beta in (0.3, 0.3):
        acc = accumulate(acc, cohort_terms(_cohort("1:2:A:G", beta, 0.1, 100)))
    row = finalize(acc).iloc[0]
    assert abs(row.pvalue - 2 * norm.cdf(-abs(row.beta / row.se))) < 1e-12

def test_i2_zero_for_single_cohort_when_min_cohorts_one():
    acc = accumulate(None, cohort_terms(_cohort("1:100:A:G", 0.3, 0.1, 1000)))
    out = finalize(acc, min_cohorts=1)
    assert len(out) == 1
    assert out.iloc[0].i2 == 0.0
    assert out.iloc[0].n_cohorts == 1

def test_finalize_one_matches_dataframe_finalize_two_equal_cohorts():
    acc = None
    for beta in (0.2, 0.4):
        acc = accumulate(acc, cohort_terms(_cohort("1:100:A:G", beta, 0.1, 1000)))
    row = finalize(acc).iloc[0]
    w, wb, wb2, n, pos_sign, k = 200.0, 60.0, 20.0, 2000, 2, 2
    result = finalize_one(w, wb, wb2, n, pos_sign, k)
    assert abs(result["beta"] - row.beta) < 1e-9
    assert abs(result["se"] - row.se) < 1e-9
    assert abs(result["pvalue"] - row.pvalue) < 1e-12
    assert result["n"] == row.n
    assert result["n_cohorts"] == row.n_cohorts
    assert abs(result["dir_concordance"] - row.dir_concordance) < 1e-9
    assert abs(result["i2"] - row.i2) < 1e-9

def test_finalize_one_returns_none_below_min_cohorts():
    assert finalize_one(w=100.0, wb=20.0, wb2=4.0, n=100, pos_sign=1, k=1) is None
