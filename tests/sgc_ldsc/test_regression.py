import numpy as np
from sgc_ldsc.regression import get_x, get_separators, univariate_ldsc


def test_get_separators_partitions_evenly():
    assert get_separators(10, 2) == [0, 5, 10]
    assert get_separators(3, 200) == [0, 1, 2, 3]  # max_blocks capped at snps


def test_get_x_conditions_on_mean_n():
    ld = np.array([[2.0], [4.0]])
    n = np.array([[100.0], [300.0]])
    weights = np.array([[0.5], [0.5]])
    # conditioned = n*ld/mean(n); mean(n)=200 -> [[1.0],[6.0]]; *weights -> [[0.5],[3.0]]
    np.testing.assert_allclose(get_x(ld, weights, n), [[0.5], [3.0]])


def test_univariate_ldsc_recovers_planted_intercept_and_slope():
    # Plant chi^2_j = intercept + N * perSNP_h2 * ld_j exactly, equal weights/N,
    # so the weighted LS solve returns the planted coefficients.
    rng = np.random.default_rng(0)
    M = 5000
    ld = rng.uniform(1.0, 50.0, size=(M, 1))
    n = np.full((M, 1), 100000.0)
    true_intercept, true_per_snp_h2 = 1.05, 3e-7
    chisq = true_intercept + n * true_per_snp_h2 * ld
    w_ld = np.ones((M, 1))
    m_snps = np.array([[1_000_000.0]])

    res = univariate_ldsc(chisq=chisq, ld=ld, w_ld=w_ld, sample_size=n,
                          m_snps=m_snps, max_blocks=200)

    assert abs(res["intercept"] - true_intercept) < 1e-3
    # h2 = per_snp_h2 * total_snps
    assert abs(res["h2"] - true_per_snp_h2 * 1_000_000.0) < 1e-3 * (true_per_snp_h2 * 1_000_000.0) + 1e-6
    assert res["n_snps"] == M
    assert abs(res["effective_n"] - 100000.0) < 1e-6
