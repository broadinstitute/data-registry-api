import numpy as np

from sgc_ldsc.compute import align_and_filter, run_univariate


def test_align_and_filter_keeps_order_and_drops_outliers():
    ld_order = ["rsA", "rsB", "rsC", "rsD"]
    data = {"rsA": (1.0, 100.0), "rsC": (90.0, 100.0), "rsD": (2.0, 100.0)}  # rsC chi2=8100 outlier
    chisq, n, idxs = align_and_filter(data, ld_order)
    # rsB missing; rsC dropped (chisq 8100 > max(0.001*100, 80)=80); keep rsA(idx0), rsD(idx3)
    assert idxs.tolist() == [0, 3]
    np.testing.assert_allclose(chisq[:, 0], [1.0, 4.0])


def test_run_univariate_on_synthetic_reference():
    rng = np.random.default_rng(1)
    M = 4000
    full_ld = rng.uniform(1, 40, size=(M, 1))
    w_ld = np.ones((M, 1))
    m_snps = np.array([[1_000_000.0]])
    n_val = 80000.0
    true_intercept, true_per_snp = 1.08, 4e-7
    # build a sumstats dict over the first 3000 SNPs in order
    order = [f"rs{i}" for i in range(M)]
    data = {}
    for i in range(3000):
        chi = true_intercept + n_val * true_per_snp * full_ld[i, 0]
        z = np.sqrt(chi)
        data[f"rs{i}"] = (z, n_val)
    res = run_univariate(data=data, ld_rs=order, baseline_ld=full_ld,
                         input_weights=w_ld, m_snps=m_snps)
    assert abs(res["intercept"] - true_intercept) < 5e-3
    assert res["n_snps"] == 3000
