"""Assemble univariate LDSC from munged sumstats + reference inputs.

Walks the reference SNP order, collects chi^2 / N / reference-row index for SNPs
present in the data, drops chi^2 outliers, then runs the univariate regression.
"""
import numpy as np

from sgc_ldsc.regression import univariate_ldsc


def align_and_filter(data, ld_rs):
    """data: {rs_id: (Z, N)}; ld_rs: reference rs order. Returns (chisq, n, idxs)
    aligned to the reference rows, with chi^2 outliers removed
    (LDSC default: chisq < max(0.001*max(N), 80))."""
    chisq, n, idxs = [], [], []
    for i, rs in enumerate(ld_rs):
        if rs in data:
            z, nn = data[rs]
            chisq.append(z * z)
            n.append(nn)
            idxs.append(i)
    chisq = np.array([chisq]).T
    n = np.array([n]).T
    idxs = np.array(idxs)
    if len(idxs) == 0:
        return chisq.reshape(0, 1), n.reshape(0, 1), idxs
    keep = chisq[:, 0] < max(0.001 * float(np.max(n)), 80)
    return chisq[keep, :], n[keep, :], idxs[keep]


def run_univariate(*, data, ld_rs, baseline_ld, input_weights, m_snps) -> dict:
    # ld_rs (live-read from the weights files) and baseline_ld/input_weights
    # (prebuilt npy) must be the SAME panel ordering, row for row. If a panel
    # version ever skews these, a shorter weights set would silently misalign and
    # produce wrong numbers — so fail loudly instead.
    if not (len(ld_rs) == baseline_ld.shape[0] == input_weights.shape[0]):
        raise ValueError(
            f"reference panel length mismatch: ld_rs={len(ld_rs)} "
            f"baseline_ld={baseline_ld.shape[0]} input_weights={input_weights.shape[0]}")
    chisq, n, idxs = align_and_filter(data, ld_rs)
    if chisq.shape[0] < 200:
        raise ValueError(f"too few SNPs after filtering for stable LDSC: {chisq.shape[0]}")
    return univariate_ldsc(
        chisq=chisq, ld=baseline_ld[idxs, :], w_ld=input_weights[idxs, :],
        sample_size=n, m_snps=m_snps,
    )
