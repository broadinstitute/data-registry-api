"""Univariate LD Score Regression. Primitives ported verbatim from
dig-ldsc-methods/src/ldsc/sldsc/xtx_xty.py; the univariate_ldsc() driver is the
strict single-annotation simplification of sldsc.sldsc() that KEEPS the
intercept coefficient (the stratified code discards it via [:-1]).
"""
from typing import List

import numpy as np

MAX_BLOCKS = 200


def get_weight(g1000_ld, l_hm3, sample_size, chisq, parameter_snps):
    g1000_ld_sum = np.sum(g1000_ld, axis=1, keepdims=True)
    tau = (np.mean(chisq) - 1) / np.mean(np.multiply(g1000_ld_sum, sample_size))
    safe_g1000_ld_sum = np.fmax(g1000_ld_sum, 1.0)
    safe_tau = min(max(tau, 0.0), 1.0 / np.sum(parameter_snps))
    safe_l_hm3 = np.fmax(l_hm3, 1.0)
    heteroskedasticity_weight = 1.0 / (1 + safe_tau * sample_size * safe_g1000_ld_sum) ** 2
    over_counting_weight = 1.0 / safe_l_hm3
    unnormalized_weight = np.sqrt(heteroskedasticity_weight * over_counting_weight)
    return unnormalized_weight / np.sum(unnormalized_weight)


def get_x(ld_matrix, weights, n):
    conditioned_ld_matrix = np.multiply(n, ld_matrix) / np.mean(n)
    return np.multiply(conditioned_ld_matrix, weights)


def get_intercept(size, weights):
    return np.multiply(np.ones((size, 1)), weights)


def get_y(chisq, weights):
    return np.multiply(chisq, weights)


def get_separators(snps: int, max_blocks: int) -> List[int]:
    return list(map(int, np.floor(np.linspace(0, snps, min(max_blocks, snps) + 1))))


def _general_xtx(x1, x2, separators):
    blocks = len(separators) - 1
    out = np.zeros((blocks, x1.shape[1], x2.shape[1]))
    for i in range(blocks):
        a, b = separators[i], separators[i + 1]
        out[i, :, :] = x1[a:b, :].T.dot(x2[a:b, :])
    return out


def univariate_ldsc(*, chisq, ld, w_ld, sample_size, m_snps, max_blocks=MAX_BLOCKS) -> dict:
    """chisq,(M,1)  ld,(M,1) base LD score  w_ld,(M,1) regression-weight LD score
    sample_size,(M,1)  m_snps,(1,1) total #SNPs in the base annotation."""
    weights = get_weight(ld, w_ld, sample_size, chisq, m_snps)
    x = np.hstack((get_x(ld, weights, sample_size), get_intercept(ld.shape[0], weights)))  # (M,2)
    y = get_y(chisq, weights)  # (M,1)
    seps = get_separators(ld.shape[0], max_blocks)
    xtx = _general_xtx(x, x, seps)
    xty = _general_xtx(x, y, seps)
    coef = np.linalg.solve(np.sum(xtx, axis=0), np.sum(xty, axis=0))  # (2,1): [slope, intercept]
    mean_n = float(np.mean(sample_size))
    total_m = float(m_snps[0, 0])
    intercept = float(coef[1, 0])
    h2 = (float(coef[0, 0]) / mean_n) * total_m
    mean_chisq = float(np.mean(chisq))
    # Ratio is only defined for inflated statistics (mean chi^2 > 1). For null or
    # deflated GWAS (mean chi^2 <= 1) it is meaningless -> None (and must not be
    # NaN, which a MySQL DOUBLE column rejects).
    ratio = (intercept - 1.0) / (mean_chisq - 1.0) if mean_chisq > 1.0 else None
    return {"intercept": intercept, "h2": h2, "ratio": ratio,
            "mean_chisq": mean_chisq, "effective_n": mean_n, "n_snps": int(ld.shape[0])}
