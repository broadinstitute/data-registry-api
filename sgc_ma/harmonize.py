"""Harmonize per-cohort GWAS to a shared per-position reference orientation."""
from typing import Optional

import pandas as pd

_COMP = {"A": "T", "T": "A", "C": "G", "G": "C"}


def _comp(a: str) -> Optional[str]:
    return _COMP.get(a)


def is_ambiguous(a1: str, a2: str) -> bool:
    """Palindromic SNP: the two alleles are Watson-Crick complements (A/T, C/G)."""
    return _COMP.get(a1) == a2


def orient(ea, oa, beta, eaf, ref, alt):
    """Return (beta, eaf) with effect allele == alt, matching ref/alt; else None.
    Handles allele swap and strand flip. beta sign flips on swap, not on pure strand."""
    ea, oa = ea.upper(), oa.upper()
    if {ea, oa} == {ref, alt}:
        return (beta, eaf) if ea == alt else (-beta, 1.0 - eaf)
    cea, coa = _comp(ea), _comp(oa)
    if cea and coa and {cea, coa} == {ref, alt}:
        return (beta, eaf) if cea == alt else (-beta, 1.0 - eaf)
    return None


def canonicalize(chrom, pos, ea, oa, beta, eaf):
    """Reference-free, order/strand-independent harmonization. Returns
    (key, chrom, pos, refA, refB, beta, eaf) with effect allele == refB, or
    None for palindromic SNPs. Deterministic so independent cohorts converge."""
    pos = int(pos)
    ea, oa = ea.upper(), oa.upper()
    is_snp = ea in _COMP and oa in _COMP
    if is_snp and is_ambiguous(ea, oa):
        return None  # palindromic A/T or C/G — strand unresolvable by alleles
    if is_snp:
        fwd = tuple(sorted((ea, oa)))
        rev = tuple(sorted((_COMP[ea], _COMP[oa])))
        if rev < fwd:                      # pick the strand with the smaller pair
            ea, oa = _COMP[ea], _COMP[oa]
    refA, refB = sorted((ea, oa))
    if ea == refB:
        b, f = beta, eaf
    else:
        b = -beta
        f = (1.0 - eaf) if eaf == eaf else eaf   # NaN-safe
    return f"{chrom}:{pos}:{refA}:{refB}", chrom, pos, refA, refB, b, f


def harmonize_cohort(df: pd.DataFrame, ref_map: dict) -> pd.DataFrame:
    """Orient one cohort's rows to ref_map (mutated in place). Drops ambiguous
    SNPs, within-cohort duplicate positions, and rows that don't match the
    reference alleles. Adds 'varId'; overwrites 'beta'/'eaf' with oriented values."""
    df = df.copy()
    df["ea"] = df["ea"].str.upper()
    df["oa"] = df["oa"].str.upper()
    # drop palindromic and within-cohort duplicate positions
    df = df[~pd.Series([is_ambiguous(a, b) for a, b in zip(df["ea"], df["oa"])], index=df.index)]
    poskey = df["chromosome"].astype(str) + ":" + df["position"].astype(str)
    df = df[~poskey.duplicated(keep=False)]
    df = df.assign(_pos=poskey.loc[df.index])

    var_ids, betas, eafs, keep = [], [], [], []
    for _pos, ea, oa, beta, eaf in zip(df["_pos"], df["ea"], df["oa"], df["beta"], df["eaf"]):
        if _pos not in ref_map:
            ref_map[_pos] = (oa, ea)   # defining cohort: effect toward its EA
        ref, alt = ref_map[_pos]
        res = orient(ea, oa, beta, eaf, ref, alt)
        if res is None:
            keep.append(False); var_ids.append(None); betas.append(None); eafs.append(None)
            continue
        keep.append(True)
        var_ids.append(f"{_pos}:{ref}:{alt}")
        betas.append(res[0]); eafs.append(res[1])
    df = df.assign(varId=var_ids, beta=betas, eaf=eafs)
    df = df[keep]
    if not df.empty:
        df = df.drop(columns=["_pos", "ea", "oa"])
    else:
        df = df.drop(columns=["_pos", "ea", "oa"], errors="ignore")
    return df.reset_index(drop=True)
