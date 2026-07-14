import pandas as pd
from sgc_ma.harmonize import is_ambiguous, orient, harmonize_cohort, canonicalize

def test_is_ambiguous():
    assert is_ambiguous("A", "T") and is_ambiguous("C", "G")
    assert not is_ambiguous("A", "G") and not is_ambiguous("A", "C")

def test_orient_keep():
    assert orient("G", "A", 0.2, 0.3, ref="A", alt="G") == (0.2, 0.3)

def test_orient_swap():
    b, f = orient("A", "G", 0.2, 0.3, ref="A", alt="G")
    assert b == -0.2 and abs(f - 0.7) < 1e-12

def test_orient_strand():
    # cohort on opposite strand: C/T with ref=A alt=G  (comp: G/A)
    assert orient("C", "T", 0.2, 0.3, ref="A", alt="G") == (0.2, 0.3)

def test_orient_mismatch_returns_none():
    assert orient("C", "T", 0.2, 0.3, ref="A", alt="C") is None

def test_harmonize_cohort_sets_reference_and_orients():
    ref_map = {}
    a = pd.DataFrame([dict(chromosome="1", position=100, ea="G", oa="A",
                           beta=0.2, se=0.1, eaf=0.3, pvalue=0.01, n=1000)])
    out_a = harmonize_cohort(a, ref_map)
    assert ref_map["1:100"] == ("A", "G")           # first cohort defines ref/alt
    assert out_a.loc[0, "varId"] == "1:100:A:G" and out_a.loc[0, "beta"] == 0.2
    # second cohort, alleles swapped -> beta flips
    b = pd.DataFrame([dict(chromosome="1", position=100, ea="A", oa="G",
                           beta=0.5, se=0.1, eaf=0.4, pvalue=0.02, n=500)])
    out_b = harmonize_cohort(b, ref_map)
    assert out_b.loc[0, "beta"] == -0.5 and abs(out_b.loc[0, "eaf"] - 0.6) < 1e-12

def test_harmonize_drops_ambiguous_and_dupes():
    ref_map = {}
    df = pd.DataFrame([
        dict(chromosome="1", position=5, ea="A", oa="T", beta=0.1, se=0.1, eaf=0.5, pvalue=0.5, n=10),  # ambiguous
        dict(chromosome="2", position=9, ea="C", oa="G", beta=0.1, se=0.1, eaf=0.5, pvalue=0.5, n=10),  # ambiguous
        dict(chromosome="3", position=7, ea="A", oa="C", beta=0.1, se=0.1, eaf=0.5, pvalue=0.5, n=10),
        dict(chromosome="3", position=7, ea="A", oa="G", beta=0.1, se=0.1, eaf=0.5, pvalue=0.5, n=10),  # dup pos
    ])
    out = harmonize_cohort(df, ref_map)
    assert out.empty

def test_canonicalize_swap_and_strand_converge():
    # same variant reported 3 ways -> identical key + oriented beta
    a = canonicalize("1", 100, "A", "G", 0.2, 0.3)   # effect A
    b = canonicalize("1", 100, "G", "A", -0.2, 0.7)  # effect G (swap)
    c = canonicalize("1", 100, "T", "C", 0.2, 0.3)   # reverse strand, effect T(=A)
    assert a[0] == b[0] == c[0] == "1:100:A:G"
    assert a[1:6] == ("1", 100, "A", "G") + (a[5],)  # shape
    assert abs(a[5] - b[5]) < 1e-12 and abs(a[5] - c[5]) < 1e-12  # same oriented beta
    assert a[5] == -0.2  # effect toward refB=G
    assert a[6] == b[6] == c[6]   # eaf converges too

def test_canonicalize_drops_palindromic():
    assert canonicalize("1", 5, "A", "T", 0.1, 0.5) is None
    assert canonicalize("1", 6, "C", "G", 0.1, 0.5) is None

def test_canonicalize_indel_no_strand():
    k = canonicalize("2", 9, "AT", "A", 0.3, 0.4)
    assert k[0] == "2:9:A:AT" and k[3] == "A" and k[4] == "AT"
    assert k[5] == 0.3  # effect allele AT == refB, kept

def test_canonicalize_nan_eaf_stays_nan():
    import math
    k = canonicalize("1", 100, "A", "G", 0.2, float("nan"))  # canonical effect=G -> swap
    assert k[5] == -0.2 and math.isnan(k[6])

def test_canonicalize_reverse_strand_swap_converges():
    a = canonicalize("1", 100, "A", "G", 0.2, 0.3)
    d = canonicalize("1", 100, "C", "T", -0.2, 0.7)  # C=comp(G), T=comp(A); effect C
    assert a[0] == d[0] == "1:100:A:G"
    assert abs(a[5] - d[5]) < 1e-12 and a[5] == -0.2
