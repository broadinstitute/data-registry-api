import pandas as pd
from sgc_ma.stream import extract_sorted, merge_and_combine

def _chunk(rows):  # rows: list of dicts with the normalized columns
    return [pd.DataFrame(rows)]

def test_extract_sorted_writes_sorted_keys(tmp_path):
    p = tmp_path / "a.tsv"
    rows = [dict(chromosome="2", position=5, ea="C", oa="A", beta=0.1, se=0.1, eaf=0.3, pvalue=0.5, n=100),
            dict(chromosome="1", position=9, ea="A", oa="G", beta=0.2, se=0.1, eaf=0.3, pvalue=0.5, n=100)]
    stats = extract_sorted(_chunk(rows), str(p))
    keys = [ln.split("\t")[0] for ln in p.read_text().splitlines()]
    assert stats["n_kept"] == 2 and keys == sorted(keys)

def test_merge_two_cohorts_ivw(tmp_path):
    a, b = tmp_path / "a.tsv", tmp_path / "b.tsv"
    extract_sorted(_chunk([dict(chromosome="1", position=100, ea="A", oa="G", beta=0.2, se=0.1, eaf=0.3, pvalue=0.01, n=1000)]), str(a))
    extract_sorted(_chunk([dict(chromosome="1", position=100, ea="G", oa="A", beta=-0.2, se=0.1, eaf=0.7, pvalue=0.01, n=1000)]), str(b))
    out = list(merge_and_combine([str(a), str(b)]))
    assert len(out) == 1
    assert out[0]["n_cohorts"] == 2 and abs(out[0]["beta"] - (-0.2)) < 1e-9  # canonical effect=G
    assert out[0]["chromosome"] == "1" and out[0]["ref"] == "A" and out[0]["alt"] == "G"

def test_merge_requires_two_cohorts(tmp_path):
    a = tmp_path / "a.tsv"
    extract_sorted(_chunk([dict(chromosome="1", position=1, ea="A", oa="C", beta=0.2, se=0.1, eaf=0.3, pvalue=0.5, n=10)]), str(a))
    assert list(merge_and_combine([str(a)])) == []

def test_extract_sorted_python_order_and_no_loss(tmp_path):
    import random
    rng = random.Random(0)
    chroms = [str(i) for i in range(1, 23)] + ["X", "Y"]
    alleles = ["A", "C", "G", "T"]
    rows, seen = [], set()
    while len(rows) < 500:
        ch = rng.choice(chroms); pos = rng.randint(1, 250_000_000)
        a, b = rng.sample(alleles, 2)
        if {a, b} in ({"A", "T"}, {"C", "G"}):   # skip palindromic (canonicalize drops them)
            continue
        k = (ch, pos, a, b)
        if k in seen:
            continue
        seen.add(k)
        rows.append(dict(chromosome=ch, position=pos, ea=a, oa=b,
                         beta=0.1, se=0.1, eaf=0.3, pvalue=0.5, n=100))
    fa, fb = tmp_path / "a.tsv", tmp_path / "b.tsv"
    extract_sorted([pd.DataFrame(rows)], str(fa))
    extract_sorted([pd.DataFrame(rows)], str(fb))
    keys = [ln.split("\t")[0] for ln in fa.read_text().splitlines()]
    assert keys == sorted(keys)                 # file order matches Python string order
    out = list(merge_and_combine([str(fa), str(fb)]))
    assert len(out) == len(rows)                # every shared variant emitted, none dropped
    assert all(r["n_cohorts"] == 2 for r in out)

def test_extract_sorted_dedups_within_cohort(tmp_path):
    a, b = tmp_path / "a.tsv", tmp_path / "b.tsv"
    dup = dict(chromosome="1", position=100, ea="A", oa="G", beta=0.2, se=0.1, eaf=0.3, pvalue=0.01, n=1000)
    sa = extract_sorted([pd.DataFrame([dup, dup])], str(a))       # same variant twice in cohort A
    extract_sorted([pd.DataFrame([dict(dup, ea="G", oa="A", beta=-0.2)])], str(b))  # cohort B once
    assert sa["n_in"] == 2 and sa["n_kept"] == 1                  # dedup collapsed the duplicate
    out = list(merge_and_combine([str(a), str(b)]))
    assert len(out) == 1 and out[0]["n_cohorts"] == 2            # NOT 3 (no double-count)
