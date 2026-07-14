import numpy as np
import pandas as pd
from sgc_ma.reader import normalize_frame, read_cohort, read_cohort_chunks

CM = {"col_chromosome": "CHR", "col_position": "POS", "col_effect_allele": "EA",
      "col_non_effect_allele": "OA", "col_beta": "BETA", "col_se": "SE",
      "col_pvalue": "P", "col_effect_allele_freq": "EAF", "col_variant_n": "N"}

def test_normalize_frame_maps_and_filters():
    raw = pd.DataFrame({
        "CHR": ["1", "chr2", "23", "99"], "POS": [10, 20, 30, 40],
        "EA": ["a", "C", "G", "T"], "OA": ["g", "T", "A", "C"],
        "BETA": [0.1, 0.2, 0.3, 0.4], "SE": [0.1, 0.0, 0.1, 0.1],
        "P": [0.5, 0.5, 0.5, 0.5], "EAF": [0.2, 0.3, 0.4, 0.5], "N": [10, 20, 30, 40],
    })
    out = normalize_frame(raw, CM, cases=None, controls=None)
    # chr 99 dropped (invalid), SE=0 row dropped, chr2->2, 23->X, alleles upper
    assert list(out["chromosome"]) == ["1", "X"]
    assert list(out["ea"]) == ["A", "G"]

def test_normalize_frame_effective_n_fallback():
    raw = pd.DataFrame({"CHR": ["1"], "POS": [10], "EA": ["A"], "OA": ["G"],
                        "BETA": [0.1], "SE": [0.1], "P": [0.5], "EAF": [0.2]})
    cm = {k: v for k, v in CM.items() if k != "col_variant_n"}
    out = normalize_frame(raw, cm, cases=100, controls=300)
    assert abs(out["n"].iloc[0] - 4 / (1/100 + 1/300)) < 1e-6

def test_normalize_frame_drops_non_finite():
    raw = pd.DataFrame({
        "CHR": ["1", "1", "1", "1"], "POS": [10, 20, 30, float("inf")],
        "EA": ["A", "A", "A", "A"], "OA": ["G", "G", "G", "G"],
        "BETA": [0.1, float("inf"), 0.2, 0.3],
        "SE": [0.1, 0.1, float("inf"), 0.1],
        "P": [0.5, 0.5, 0.5, 0.5], "EAF": [0.2, 0.2, 0.2, 0.2], "N": [10, 20, 30, 40],
    })
    out = normalize_frame(raw, CM, cases=None, controls=None)
    assert list(out["position"]) == [10]   # inf beta, inf se, inf position rows all dropped, no crash

def test_read_cohort_reads_gzip_tsv(tmp_path):
    import gzip
    p = tmp_path / "cohort.tsv.gz"
    with gzip.open(p, "wt") as fh:
        fh.write("CHR\tPOS\tEA\tOA\tBETA\tSE\tP\tEAF\tN\n")
        fh.write("1\t100\ta\tG\t0.1\t0.1\t0.5\t0.2\t1000\n")
    out = read_cohort(str(p), CM, cases=None, controls=None)
    assert len(out) == 1
    assert out.iloc[0]["chromosome"] == "1" and out.iloc[0]["ea"] == "A"

def test_read_cohort_raises_on_missing_required_mapping_key(tmp_path):
    import gzip, pytest
    p = tmp_path / "c.tsv.gz"
    with gzip.open(p, "wt") as fh:
        fh.write("CHR\tPOS\tEA\tOA\tSE\tP\n1\t100\tA\tG\t0.1\t0.5\n")
    bad = {k: v for k, v in CM.items() if k != "col_beta"}  # drop beta mapping
    with pytest.raises(ValueError):
        read_cohort(str(p), bad, cases=None, controls=None)

def test_read_cohort_chunks_matches_read_cohort(tmp_path):
    import gzip
    p = tmp_path / "cohort.tsv.gz"
    with gzip.open(p, "wt") as fh:
        fh.write("CHR\tPOS\tEA\tOA\tBETA\tSE\tP\tEAF\tN\n")
        fh.write("1\t100\ta\tG\t0.1\t0.1\t0.5\t0.2\t1000\n")
        fh.write("1\t200\tC\tT\t0.2\t0.1\t0.4\t0.3\t2000\n")
        fh.write("2\t300\tA\tG\t0.3\t0.1\t0.3\t0.4\t3000\n")
    # chunksize smaller than the row count forces multiple chunks
    chunks = list(read_cohort_chunks(str(p), CM, cases=None, controls=None, chunksize=1))
    assert len(chunks) == 3
    combined = pd.concat(chunks, ignore_index=True)
    expected = read_cohort(str(p), CM, cases=None, controls=None)
    pd.testing.assert_frame_equal(combined, expected)

def test_read_cohort_chunks_raises_eagerly_on_missing_required_mapping_key(tmp_path):
    import gzip, pytest
    p = tmp_path / "c.tsv.gz"
    with gzip.open(p, "wt") as fh:
        fh.write("CHR\tPOS\tEA\tOA\tSE\tP\n1\t100\tA\tG\t0.1\t0.5\n")
    bad = {k: v for k, v in CM.items() if k != "col_beta"}  # drop beta mapping
    with pytest.raises(ValueError):
        # validation must happen before iteration begins, like read_cohort
        read_cohort_chunks(str(p), bad, cases=None, controls=None)
