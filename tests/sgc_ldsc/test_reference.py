import gzip
import io
import zipfile

import numpy as np

from sgc_ldsc.reference import (
    load_baseline_ld_col0, load_input_weights, load_baseline_m, load_snpmap, BUILD_TOKEN,
)


def test_build_token_maps_grch38():
    assert BUILD_TOKEN["GRCh38"] == "GRCh38"


def test_load_input_weights_reads_col_index_3(tmp_path):
    d = tmp_path / "weights" / "EUR"
    d.mkdir(parents=True)
    for chrom in range(1, 23):
        with gzip.open(d / f"weights.{chrom}.l2.ldscore.gz", "wt") as f:
            f.write("CHR\tSNP\tBP\tL2\n")
            f.write(f"{chrom}\trs{chrom}\t1\t{float(chrom)}\n")
    w = load_input_weights(str(tmp_path), "EUR")
    assert w.shape == (22, 1)
    assert w[0, 0] == 1.0 and w[21, 0] == 22.0


def test_load_snpmap_splits_var_and_rs(tmp_path):
    p = tmp_path / "snpmap"
    p.mkdir()
    f = p / "sumstats.standard.GRCh38.EUR.snpmap"
    f.write_text("1:100:A:G\trs1\n2:200:C:T\trs2\n")
    m = load_snpmap(str(tmp_path), "EUR", "GRCh38", "standard")
    assert m == {"1:100:A:G": "rs1", "2:200:C:T": "rs2"}


def test_baseline_loaders_read_zip_members(tmp_path):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    # baseline_ld: 3 SNPs x 2 annotations; column 0 is the genome-wide LD score.
    ld = np.array([[10.0, 1.0], [20.0, 2.0], [30.0, 3.0]])
    base_snps = np.array([[1234.0], [99.0]])  # base annotation count is row 0
    with zipfile.ZipFile(inputs / "sldsc_inputs.EUR.zip", "w") as z:
        for name, arr in [("baseline/baseline_ld.EUR.npy", ld),
                          ("baseline/baseline_parameter_snps.EUR.npy", base_snps)]:
            buf = io.BytesIO()
            np.save(buf, arr)
            z.writestr(name, buf.getvalue())
    col0 = load_baseline_ld_col0(str(tmp_path), "EUR")
    assert col0.shape == (3, 1)
    np.testing.assert_allclose(col0[:, 0], [10.0, 20.0, 30.0])
    base_m = load_baseline_m(str(tmp_path), "EUR")
    assert base_m.shape == (1, 1) and base_m[0, 0] == 1234.0
