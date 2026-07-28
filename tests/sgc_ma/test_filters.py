from sgc_ma.filters import maf_ok, info_ok


def test_maf_ok_common_variant_passes():
    assert maf_ok(0.006) is True
    assert maf_ok(0.30) is True


def test_maf_ok_rare_variant_fails_both_tails():
    assert maf_ok(0.004) is False
    assert maf_ok(0.996) is False   # symmetric: MAF 0.004


def test_maf_ok_boundary():
    assert maf_ok(0.005) is True    # >= is inclusive


def test_maf_ok_missing_is_not_applicable():
    assert maf_ok(float("nan")) is True
    assert maf_ok(None) is True


def test_maf_ok_out_of_range_fails():
    assert maf_ok(1.5) is False
    assert maf_ok(-0.1) is False


def test_maf_ok_custom_threshold():
    assert maf_ok(0.02, maf_min=0.05) is False
    assert maf_ok(0.06, maf_min=0.05) is True


def test_info_ok():
    assert info_ok(0.3) is True
    assert info_ok(0.29) is False
    assert info_ok(1.2) is True          # no upper bound
    assert info_ok(float("nan")) is True
    assert info_ok(None) is True
    assert info_ok(0.5, info_min=0.8) is False
