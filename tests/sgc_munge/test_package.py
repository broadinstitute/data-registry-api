def test_package_imports_filters():
    import sgc_munge  # noqa
    from sgc_ma.filters import maf_ok, info_ok
    assert maf_ok(float("nan"), 0.005) and not maf_ok(0.001, 0.005)
    assert info_ok(float("nan"), 0.3) and not info_ok(0.2, 0.3)
