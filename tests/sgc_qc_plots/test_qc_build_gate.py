def test_qc_list_files_sql_reads_effective_build():
    from sgc_qc_plots.submit_qc_plots_batch import _LIST_SQL
    assert "LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id" in _LIST_SQL
    assert ("COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build')), "
            "JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))) AS genome_build") in _LIST_SQL


def test_is_grch38_effective():
    from sgc_qc_plots.submit_qc_plots_batch import _is_grch38_effective
    assert _is_grch38_effective({"genome_build": "GRCh38"}) is True
    assert _is_grch38_effective({"genome_build": "liftover to GRCh38"}) is True   # normalize_build
    assert _is_grch38_effective({"genome_build": "GRCh37"}) is False
    assert _is_grch38_effective({"genome_build": "hg19"}) is False
    assert _is_grch38_effective({"genome_build": None}) is False
