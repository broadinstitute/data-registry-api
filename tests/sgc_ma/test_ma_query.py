from dataregistry.api.query import MA_INSERT_SQL, MA_LIST_SQL


def test_ma_insert_sql_upserts_by_pheno_ancestry():
    assert "sgc_gwas_ma_results" in MA_INSERT_SQL
    assert "ON DUPLICATE KEY UPDATE" in MA_INSERT_SQL
    assert "PENDING" in MA_INSERT_SQL


def test_ma_list_sql_selects_all():
    assert "FROM sgc_gwas_ma_results" in MA_LIST_SQL
