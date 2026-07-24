from dataregistry.api.query import MA_INSERT_SQL, MA_LIST_SQL


def test_ma_insert_sql_upserts_by_pheno_ancestry():
    assert "sgc_gwas_ma_results" in MA_INSERT_SQL
    assert "ON DUPLICATE KEY UPDATE" in MA_INSERT_SQL
    assert "PENDING" in MA_INSERT_SQL


def test_ma_list_sql_selects_all():
    assert "FROM sgc_gwas_ma_results" in MA_LIST_SQL


def test_ma_insert_sql_clears_totals_on_reset():
    assert "total_cases=NULL" in MA_INSERT_SQL
    assert "total_controls=NULL" in MA_INSERT_SQL


def test_ma_list_sql_selects_totals():
    assert "total_cases" in MA_LIST_SQL
    assert "total_controls" in MA_LIST_SQL


def test_sgc_ma_result_model_has_totals():
    from dataregistry.api.model import SGCMAResult
    m = SGCMAResult(id="x", phenotype="p", ancestry="a", status="SUCCEEDED",
                    total_cases=150, total_controls=300)
    assert m.total_cases == 150 and m.total_controls == 300
