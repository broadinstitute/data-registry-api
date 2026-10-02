from dataregistry.api.peg_evidence import classify_evidence_columns


def test_groups_evidence_columns_by_category_in_header_order():
    header = ["PrimaryVariantID", "GeneSymbol", "PROX", "FUNC", "QTL", "DB",
              "LIT_RareVariant", "LIT_MRorDrug", "PERTURB",
              "INT_PoPS", "INT_n_predictors", "INT_author_conclusion"]

    assert classify_evidence_columns(header) == {
        "PROX": ["PROX"],
        "FUNC": ["FUNC"],
        "QTL": ["QTL"],
        "DB": ["DB"],
        "LIT": ["LIT_RareVariant", "LIT_MRorDrug"],
        "PERTURB": ["PERTURB"],
    }


def test_ignores_identifier_integration_and_unknown_columns():
    header = ["PrimaryVariantID", "GeneSymbol", "INT_QTL_score", "notes", "QTLish", "XYZ_QTL"]

    assert classify_evidence_columns(header) == {}


def test_stream_suffixed_columns_group_under_their_category():
    assert classify_evidence_columns(["QTL_eQTL-aorta", "QTL_pQTL"]) == {"QTL": ["QTL_eQTL-aorta", "QTL_pQTL"]}
