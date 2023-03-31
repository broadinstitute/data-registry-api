from dataregistry.pub_ids import infer_id_type, PubIdType


def test_infer_poi():
    assert infer_id_type("10.1000/182") == PubIdType.DOI
    assert infer_id_type("10.1093/nar/gks1195") == PubIdType.DOI


def test_infer_pmid():
    assert infer_id_type("12345678") == PubIdType.PMID


def test_infer_pmcid():
    assert infer_id_type("PMC1004567") == PubIdType.PMCID
