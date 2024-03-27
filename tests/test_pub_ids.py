from dataregistry.pub_med import infer_id_type, PubIdType, format_authors, get_elocation_id


def test_infer_poi():
    assert infer_id_type("10.1000/182") == PubIdType.DOI
    assert infer_id_type("10.1093/nar/gks1195") == PubIdType.DOI


def test_infer_pmid():
    assert infer_id_type("12345678") == PubIdType.PMID


def test_infer_pmcid():
    assert infer_id_type("PMC1004567") == PubIdType.PMCID


def test_single_author_list():
    assert format_authors([{'LastName': 'Hite', 'Initials': 'D.M.'}]) == "Hite D.M."


def test_two_author_list():
    assert (format_authors([{'LastName': 'Newton', 'Initials': 'I.R.'}, {'LastName': 'Descartes', 'Initials': 'R.T.'}])
            == "Newton I.R., Descartes R.T.")


def test_get_elocation_id():
    assert get_elocation_id({'ELocationID': [{'@EIdType': 'Type', '#text': 'Blah, blah'}]}) == "Type: Blah, blah"
