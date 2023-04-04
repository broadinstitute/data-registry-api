from enum import Enum
import re


class PubIdType(str, Enum):
    DOI = "doi"
    PMID = "pmid"
    PMCID = "pmcid"


def infer_id_type(pub_id: str) -> PubIdType:
    if re.search("^[0-9]+$", pub_id):
        return PubIdType.PMID
    elif re.search("PMC[0-9]+$", pub_id):
        return PubIdType.PMCID
    else:
        return PubIdType.DOI
