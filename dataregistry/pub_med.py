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


def get_elocation_id(article_meta):
    eloc_dict_list = article_meta.get('ELocationID')
    if not eloc_dict_list:
        return None
    if isinstance(eloc_dict_list, list):
        eloc_dict_list = eloc_dict_list[0]
    return f"{eloc_dict_list.get('@EIdType')}: {eloc_dict_list.get('#text')}"


def format_authors(author_list):
    if isinstance(author_list, list):
        if len(author_list) < 2:
            return f"{author_list[0].get('LastName', '')} {author_list[0].get('Initials', '')}"
        else:
            result = ''
            for author in author_list[0:2]:
                result += f"{author.get('LastName', '')} {author.get('Initials', '')}, "
            if len(author_list) > 2:
                return result + 'et al.'
            else:
                return result[:-2]
    else:
        return f"{author_list.get('LastName', '')} {author_list.get('Initials', '')}"
