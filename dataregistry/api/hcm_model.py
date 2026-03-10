from datetime import datetime
from typing import Dict, Union

from pydantic import BaseModel


class HCMGWASFile(BaseModel):
    id: Union[str, None] = None
    cohort_name: str
    sarc: str  # ALL, SP, SN
    ancestry: str  # EUR, AFR, EAS, SAS, AMR, ALL
    sex: str  # ALL, MALE, FEMALE
    genome_build: str  # GRCh37, GRCh38
    software: str  # e.g. REGENIE, SAIGE
    analyst: str
    file_name: str
    file_size: int
    s3_path: str
    uploaded_at: Union[datetime, None] = None
    uploaded_by: str
    column_mapping: Dict[str, str]
    cases: Union[int, None] = None
    controls: Union[int, None] = None
    metadata: Union[Dict, None] = None
