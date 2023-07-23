from datetime import datetime
from enum import Enum
from typing import Union
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, Extra


class DataSourceType(str, Enum):
    api = "api"
    file = "file"
    remote = "remote"


class ResearchStatus(str, Enum):
    pre = "pre"
    open = "open"


class DataFormat(str, Enum):
    gwas = "gwas"
    exomchip = "exomchip"
    exomseq = "exomseq"
    ichip = "ichip"
    wgs = "wgs"
    other = "other"


class Sex(str, Enum):
    mixed = "mixed"
    male = "male"
    female = "female"
    na = "n/a"


class GenomeBuild(str, Enum):
    grch38 = "grch38"
    hg19 = "hg19"
    na = "n/a"


class Ancestry(str, Enum):
    AA = "AA"
    ABA = "ABA"
    AF = "AF"
    SSAF = "SSAF"
    ASUN = "ASUN"
    CA = "CA"
    EA = "EA"
    SA = "SA"
    SEA = "SEA"
    EU = "EU"
    GME = "GME"
    HS = "HS"
    NAM = "NAM"
    NR = "NR"
    OC = "OC"
    OTH = "OTH"
    OAD = "OAD"
    Mixed = "Mixed"
    na = "n/a"


class Study(BaseModel, extra=Extra.forbid):
    name: str = Field(example="Cade2021_SleepApnea_Mixed_Female", default='...')
    institution: str = Field(example="Harvard University")


class SavedStudy(Study):
    id: UUID
    created_at: datetime


class DataSet(BaseModel, extra=Extra.forbid):
    name: str = Field(example="Cade2021_SleepApnea_Mixed_Female", default='...')
    data_source_type: DataSourceType = Field(title="How the owner can transmit the data to the portal", example="file")
    data_type: DataFormat = Field(example="wgs")
    genome_build: GenomeBuild = Field(example="grch38")
    ancestry: Ancestry
    data_submitter: str = Field(example="Frances Crick")
    data_submitter_email: EmailStr
    data_contributor_email: Union[EmailStr, None]
    data_contributor: Union[str, None]
    sex: Sex
    global_sample_size: int
    status: ResearchStatus = Field(title="Where the research is in the publication process")
    description: str = Field(example="More descriptive text...")
    study_id: str
    pub_id: Union[str, None]
    publication: Union[str, None]
    publicly_available: Union[bool, None] = Field(title="Whether the data is publicly available")


class SavedDataset(DataSet):
    id: UUID
    created_at: Union[datetime, None]


class SavedPhenotypeDataSet(BaseModel):
    id: UUID
    phenotype: str
    dichotomous: bool
    file_name: str
    sample_size: int
    cases: Union[int, None]
    controls: Union[int, None]
    created_at: datetime
    s3_path: str
    file_size: int


class SavedDatasetInfo(BaseModel):
    dataset: SavedDataset
    study: SavedStudy
    phenotypes: list
    credible_sets: list


class SavedCredibleSet(BaseModel):
    id: UUID
    phenotype_data_set_id: UUID
    phenotype: str
    name: str
    file_name: str
    s3_path: str
    created_at: datetime
    file_size: int
