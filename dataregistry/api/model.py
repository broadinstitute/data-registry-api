from datetime import datetime
from enum import Enum
from typing import Union

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


class Sex(str, Enum):
    mixed = "mixed"
    male = "male"
    female = "female"


class GenomeBuild(str, Enum):
    grch38 = "grch38"
    hg19 = "hg19"


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


class Record(BaseModel, extra=Extra.forbid):
    name: str = Field(example="Cade2021_SleepApnea_Mixed_Female", default='...')
    data_source_type: DataSourceType = Field(title="How the owner can transmit the data to the portal", example="file")
    data_source: str = Field(example="??")
    data_type: DataFormat = Field(example="wgs")
    genome_build: GenomeBuild = Field(example="grch38")
    ancestry: Ancestry
    data_submitter: str = Field(example="Frances Crick")
    data_submitter_email: EmailStr
    institution: str = Field(example="Harvard University")
    sex: Sex
    global_sample_size: int
    t1d_sample_size: int
    bmi_adj_sample_size: int
    status: ResearchStatus = Field(title="Where the research is in the publication process")
    additional_data: str = Field(example="More descriptive text...")
    credible_set: Union[str, None] = Field(default=None, example="??")


class SavedRecord(Record):
    id: int
    created_at: datetime
    deleted_at: datetime
    s3_bucket_id: str
