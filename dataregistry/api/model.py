from datetime import datetime
from enum import Enum

from pydantic import BaseModel, EmailStr, Field


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


class Record(BaseModel):
    name: str
    metadata: dict
    data_source_type: DataSourceType = Field(title="How the owner can transmit the data to the portal")
    data_source: str
    data_type: DataFormat
    genome_build: GenomeBuild
    ancestry: Ancestry
    data_submitter: str
    data_submitter_email: EmailStr
    institution: str
    sex: Sex
    global_sample_size: float
    t1d_sample_size: float
    bmi_adj_sample_size: float
    status: ResearchStatus = Field(title="Where the research is in the publication process")
    additional_data: str

    class Config:
        schema_extra = {
            "example": {
                "name": "Cade2021_SleepApnea_Mixed_Female",
                "data_source_type": "file",
                "data_source": "??",
                "data_type": "wgs",
                "genome_build": "grch38",
                "ancestry": "EA",
                "data_submitter": "Jennifer Doudna",
                "data_submitter_email": "researcher@institute.org",
                "institution": "UCSD",
                "sex": "female",
                "global_sample_size": 11,
                "t1d_sample_size": 12,
                "bmi_adj_sample_size": 19,
                "status": "open",
                "additional_data": "Lorem ipsum..",
                "metadata": {"some_key": "some_value"}
            }
        }


class SavedRecord(Record):
    id: int
    created_at: datetime
    deleted_at: datetime
    s3_bucket_id: str
