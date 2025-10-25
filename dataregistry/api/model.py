from datetime import datetime
from enum import Enum
from typing import Union, List, Dict
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, Extra


class StartAggregatorRequest(BaseModel):
    branch: str
    method: str
    args: str


class MetaAnalysisRequest(BaseModel):
    method: str
    datasets: List[UUID]
    name: str
    phenotype: str
    created_by: Union[str, None]


class SavedMetaAnalysisRequest(MetaAnalysisRequest):
    id: UUID
    created_at: datetime
    dataset_names: List[str]
    status: str
    log: Union[str, None]


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
    exseq = "exseq"
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

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, o: object) -> bool:
        return isinstance(o, DataSet) and self.name == o.name


class BioIndexCreationStatus(str, Enum):
    FILE_UPLOADED = "FILE UPLOADED"
    SUBMITTED_FOR_PROCESSING = "SUBMITTED FOR PROCESSING"
    SORTING = "SORTING"
    CONVERTING_TO_JSON = "CONVERTING TO JSON"
    INDEXING = "INDEXING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class HermesFileStatus(str, Enum):
    SUBMITTED_TO_QC = "SUBMITTED TO QC"
    SUBMISSION_TO_QC_FAILED = "FAILED TO SUBMIT TO QC"
    FAILED_QC = "FAILED QC"
    READY_FOR_REVIEW = "READY FOR REVIEW"
    REVIEW_APPROVED = "REVIEW APPROVED"
    REVIEW_REJECTED = "REVIEW REJECTED"


class HermesMetaAnalysisStatus(str, Enum):
    SUBMITTED = "SUBMITTED"
    FAILED = "FAILED"
    READY_FOR_REVIEW = "READY FOR REVIEW"


class HermesUploadStatus(BaseModel):
    status: HermesFileStatus


class NewUserRequest(BaseModel):
    user_name: EmailStr
    password: str
    user_type: str
    first_name: Union[str, None] = None
    last_name: Union[str, None] = None


class CsvBioIndexRequest(BaseModel):
    column: str
    status: BioIndexCreationStatus
    already_sorted: bool
    s3_path: str
    data_types: Union[dict, None]
    created_at: Union[datetime, None]


class SavedCsvBioIndexRequest(CsvBioIndexRequest):
    name: UUID
    ip_address: Union[str, None]


class SavedDataset(DataSet):
    id: UUID
    user_id: Union[int, None]
    created_at: Union[datetime, None]


class SavedPhenotypeDataSet(BaseModel):
    id: UUID
    dataset_id: UUID
    phenotype: str
    dichotomous: bool
    file_name: str
    sample_size: int
    cases: Union[int, None]
    controls: Union[int, None]
    created_at: datetime
    s3_path: str
    file_size: int
    short_id: Union[str, None]

    def __hash__(self) -> int:
        return hash((self.dataset_id, self.phenotype))

    def __eq__(self, o: object) -> bool:
        return isinstance(o,
                          SavedPhenotypeDataSet) and self.phenotype == o.phenotype and self.dataset_id == o.dataset_id


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
    short_id: Union[str, None]


class UserCredentials(BaseModel):
    user_name: str
    password: Union[str, None]


class HermesUser(BaseModel):
    id: int
    user_name: str
    created_at: datetime
    last_login: Union[datetime, None]
    is_active: bool
    role: Union[str, None]


class User(BaseModel):
    user_name: str
    first_name: Union[str, None]
    last_name: Union[str, None]
    email: Union[EmailStr, None]
    avatar: Union[str, None]
    is_active: Union[bool, None]
    roles: List[str]
    groups: Union[List[str], None]
    permissions: Union[List[str], None]
    is_internal: Union[bool, None]
    api_token: Union[str, None]
    id: Union[int, None]


class CreateBiondexRequest(BaseModel):
    dataset_id: UUID
    schema_desc: str


class BioIndex(BaseModel):
    dataset_id: UUID
    schema_desc: str
    url: str

class QCScriptOptions(BaseModel):
    fd: float
    adj: Union[str, None]
    noind: Union[bool, None]
    it: Union[float, None]

class QCHermesFileRequest(BaseModel):
    file_name: str
    dataset: str
    metadata: dict
    qc_script_options: QCScriptOptions

class HermesPhenotype(BaseModel):
    name: str
    description: str
    dichotomous: bool


class SGCPhenotype(BaseModel):
    phenotype_code: str
    description: str
    created_at: datetime


class SGCCohort(BaseModel):
    id: Union[UUID, None] = None
    name: str
    uploaded_by: Union[str, None] = None
    total_sample_size: int
    number_of_males: int
    number_of_females: int
    cohort_metadata: Union[Dict, None] = None
    validation_status: Union[bool, None] = None
    created_at: Union[datetime, None] = None
    updated_at: Union[datetime, None] = None


class SGCCohortFile(BaseModel):
    id: Union[UUID, None] = None
    cohort_id: UUID
    file_type: str
    file_path: str
    file_name: str
    file_size: int
    column_mapping: Union[Dict, None] = None
    uploaded_at: Union[datetime, None] = None


class SGCCasesControlsMetadata(BaseModel):
    id: Union[UUID, None] = None
    file_id: Union[UUID, None] = None
    distinct_phenotypes: List[str]  # Treated as set logically
    total_cases: int
    total_controls: int
    phenotype_counts: Dict[str, Dict[str, int]] = {}  # {phenotype: {"cases": count, "controls": count}}


class SGCCoOccurrenceMetadata(BaseModel):
    id: Union[UUID, None] = None
    file_id: Union[UUID, None] = None
    distinct_phenotypes: List[str]  # Treated as set logically
    total_pairs: int
    total_cooccurrence_count: int
    phenotype_pair_counts: Dict[str, int] = {}  # {"phenotype1|phenotype2": count}


class SGCPhenotypeCaseTotals(BaseModel):
    phenotype_code: str
    total_cases_across_cohorts: int
    total_controls_across_cohorts: int
    num_cohorts: int


class SGCPhenotypeCaseCountsBySex(BaseModel):
    phenotype_code: str
    male_cases: int
    male_controls: int
    male_num_cohorts: int
    female_cases: int
    female_controls: int
    female_num_cohorts: int
    both_cases: int
    both_controls: int
    both_num_cohorts: int


class FileUpload(BaseModel):
    id: UUID
    dataset_name: str
    file_name: str
    file_size: int
    uploaded_at: datetime
    uploaded_by: str
    phenotype: Union[str, None]
    ancestry: Union[str, None]
    metadata: Union[dict, None]
    qc_status: HermesFileStatus
    s3_path: Union[str, None]
    qc_log: Union[str, None]
    qc_script_options: Union[dict, None]
    qc_job_submitted_at: Union[datetime, None]
    qc_job_completed_at: Union[datetime, None]

    def dict(self, **kwargs):
        d = super().dict(**kwargs)
        d['status'] = d.get('qc_status', None)
        d['log'] = d.get('qc_log', None)
        return d
