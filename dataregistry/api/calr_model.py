from datetime import datetime
from typing import Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, conlist


class CalRNewUserRequest(BaseModel):
    user_name: EmailStr
    password: str


class CalRSubmissionMetadata(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    species: Optional[str] = None
    tissue: Optional[str] = None
    litter: Optional[int] = None
    bedding: Optional[str] = None
    ee_calc: Optional[str] = None
    enrich: Optional[str] = None
    experiment_id: Optional[str] = None
    age: Optional[float] = None
    strain: Optional[str] = None
    genetic_background: Optional[str] = None
    sex: Optional[str] = None  # male / female / both / other
    temperature: Optional[float] = None
    quality_score: Optional[float] = None
    system: Optional[str] = None  # CLAMS / TSE / Sable / Other
    location: Optional[str] = None
    pmid: Optional[str] = None
    investigator: Optional[str] = None
    treatment: Optional[str] = None


class CALRSubmission(BaseModel):
    id: Union[UUID, None] = None
    name: str
    description: Union[str, None] = None
    public: bool = False
    uploaded_by: str
    uploaded_at: Union[datetime, None] = None


class CALRFile(BaseModel):
    id: Union[UUID, None] = None
    submission_id: str
    file_type: str  # 'standard' or 'session'
    file_name: str
    file_size: int
    s3_path: str
    uploaded_at: Union[datetime, None] = None


class AnovaRequest(BaseModel):
    session_id: str
    variable: str
    mass_variable: str = 'subject.mass'
    time_of_day: str = 'total'  # 'light', 'dark', or 'total'


class AncovaTableRequest(BaseModel):
    session_id: str
    mass_variable: str = 'subject.mass'


class QualityControlRequest(BaseModel):
    session_id: str
    n_mass_measurements: int = Field(default=5, ge=1, le=15)


class PowerCalcRequest(BaseModel):
    session_id: str
    variable: str
    mass_variable: str = 'subject.mass'
    time_of_day: str = 'total'  # 'light', 'dark', or 'total'
    sample_sizes: List[int] = [4, 8, 12, 16, 20, 24]
    alpha: float = Field(default=0.05, gt=0, lt=1)


class Group(BaseModel):
    name: str
    diet_name: Optional[str] = None
    diet_kcal: Optional[float] = None


class Subject(BaseModel):
    subject: str
    groupIndex: int
    total_mass: Optional[float] = None
    lean_mass: Optional[float] = None
    fat_mass: Optional[float] = None
    exc_hour: Optional[float] = None
    exc_reason: Optional[str] = None


class CalRSession(BaseModel):
    """
    Experiment session configuration used as input to all CalR analysis endpoints.

    groups:             list of group definitions (name, diet_name, diet_kcal)
    subjects:           list of subjects — group membership, mass values, and exclusions
    light_cycle_start:  hour (0-23) when light cycle begins
    dark_cycle_start:   hour (0-23) when dark cycle begins
    hour_range:         [start_hour, end_hour] window for analysis
    food_cutoff:        optional hard food cutoff value; null means no cutoff
    remove_outliers:    whether to exclude statistical outliers
    group_colors:       optional mapping of group name → hex color (e.g. "#3B73C7")
    """
    submission_id: str
    groups: List[Group]
    subjects: List[Subject]
    light_cycle_start: int = Field(ge=0, le=23)
    dark_cycle_start: int = Field(ge=0, le=23)
    hour_range: conlist(float, min_items=2, max_items=2)
    food_cutoff: Optional[float] = None
    remove_outliers: bool = False
    group_colors: Optional[Dict[str, str]] = None


class CalRSessionUpdate(CalRSession):
    """CalRSession variant for PUT requests where submission_id is derived from the existing record."""
    submission_id: Optional[str] = None
