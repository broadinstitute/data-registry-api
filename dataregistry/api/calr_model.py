from datetime import datetime
from typing import Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field, conlist


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


class SubjectExclusion(BaseModel):
    hours: List[float]
    reason: Union[str, None] = None


class CalRSession(BaseModel):
    """
    Experiment session configuration used as input to all CalR analysis endpoints.

    groups:             mapping of group name → list of subject IDs
    group_colors:       optional mapping of group name → hex color (e.g. "#3B73C7")
    light_cycle_start:  hour (0-23) when light cycle begins
    dark_cycle_start:   hour (0-23) when dark cycle begins
    hour_range:         [start_hour, end_hour] window for analysis
    remove_outliers:    whether to exclude statistical outliers
    subject_mass:       optional mapping of subject ID → body mass (g); required for ANCOVA
    exclusions:         optional per-subject exclusion windows
    """
    submission_id: str
    groups: Dict[str, List[str]]
    group_colors: Optional[Dict[str, str]] = None
    light_cycle_start: int = Field(ge=0, le=23)
    dark_cycle_start: int = Field(ge=0, le=23)
    hour_range: conlist(float, min_items=2, max_items=2)
    remove_outliers: bool = False
    subject_mass: Optional[Dict[str, float]] = None
    exclusions: Optional[Dict[str, SubjectExclusion]] = None
