import fastapi
from fastapi import APIRouter, BackgroundTasks, Depends
from pydantic import BaseModel
from typing import Optional, Dict

from dataregistry.api import query, s3, qc_runner
from dataregistry.api.api import get_current_user
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import User

router = APIRouter()
engine = DataRegistryReadWriteDB().get_engine()


class QCRunRequest(BaseModel):
    input_s3_path: str
    pipeline: str
    params: Dict
    pinned_commit: Optional[str] = None


@router.post("/qc/run")
async def start_qc_run(request: QCRunRequest, background_tasks: BackgroundTasks,
                       user: User = Depends(get_current_user)):
    run_id = qc_runner.kick_off_qc_run(
        engine, background_tasks, request.input_s3_path, request.pipeline,
        request.params, submitted_by=user.user_name, pinned_commit=request.pinned_commit,
    )
    return {"run_id": run_id, "status": "SUBMITTED"}


@router.get("/qc/run/{run_id}")
async def get_qc_run(run_id: str, user: User = Depends(get_current_user)):
    run = query.get_qc_run_by_id(engine, run_id)
    if not run:
        raise fastapi.HTTPException(status_code=404, detail="run not found")
    for key_field, url_field in (('gwas_filtered_s3_key', 'gwas_filtered_url'),
                                 ('qc_report_s3_key', 'qc_report_url')):
        if run.get(key_field):
            run[url_field] = s3.generate_presigned_url(
                'get_object', {'Bucket': s3.BASE_BUCKET, 'Key': run[key_field]}, 3600)
    steps = query.list_qc_step_results(engine, run_id)
    return {"run": run, "steps": steps}
