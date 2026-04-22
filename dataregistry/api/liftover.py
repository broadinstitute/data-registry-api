"""
liftover.py — orchestrates the GWAS liftover flow.

When a user uploads a Hermes GWAS file whose declared genome_build differs from
the portal's target_genome_build, we:
  1. Create a liftover_jobs row (status SUBMITTED).
  2. Submit gwas-liftover-job to AWS Batch (via background task).
  3. On SUCCEEDED: flip file_uploads.genome_build to the target build, set
     qc_status = SUBMITTED TO QC, then enqueue the hermes-qc-job (same path
     as today's direct QC submission).
  4. On FAILED: set qc_status = LIFTOVER FAILED; QC is NOT submitted.
"""
import json
import logging
import re
import uuid

from starlette.background import BackgroundTasks

from dataregistry.api import batch, query, s3
from dataregistry.api.model import GenomeBuild, HermesFileStatus, LiftoverJobStatus

logger = logging.getLogger(__name__)


def should_liftover(source: GenomeBuild, target: GenomeBuild) -> bool:
    """Return True iff a liftover is needed.

    False when:
    - source is GenomeBuild.na (no build declared)
    - source already matches target
    """
    if source == GenomeBuild.na:
        return False
    return source != target


def submit_liftover_then_qc(
    engine,
    file_id: str,
    source_build: GenomeBuild,
    target_build: GenomeBuild,
    dataset: str,
    file_name: str,
    column_mapping: dict,
    qc_script_options: dict,
    user_name: str,
    background_tasks: BackgroundTasks,
):
    """Create a liftover_jobs row and enqueue the AWS Batch gwas-liftover-job.

    On completion the batch callback will update the liftover_jobs row and
    file_uploads row, then (on success) chain into the hermes-qc-job.
    """
    liftover_job_id = str(uuid.uuid4())

    input_s3_path = f"s3://{s3.BASE_BUCKET}/hermes/{dataset}/{file_name}"
    # The worker writes the lifted file back over the original input path.
    output_s3_path = input_s3_path
    archive_s3_path = f"s3://{s3.BASE_BUCKET}/hermes/archive/{file_id}/{file_name}"
    unmapped_s3_path = f"s3://{s3.BASE_BUCKET}/hermes/liftover/{file_id}/unmapped.tsv"
    summary_s3_path = f"s3://{s3.BASE_BUCKET}/hermes/liftover/{file_id}/summary.json"

    query.create_liftover_job(
        engine,
        liftover_job_id,
        file_id,
        source_build,
        target_build,
        input_s3_path,
        unmapped_s3_path,
        user_name,
    )

    job_config = {
        'jobName': 'gwas-liftover-job',
        'jobQueue': 'gwas-liftover-job-queue',
        'jobDefinition': 'gwas-liftover-job',
        'parameters': {
            'input-s3-path': input_s3_path,
            'output-s3-path': output_s3_path,
            'archive-s3-path': archive_s3_path,
            'unmapped-s3-path': unmapped_s3_path,
            'summary-s3-path': summary_s3_path,
            'source-build': source_build.value,
            'target-build': target_build.value,
            'column-mapping': json.dumps(column_mapping),
            'job-id': liftover_job_id,
        },
    }

    callback = _build_callback(
        file_id=file_id,
        dataset=dataset,
        file_name=file_name,
        column_mapping=column_mapping,
        qc_script_options=qc_script_options,
        target_build=target_build,
    )

    # Submit with is_qc=False so batch.submit_and_await_job passes the raw
    # job_status string ('SUCCEEDED' or 'FAILED') to our callback rather than
    # converting it to a HermesFileStatus.  The is_qc=False branch also calls
    # record_meta_analysis_job_submission_time, which issues an UPDATE against
    # meta_analyses — there is no matching row, so the UPDATE is a harmless
    # no-op.  The liftover_jobs.submitted_at timestamp is set by the DB column
    # DEFAULT (CURRENT_TIMESTAMP), so timing is correct without modifying
    # batch.py.
    background_tasks.add_task(
        batch.submit_and_await_job,
        engine,
        job_config,
        callback,
        liftover_job_id,
        False,  # is_qc=False → raw status string in callback
    )


def _build_callback(
    file_id: str,
    dataset: str,
    file_name: str,
    column_mapping: dict,
    qc_script_options: dict,
    target_build: GenomeBuild,
):
    """Return the closure invoked by batch.submit_and_await_job on completion.

    Signature expected by submit_and_await_job (is_qc=False):
        callback(engine, complete_log, liftover_job_id, job_status)
    where job_status is the raw string 'SUCCEEDED' or 'FAILED'.
    """

    def liftover_completion_callback(
        cb_engine, complete_log: str, liftover_job_id: str, job_status: str
    ):
        # --- Parse the summary JSON from the worker log ---
        # The JSON is a compact single line; use a non-greedy match and no
        # re.DOTALL so trailing log lines after the JSON do not confuse the
        # anchor.
        summary = None
        match = re.search(r'LIFTOVER_SUMMARY_JSON:\s*(\{.*?\})', complete_log or '')
        if match:
            try:
                summary = json.loads(match.group(1))
            except json.JSONDecodeError:
                logger.warning("Could not parse LIFTOVER_SUMMARY_JSON from log")

        if job_status == 'SUCCEEDED':
            liftover_status = LiftoverJobStatus.COMPLETE.value
            new_qc_status = HermesFileStatus.SUBMITTED_TO_QC
            new_genome_build = target_build
        else:
            liftover_status = LiftoverJobStatus.FAILED.value
            new_qc_status = HermesFileStatus.LIFTOVER_FAILED
            new_genome_build = None  # do NOT flip on failure

        query.update_liftover_job(
            cb_engine,
            liftover_job_id,
            status=liftover_status,
            log=complete_log,
            summary=summary,
        )
        query.update_file_upload_after_liftover(
            cb_engine,
            file_id,
            qc_status=new_qc_status,
            genome_build=new_genome_build,
        )

        if job_status == 'SUCCEEDED':
            # The lifted file overwrites the original S3 path, so we still
            # reference the same s3_path as before.
            s3_path = f"hermes/{dataset}/{file_name}"
            qc_job_config = {
                'jobName': 'hermes-qc-job',
                'jobQueue': 'hermes-qc-job-queue',
                'jobDefinition': 'hermes-qc-job',
                'parameters': {
                    's3-path': f"s3://{s3.BASE_BUCKET}/{s3_path}",
                    'file-guid': file_id,
                    'col-map': json.dumps(column_mapping),
                    'script-options': json.dumps(qc_script_options),
                },
            }
            # Synchronous call — we are already inside the background task, so
            # nesting submit_and_await_job here is intentional.
            try:
                batch.submit_and_await_job(
                    cb_engine,
                    qc_job_config,
                    query.update_file_upload_qc_log,
                    file_id,
                    True,  # is_qc=True → HermesFileStatus converted inside batch.py
                )
            except Exception:
                logger.exception(
                    "Failed to submit QC job after successful liftover for file %s; "
                    "marking as SUBMISSION_TO_QC_FAILED",
                    file_id,
                )
                query.update_file_upload_after_liftover(
                    cb_engine,
                    file_id,
                    qc_status=HermesFileStatus.SUBMISSION_TO_QC_FAILED.value,
                )
                raise

    return liftover_completion_callback
