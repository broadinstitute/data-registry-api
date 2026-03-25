"""backfill calr_files records for session JSONs already in S3

Revision ID: backfill_calr_session_files
Revises: create_hcm_gwas_validation_jobs
Create Date: 2026-03-25 10:00:00.000000

"""
import json
import os

import boto3
from alembic import op
from sqlalchemy import text

revision = 'backfill_calr_session_files'
down_revision = 'create_hcm_gwas_validation_jobs'
branch_labels = None
depends_on = None

S3_REGION = 'us-east-1'
BASE_BUCKET = os.environ.get('DATA_REGISTRY_BUCKET', 'dig-data-registry')


def upgrade() -> None:
    conn = op.get_bind()
    s3_client = boto3.client('s3', region_name=S3_REGION)
    paginator = s3_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=BASE_BUCKET, Prefix='calr/'):
        for obj in page.get('Contents', []):
            key = obj['Key']

            # Only process session files: calr/{username}/sessions/{session_id}.json
            parts = key.split('/')
            if len(parts) != 4 or parts[2] != 'sessions' or not parts[3].endswith('.json'):
                continue

            session_id = parts[3][:-len('.json')]

            # Skip if already in DB
            existing = conn.execute(
                text("SELECT id FROM calr_files WHERE id = :id"),
                {'id': session_id}
            ).first()
            if existing:
                continue

            # Read session JSON to get submission_id
            try:
                response = s3_client.get_object(Bucket=BASE_BUCKET, Key=key)
                session_data = json.loads(response['Body'].read())
            except Exception as e:
                print(f"WARNING: could not read {key}: {e}")
                continue

            submission_id = session_data.get('submission_id')
            if not submission_id:
                print(f"WARNING: no submission_id in {key}, skipping")
                continue

            # Verify the submission exists
            sub = conn.execute(
                text("SELECT id FROM calr_submissions WHERE id = :id"),
                {'id': submission_id}
            ).first()
            if not sub:
                print(f"WARNING: submission {submission_id} not found for {key}, skipping")
                continue

            conn.execute(text("""
                INSERT INTO calr_files (id, submission_id, file_type, file_name, file_size, s3_path)
                VALUES (:id, :submission_id, 'session', :file_name, :file_size, :s3_path)
            """), {
                'id': session_id,
                'submission_id': submission_id,
                'file_name': parts[3],
                'file_size': obj['Size'],
                's3_path': key,
            })
            print(f"Inserted calr_files record for session {session_id}")


def downgrade() -> None:
    # Sessions created before this migration had no DB record; removing them here
    # would delete records that the app now depends on, so downgrade is a no-op.
    pass
