"""Copy one phenotype's GRCh38 SGC GWAS files from prod to QA (QA-only tooling).

Purpose: to exercise the munge + METAL pipeline in QA with real data, copy the
sgc_gwas_files rows (plus their sgc_cohorts / sgc_gwas_cohorts parents) and the
S3 objects for one phenotype from prod into QA. Primary keys and S3 keys are
preserved verbatim so later stages behave exactly as in prod. Rows are written
with INSERT IGNORE, so re-runs are idempotent. Both DB URLs are mandatory:
nothing is ever selected implicitly. Prod is only read.

Example:
    python scripts/sgc/copy_prod_phenotype_to_qa.py --phenotype PSOR \\
        --cohort UKBB --prod-db-connection 'mysql+pymysql://u:p@prod-host/db' \\
        --qa-db-connection 'mysql+pymysql://u:p@qa-host/db' --dry-run
"""
import click
import sqlalchemy
from sqlalchemy import bindparam, text

from sgc_ma.select import normalize_build

_FILES_BASE_SQL = """
SELECT f.*,
       COALESCE(JSON_UNQUOTE(JSON_EXTRACT(f.metadata, '$.genome_build')),
                JSON_UNQUOTE(JSON_EXTRACT(gc.metadata, '$.genome_build'))) AS genome_build_raw,
       sc.name AS cohort_name
FROM sgc_gwas_files f
LEFT JOIN sgc_gwas_cohorts gc ON gc.cohort_id = f.cohort_id
LEFT JOIN sgc_cohorts sc ON sc.id = f.cohort_id
WHERE f.phenotype = :phenotype
"""
_COHORT_CLAUSE = " AND sc.name IN :cohorts"
_FILES_SQL = _FILES_BASE_SQL + _COHORT_CLAUSE

_HELPER_COLS = ("genome_build_raw", "cohort_name")


def select_rows(prod_engine, phenotype, cohorts=()):
    cohorts = tuple(cohorts)
    if cohorts:
        stmt = text(_FILES_SQL).bindparams(bindparam("cohorts", expanding=True))
        params = {"phenotype": phenotype, "cohorts": list(cohorts)}
    else:
        stmt = text(_FILES_BASE_SQL)
        params = {"phenotype": phenotype}
    with prod_engine.connect() as conn:
        raw = [dict(r) for r in conn.execute(stmt, params).mappings()]
        files = []
        for r in raw:
            if normalize_build(r.get("genome_build_raw")) != "GRCh38":
                continue
            files.append({k: v for k, v in r.items() if k not in _HELPER_COLS})
        ids = sorted({f["cohort_id"] for f in files if f.get("cohort_id") is not None})
        if not ids:
            return {"cohorts": [], "gwas_cohorts": [], "files": files}
        coh = conn.execute(
            text("SELECT * FROM sgc_cohorts WHERE id IN :ids").bindparams(
                bindparam("ids", expanding=True)), {"ids": ids}).mappings()
        gcoh = conn.execute(
            text("SELECT * FROM sgc_gwas_cohorts WHERE cohort_id IN :ids").bindparams(
                bindparam("ids", expanding=True)), {"ids": ids}).mappings()
        return {"cohorts": [dict(r) for r in coh],
                "gwas_cohorts": [dict(r) for r in gcoh],
                "files": files}


def plan_copy(rows, src_bucket, dst_bucket):
    return [(src_bucket, f["s3_path"], dst_bucket) for f in rows["files"]]


_ORDER = (("sgc_cohorts", "cohorts"), ("sgc_gwas_cohorts", "gwas_cohorts"),
          ("sgc_gwas_files", "files"))


def insert_rows(qa_engine, rows):
    counts = {t: 0 for t, _ in _ORDER}
    with qa_engine.begin() as conn:
        for table, key in _ORDER:
            for row in rows.get(key, []):
                cols = list(row.keys())
                sql = (f"INSERT IGNORE INTO {table} ({', '.join(f'`{c}`' for c in cols)}) "
                       f"VALUES ({', '.join(f':{c}' for c in cols)})")
                res = conn.execute(text(sql), row)
                counts[table] += res.rowcount
    return counts


def copy_objects(plan):
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3", region_name="us-east-1")
    for src, key, dst in plan:
        try:
            s3.head_object(Bucket=dst, Key=key)
            print(f"exists  {dst}/{key}")
            continue
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") not in ("404", "NoSuchKey", "NotFound"):
                raise
        s3.copy({"Bucket": src, "Key": key}, dst, key)
        print(f"copied  {src}/{key} -> {dst}/{key}")


@click.command()
@click.option("--phenotype", required=True)
@click.option("--cohort", "cohorts", multiple=True)
@click.option("--src-bucket", default="dig-data-registry", show_default=True)
@click.option("--dst-bucket", default="dig-data-registry-qa", show_default=True)
@click.option("--prod-db-connection", required=True, help="SQLAlchemy URL (read only).")
@click.option("--qa-db-connection", required=True, help="SQLAlchemy URL (written).")
@click.option("--dry-run", is_flag=True)
@click.option("--skip-s3", is_flag=True)
def main(phenotype, cohorts, src_bucket, dst_bucket, prod_db_connection,
         qa_db_connection, dry_run, skip_s3):
    if prod_db_connection == qa_db_connection:
        raise click.UsageError("prod and QA connection URLs must differ")
    rows = select_rows(sqlalchemy.create_engine(prod_db_connection), phenotype, cohorts)
    plan = plan_copy(rows, src_bucket, dst_bucket)
    cohort_names = {c["id"]: c.get("name") for c in rows["cohorts"]}
    for f in rows["files"]:
        print(f"{cohort_names.get(f['cohort_id'])}\t{f.get('dataset')}\t{f['s3_path']}")
    print(f"cohorts={len(rows['cohorts'])} gwas_cohorts={len(rows['gwas_cohorts'])} "
          f"files={len(rows['files'])} s3_objects={0 if skip_s3 else len(plan)}")
    if dry_run:
        print("dry run: nothing copied or inserted")
        return
    if not skip_s3:
        copy_objects(plan)
    print(insert_rows(sqlalchemy.create_engine(qa_db_connection), rows))


if __name__ == "__main__":
    main()
