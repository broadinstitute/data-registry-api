"""Launch the full battery of SGC bottom-line meta-analyses described by
Logic.docx: for every phenotype, the runnable subset of the nine analysis
targets —

    1. Combined (all individuals)          -> (ancestry=Combined, sex=All)
    2. Sex-stratified                       -> (Combined, Male), (Combined, Female)
    3. Ancestry-stratified                  -> (AFR|AMR|EAS|EUR|MID|SAS, All)

A target is "runnable" when >= 2 QC-passed cohorts resolve for it (via the same
selection logic the app uses, incl. the combined-fallback and sex "any-ancestry"
dedup, and the MA ignore-list).

DRY-RUN BY DEFAULT: prints the launch order, the cohort files chosen for each MA,
and a note per MA. Pass --execute to actually insert runs + submit Batch jobs.

REFERENCE PANEL: the MA pipeline is REFERENCE-FREE. Each cohort is deterministically
allele-canonicalized (sgc_ma.harmonize.canonicalize) and the cohorts are k-way
merged (sgc_ma.stream.merge_and_combine); the result does not depend on file order,
and no file acts as a reference panel. The dead reference-based path
(harmonize.harmonize_cohort/ref_map) is not used. The "anchor" shown per MA is only
the first cohort processed (cohorts are sorted by dataset name in the worker); it has
no effect on the meta-analysis and is shown solely to answer the "which file is the
reference" question explicitly: none is.

Safety: defaults to --db-name dataregistry_qa and dry-run; you must pass BOTH
`--db-name dataregistry` and `--execute` to launch against prod.
"""
import os
import time

import click
from sqlalchemy import text

# The nine targets in Logic.docx analysis-type order. label is the UI/display name.
TARGETS = [
    ("Combined", "All", "Combined (all individuals)"),
    ("Combined", "Male", "Male"),
    ("Combined", "Female", "Female"),
    ("AFR", "All", "AFR"),
    ("AMR", "All", "AMR"),
    ("EAS", "All", "EAS"),
    ("EUR", "All", "EUR"),
    ("MID", "All", "MID"),
    ("SAS", "All", "SAS"),
]


def _distinct_phenotypes(engine):
    with engine.connect() as c:
        return [r[0] for r in c.execute(text(
            "SELECT DISTINCT phenotype FROM sgc_gwas_files "
            "WHERE dataset NOT LIKE 'meta_analysis_%' ORDER BY phenotype"))]


def _existing_targets(engine):
    """(phenotype, ancestry, sex) triples that already have a non-FAILED run."""
    with engine.connect() as c:
        return {(r.phenotype, r.ancestry, r.sex) for r in c.execute(text(
            "SELECT phenotype, ancestry, sex FROM sgc_gwas_ma_results "
            "WHERE status <> 'FAILED'"))}


def build_plan(engine, phenotypes, skip_existing):
    """Return an ordered list of runnable MAs:
    [{phenotype, ancestry, sex, label, cohorts:[selected rows]}]. Phenotype-major,
    targets in Logic.docx order (Combined, sex-stratified, ancestry-stratified)."""
    from sgc_ma.select import select_cohorts
    existing = _existing_targets(engine) if skip_existing else set()
    plan = []
    for p in phenotypes:
        for anc, sex, label in TARGETS:
            if (p, anc, sex) in existing:
                continue
            cohorts = select_cohorts(engine, p, anc, sex)  # resolved + ignore-filtered
            if len(cohorts) < 2:
                continue
            plan.append({"phenotype": p, "ancestry": anc, "sex": sex,
                         "label": label, "cohorts": cohorts})
    return plan


def print_plan(plan, execute, db_name, maf_min, info_min, skip_existing):
    mode = "EXECUTE" if execute else "DRY-RUN"
    print(f"# SGC MA battery: {len(plan)} meta-analyses  [{mode}]  "
          f"db={db_name}  skip_existing={skip_existing}  maf_min={maf_min}  info_min={info_min}")
    by_type = {}
    for m in plan:
        t = "Combined" if (m["ancestry"] == "Combined" and m["sex"] == "All") \
            else ("Sex-stratified" if m["sex"] != "All" else "Ancestry-stratified")
        by_type[t] = by_type.get(t, 0) + 1
    print(f"#   by type: {by_type}")
    print("# REFERENCE-FREE pipeline: no reference panel; results are order-independent.")
    print("#   'anchor' = first cohort processed (sorted by dataset); has NO effect on results.")
    for i, m in enumerate(plan, 1):
        anchor = min(m["cohorts"], key=lambda c: c["dataset"])
        print(f"\n[{i}/{len(plan)}] {m['phenotype']}  |  target={m['label']}  "
              f"({m['ancestry']}/{m['sex']})  |  cohorts={len(m['cohorts'])}")
        for co in sorted(m["cohorts"], key=lambda c: c["dataset"]):
            print(f"       - {str(co['cohort']):34s} dataset={str(co['dataset']):30s} file_id={co['file_id']}")
        print(f"       anchor (no-op / reference-free): {anchor['cohort']} · {anchor['dataset']}")


@click.command()
@click.option("--db-name", default="dataregistry_qa", show_default=True,
              help="Target DB. Use 'dataregistry' for prod.")
@click.option("--bucket", default="dig-data-registry", show_default=True)
@click.option("--phenotype", default=None, help="Limit to a single phenotype.")
@click.option("--maf-min", default=0.005, show_default=True, type=float)
@click.option("--info-min", default=0.3, show_default=True, type=float)
@click.option("--skip-existing/--no-skip-existing", default=True, show_default=True,
              help="Skip a target that already has a non-FAILED run (avoids duplicates).")
@click.option("--limit", default=None, type=int, help="Cap the number of MAs (after ordering).")
@click.option("--execute", is_flag=True, default=False,
              help="Actually insert runs + submit Batch jobs. Omitted = dry-run.")
def main(db_name, bucket, phenotype, maf_min, info_min, skip_existing, limit, execute):
    os.environ["DATA_REGISTRY_DB_NAME"] = db_name
    from dataregistry.api.db import DataRegistryReadWriteDB
    engine = DataRegistryReadWriteDB().get_engine()

    phenotypes = [phenotype] if phenotype else _distinct_phenotypes(engine)
    plan = build_plan(engine, phenotypes, skip_existing)
    if limit is not None:
        plan = plan[:limit]

    print_plan(plan, execute, db_name, maf_min, info_min, skip_existing)

    if not execute:
        print(f"\n# DRY-RUN — nothing submitted. Re-run with --execute (and --db-name dataregistry "
              f"for prod) to launch these {len(plan)} MAs.")
        return

    import boto3
    from dataregistry.api import query
    from sgc_ma import submit_ma_batch
    batch = boto3.client("batch", region_name=os.getenv("AWS_REGION", "us-east-1"))
    print(f"\n# Launching {len(plan)} MAs against db={db_name} ...")
    launched = 0
    for i, m in enumerate(plan, 1):
        run_id = query.insert_sgc_ma_run(
            engine, m["phenotype"], m["ancestry"], sex=m["sex"], run_type="auto",
            dataset_file_ids=[c["file_id"] for c in m["cohorts"]],
            maf_min=maf_min, info_min=info_min)
        submit_ma_batch.submit_run(engine=engine, batch=batch, run_id=run_id,
                                   bucket=bucket, db_name=db_name)
        launched += 1
        print(f"[{i}/{len(plan)}] launched {m['phenotype']}/{m['label']}  run={run_id}")
        time.sleep(0.3)  # gentle on the Batch submit API
    print(f"\n# Done — launched {launched} meta-analyses.")


if __name__ == "__main__":
    main()
