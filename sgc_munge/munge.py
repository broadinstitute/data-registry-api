"""Streaming per-file munge: harmonise a GWAS file against GRCh38 (gwas2vcf method).

Core only: reads the input line by line (never a whole-file DataFrame) and writes
``standardised.tsv.gz``, ``ref_mismatch.tsv.gz`` (only when non-empty) and
``munge.json`` into ``out_dir``.

Two passes around one bulk dbSNP query. Pass 1 filters, orients and normalises every
row and stages it on disk per contig, collecting the positions to look up. One
``bcftools query --targets-file`` over dbSNP then writes the records at those positions
per contig. Pass 2 walks the contigs in canonical order (1..22, X, Y), joins each
contig's records to its rows, de-duplicates and writes the output. Scratch files live
in ``out_dir/.work``, which is removed when the munge ends.
"""
import gzip
import json
import math
import os
import resource
import shutil
import signal
import sys
import tempfile
import time
import traceback
from contextlib import ExitStack, suppress
from typing import NamedTuple

import boto3
import click

from dataregistry.api import sgc_munge_results as munge_db
from sgc_ma.filters import info_ok, maf_ok
from sgc_munge import rsid, staging
from sgc_munge.reference import (PADDING, Reference, align_to_reference, normalize_indel,
                                 pick_rsid, snp_id)
from sgc_munge.rows import ColumnIndex, ParsedRow, RowError, parse_row

OUTPUT_COLUMNS = ["SNP_ID", "Chromosome", "Position", "Effect_allele", "Non-effect_allele",
                  "Beta", "SE", "P-value", "Effect_AF", "Imp_Quality", "N_CAS", "N_CON", "RSID"]
MISMATCH_COLUMNS = ["Chromosome", "Position", "Effect_allele", "Non-effect_allele",
                    "Beta", "SE", "P-value", "Effect_AF", "Imp_Quality", "Ref_seq"]
ENGINE = "sgc_munge 1.1 (gwas2vcf-method)"

NA = "NA"
_DROP_KINDS = ("chrom", "invalid", "maf", "info", "ref_mismatch", "dup")
_MAX_PAD_DOUBLINGS = 4  # left window for indel normalisation: PADDING .. PADDING * 2**4 bp
_PRESCAN_MAX_SNVS = 200_000
# Above this minority-orientation SNV fraction (purity), an indel whose alleles both match
# the reference is oriented by dbSNP (see _resolve_tie) instead of the file-level
# prefer. Low on purpose: only a perfectly consistent file (purity ~0) relies on prefer
# alone; real QA files with a sliver of effect=REF coding (SGC_GNH ~0.008) get the tie-break.
_TIE_BREAK_MIN_PURITY = 0.001
# Wrong-build guard: over at least this many pre-scanned SNVs, one allele must be the
# GRCh38 base for at least this fraction.
_BUILD_CHECK_MIN_SNVS = 1000
_BUILD_CHECK_MIN_RATE = 0.95


class _Hit(NamedTuple):
    ref: str
    alt: str
    swapped: bool
    ambiguous: bool  # indel whose alleles both match the reference at their own lengths


def _seconds_since(started: float) -> float:
    return round(time.monotonic() - started, 3)


def _log_phase(phase: str, seconds: float, **counts) -> None:
    """One progress line per phase on stderr (Batch logs otherwise show nothing for ~20 min)."""
    detail = " ".join(f"{k}={v}" for k, v in counts.items())
    print(f"[munge] {phase}: {detail} {seconds:.1f}s", file=sys.stderr, flush=True)


def _open_text(path: str):
    return gzip.open(path, "rt") if path.endswith(".gz") else open(path)


def _open_gz_out(path: str, header: list[str]):
    fh = gzip.open(path, "wt", compresslevel=6)
    _write(fh, header)
    return fh


def _write(fh, values: list[str]) -> None:
    fh.write("\t".join(values) + "\n")


def _split(line: str) -> list[str]:
    return line.rstrip("\r\n").split("\t")


def _count_str(n: int | None) -> str:
    return NA if n is None else str(int(n))


def _reported(fields: list[str], idx: ColumnIndex, row: ParsedRow) -> list[str]:
    """Chromosome .. Imp_Quality as reported: numbers are kept as the input strings, except
    Position, which is written as the parsed integer (``1.2e+02`` -> ``120``)."""
    def optional(i, value):
        return NA if math.isnan(value) else fields[i].strip()

    return [row.chrom, str(row.pos), row.ea, row.oa,
            fields[idx.beta].strip(), fields[idx.se].strip(), fields[idx.pvalue].strip(),
            optional(idx.eaf, row.eaf), optional(idx.info, row.info)]


def _prescan_orientation(path: str, idx: ColumnIndex, ref: Reference,
                         max_rows: int = _PRESCAN_MAX_SNVS) -> dict:
    """Over the first ``max_rows`` parseable SNVs, count which allele is the reference base.

    The majority orientation (``prefer``) decides indels whose alleles both match the
    reference. ``purity`` is the minority fraction, ``min(n_oa_is_ref, n_ea_is_ref) / n_snv``
    (0 without SNVs): above ``_TIE_BREAK_MIN_PURITY`` the file mixes orientations and
    such indels are first offered to dbSNP.
    """
    n_snv = n_oa_is_ref = n_ea_is_ref = 0
    with _open_text(path) as src:
        src.readline()  # header
        for line in src:
            if n_snv >= max_rows:
                break
            try:
                row = parse_row(_split(line), idx)
            except RowError:
                continue
            if len(row.ea) != 1 or len(row.oa) != 1:
                continue
            n_snv += 1
            base = ref.fetch(row.chrom, row.pos - 1, row.pos)
            n_oa_is_ref += row.oa == base
            n_ea_is_ref += row.ea == base
    return {"n_snv": n_snv, "n_oa_is_ref": n_oa_is_ref, "n_ea_is_ref": n_ea_is_ref,
            "prefer": "oa" if n_oa_is_ref >= n_ea_is_ref else "ea",
            "purity": min(n_oa_is_ref, n_ea_is_ref) / n_snv if n_snv else 0.0}


def _check_build(prescan: dict) -> None:
    """Raise ValueError when too few pre-scanned SNVs carry the GRCh38 base (wrong build)."""
    n_snv = prescan["n_snv"]
    if n_snv < _BUILD_CHECK_MIN_SNVS:
        return
    rate = (prescan["n_oa_is_ref"] + prescan["n_ea_is_ref"]) / n_snv
    if rate < _BUILD_CHECK_MIN_RATE:
        raise ValueError(f"reference match rate {rate:.3f} over {n_snv} SNVs suggests the "
                         f"file is not GRCh38 (expected >= {_BUILD_CHECK_MIN_RATE})")


def _align(ref: Reference, row: ParsedRow, prefer: str) -> tuple[_Hit | None, str]:
    """Orient the alleles as (REF, ALT); also return the bases under the non-effect allele.

    Equal-length alleles cannot both match one window, so the gwas2vcf oa-first test
    decides them. Indel alleles share the anchor base, so both can match at their own
    lengths (every deletion, and insertions that copy the following bases); the hit is
    then ``ambiguous`` and oriented by the file's ``prefer`` (from the pre-scan), which
    dbSNP may override for mixed-orientation files (``_tie_readings`` / ``_resolve_tie``).
    """
    start0 = row.pos - 1
    oa_seq = ref.fetch(row.chrom, start0, start0 + len(row.oa))
    if len(row.ea) == len(row.oa):
        hit = align_to_reference(row.ea, row.oa, oa_seq)
        return (None if hit is None else _Hit(*hit, ambiguous=False)), oa_seq
    oa_is_ref = row.oa == oa_seq
    ea_is_ref = row.ea == ref.fetch(row.chrom, start0, start0 + len(row.ea))
    if not (oa_is_ref or ea_is_ref):
        return None, oa_seq
    ambiguous = oa_is_ref and ea_is_ref
    swapped = (prefer == "ea") if ambiguous else ea_is_ref
    REF, ALT = (row.ea, row.oa) if swapped else (row.oa, row.ea)
    return _Hit(REF, ALT, swapped, ambiguous), oa_seq


def _normalise(ref: Reference, chrom: str, pos: int, REF: str,
               ALT: str) -> tuple[int, str, str, bool]:
    """Left-align/trim indels against the FASTA; SNVs pass through unchanged.

    The left window doubles (up to ``_MAX_PAD_DOUBLINGS`` times) while the alignment
    runs into its start. If it still cannot be anchored (or reaches the contig start
    with an empty allele), the reported (pos, REF, ALT) is kept and flagged exhausted.
    """
    if len(REF) == 1 and len(ALT) == 1:
        return pos, REF, ALT, False
    pad = PADDING
    for _ in range(_MAX_PAD_DOUBLINGS + 1):
        start0 = max(0, pos - 1 - pad)
        seq = ref.fetch(chrom, start0, pos - 1 + len(REF) + PADDING)
        np0, norm_ref, norm_alt = normalize_indel(seq, pos - 1 - start0, REF, ALT)
        if norm_ref and norm_alt and (np0 > 0 or start0 == 0):
            return start0 + np0 + 1, norm_ref, norm_alt, False
        if start0 == 0:
            break
        pad *= 2
    return pos, REF, ALT, True


def _tie_readings(ref: Reference, row: ParsedRow,
                  prefer: str) -> tuple[staging.Reading, staging.Reading]:
    """Both readings of an ambiguous indel, ``prefer``'s first: oa as REF (not swapped) and
    ea as REF (swapped), each normalised with its own REF/ALT. ``_resolve_tie`` picks one
    in pass 2, once the dbSNP records at both positions are known.
    """
    oa_is_ref = staging.Reading(False, *_normalise(ref, row.chrom, row.pos, row.oa, row.ea))
    ea_is_ref = staging.Reading(True, *_normalise(ref, row.chrom, row.pos, row.ea, row.oa))
    return (ea_is_ref, oa_is_ref) if prefer == "ea" else (oa_is_ref, ea_is_ref)


def _resolve_tie(staged: staging.StagedRow,
                 hits: dict[int, list[rsid.HitRecord]]) -> tuple[staging.Reading, str | None, bool]:
    """Orient an ambiguous indel by dbSNP: look each reading up with its own REF/ALT. If
    exactly one has a dbSNP allele match it wins; otherwise the ``prefer`` reading
    (``primary``) is kept. Returns (reading, rsid, resolved).
    """
    looked_up = [(reading, pick_rsid(hits.get(reading.pos, ()), reading.ref, reading.alt))
                 for reading in (staged.primary, staged.alternate)]
    known = [pair for pair in looked_up if pair[1] is not None]
    if len(known) == 1:
        return known[0][0], known[0][1], True
    reading, rs_id = looked_up[0]
    return reading, rs_id, False


def munge_file(in_path: str, column_mapping: dict, ref: Reference, out_dir: str, *,
               cases: int | None, controls: int | None, maf_min: float = 0.005,
               info_min: float = 0.3, ref_version: str = "unknown",
               file_id: str | None = None) -> dict:
    t0 = time.monotonic()
    # fail fast, before any output or scratch exists, when the dbSNP lookup cannot run
    bcftools_version = rsid.check_bcftools() if ref.dbsnp_path else None
    os.makedirs(out_dir, exist_ok=True)
    mismatch_path = os.path.join(out_dir, "ref_mismatch.tsv.gz")
    if os.path.exists(mismatch_path):  # never leave a stale sidecar behind
        os.remove(mismatch_path)
    work_dir = os.path.join(out_dir, ".work")
    # always a fresh scratch dir: query_dbsnp does not clear stale per-contig hit files
    shutil.rmtree(work_dir, ignore_errors=True)
    os.makedirs(work_dir)
    targets_path = os.path.join(work_dir, "targets.tsv")
    sorted_path = os.path.join(work_dir, "targets.sorted.tsv")
    hits_dir = os.path.join(work_dir, "hits")
    counts_suffix = [_count_str(cases), _count_str(controls)]
    n_dropped = dict.fromkeys(_DROP_KINDS, 0)
    n_in = n_staged = n_out = n_with_rsid = n_swapped = 0
    n_indel_ambiguous = n_indel_norm_exhausted = n_tie_resolved = n_tie_unresolved = 0
    n_hit_positions = 0
    has_eaf = has_info = False
    timings = {}
    stage = None
    try:
        stage = staging.Staging(os.path.join(work_dir, "stage"))
        os.makedirs(hits_dir)
        # pass 1: filter, orient and normalise; stage rows per contig, collect dbSNP targets
        started = time.monotonic()
        with ExitStack() as stack:
            src = stack.enter_context(_open_text(in_path))
            idx = ColumnIndex.from_header(_split(src.readline()), column_mapping)
            prescan = _prescan_orientation(in_path, idx, ref)
            _check_build(prescan)
            prefer = prescan["prefer"]
            break_ties = prescan["purity"] > _TIE_BREAK_MIN_PURITY
            targets = stack.enter_context(open(targets_path, "w"))
            mismatch = None
            for line in src:
                if not line.strip():
                    continue
                n_in += 1
                fields = _split(line)
                try:
                    row = parse_row(fields, idx)
                except RowError as e:
                    n_dropped[e.kind] += 1
                    continue
                has_eaf = has_eaf or not math.isnan(row.eaf)
                has_info = has_info or not math.isnan(row.info)
                if not maf_ok(row.eaf, maf_min):
                    n_dropped["maf"] += 1
                    continue
                if not info_ok(row.info, info_min):
                    n_dropped["info"] += 1
                    continue
                hit, ref_seq = _align(ref, row, prefer)
                reported = _reported(fields, idx, row)
                if hit is None:
                    n_dropped["ref_mismatch"] += 1
                    if mismatch is None:
                        mismatch = stack.enter_context(
                            _open_gz_out(mismatch_path, MISMATCH_COLUMNS))
                    _write(mismatch, reported + [ref_seq or NA])
                    continue
                if hit.ambiguous and break_ties:  # dbSNP picks one of the readings in pass 2
                    primary, alternate = _tie_readings(ref, row, prefer)
                else:
                    primary = staging.Reading(
                        hit.swapped, *_normalise(ref, row.chrom, row.pos, hit.ref, hit.alt))
                    alternate = None
                stage.write(row.chrom,
                            staging.StagedRow(hit.ambiguous, primary, alternate, reported))
                n_staged += 1
                if row.chrom in ref.dbsnp_contigs:
                    targets.write(f"{row.chrom}\t{primary.pos}\n")
                    if alternate is not None:
                        targets.write(f"{row.chrom}\t{alternate.pos}\n")
        stage.close()
        timings["pass1_s"] = _seconds_since(started)
        _log_phase("pass1", timings["pass1_s"], n_in=n_in, n_staged=n_staged)

        started = time.monotonic()
        n_targets = rsid.sort_targets(targets_path, sorted_path, work_dir)
        timings["sort_s"] = _seconds_since(started)
        _log_phase("sort", timings["sort_s"], n_targets=n_targets)

        n_records, timings["bcftools_s"] = 0, 0.0
        if n_targets:  # bcftools never runs without targets (or without dbSNP)
            started = time.monotonic()
            n_records = sum(rsid.query_dbsnp(ref.dbsnp_path, sorted_path, hits_dir).values())
            timings["bcftools_s"] = _seconds_since(started)
        _log_phase("bcftools", timings["bcftools_s"], n_records=n_records)

        # pass 2: per contig, join the dbSNP records, de-duplicate and write
        started = time.monotonic()
        with _open_gz_out(os.path.join(out_dir, "standardised.tsv.gz"), OUTPUT_COLUMNS) as out:
            for chrom in stage.contigs():
                # SNP_IDs begin with the contig, so dedup per contig (one contig's IDs in memory)
                seen_ids: set[str] = set()
                hits = rsid.load_hits(os.path.join(hits_dir, f"{chrom}.tsv"))
                n_hit_positions += len(hits)
                for staged in stage.rows(chrom):
                    tie_break = staged.alternate is not None
                    if tie_break:
                        reading, rs_id, resolved = _resolve_tie(staged, hits)
                    else:
                        reading = staged.primary
                        rs_id = pick_rsid(hits.get(reading.pos, ()), reading.ref, reading.alt)
                    sid = snp_id(chrom, reading.pos, reading.ref, reading.alt)
                    if sid in seen_ids:
                        n_dropped["dup"] += 1
                        continue
                    seen_ids.add(sid)
                    if tie_break:
                        n_tie_resolved += resolved
                        n_tie_unresolved += not resolved
                    n_out += 1
                    n_swapped += reading.swapped
                    n_indel_ambiguous += staged.ambiguous
                    n_indel_norm_exhausted += reading.exhausted
                    n_with_rsid += rs_id is not None
                    _write(out, [sid, *staged.reported, *counts_suffix, rs_id or NA])
        timings["pass2_s"] = _seconds_since(started)
        _log_phase("pass2", timings["pass2_s"], n_out=n_out, n_with_rsid=n_with_rsid)
    finally:
        try:
            if stage is not None:
                # still open only when pass 1 failed: keep that error, not a close() failure
                with suppress(OSError):
                    stage.close()
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)  # never raises over the original error

    assert n_in == n_out + sum(n_dropped.values()), "munge row accounting is inconsistent"
    result = {
        "file_id": file_id, "n_in": n_in, "n_out": n_out, "n_with_rsid": n_with_rsid,
        "n_swapped": n_swapped, "n_dropped": n_dropped, "has_eaf": has_eaf,
        "has_info": has_info, "cases": cases, "controls": controls, "maf_min": maf_min,
        "info_min": info_min, "ref_version": ref_version, "engine": ENGINE,
        "column_mapping": column_mapping, "orientation_prescan": prescan,
        "n_indel_ambiguous": n_indel_ambiguous,
        "n_indel_tie_dbsnp_resolved": n_tie_resolved, "n_indel_tie_unresolved": n_tie_unresolved,
        "n_indel_norm_exhausted": n_indel_norm_exhausted,
        "rsid_lookup": {"n_targets": n_targets, "n_hit_positions": n_hit_positions,
                        "bcftools": bcftools_version},
        "timings": timings,
        "elapsed_s": round(time.monotonic() - t0, 3),
        "peak_rss_mb": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
    }
    with open(os.path.join(out_dir, "munge.json"), "w") as fh:
        json.dump(result, fh, indent=2)
    return result


REF_FILES = ["GRCh38.primary.fa.gz", "GRCh38.primary.fa.gz.fai", "GRCh38.primary.fa.gz.gzi",
             "dbsnp155.grch38.slim.vcf.gz", "dbsnp155.grch38.slim.vcf.gz.tbi", "MANIFEST.json"]


def _get_engine():
    """Same Secrets Manager-backed engine the API uses; imported lazily so tests can mock it."""
    from dataregistry.api.db import DataRegistryReadWriteDB
    return DataRegistryReadWriteDB().get_engine()


def parse_s3_prefix(prefix: str) -> tuple[str, str]:
    """``s3://bucket/key/`` (trailing slash optional) -> ``("bucket", "key/")``."""
    prefix = prefix.rstrip("/") + "/"
    rest = prefix[len("s3://"):] if prefix.startswith("s3://") else prefix
    bucket, _, key = rest.partition("/")
    return bucket, key


def download_refs(s3, ref_prefix: str, dest_dir: str) -> tuple[str, str, str]:
    bucket, key_prefix = parse_s3_prefix(ref_prefix)
    for name in REF_FILES:
        s3.download_file(bucket, key_prefix + name, os.path.join(dest_dir, name))
    with open(os.path.join(dest_dir, "MANIFEST.json")) as fh:
        ref_version = json.load(fh)["ref_version"]
    return (os.path.join(dest_dir, "GRCh38.primary.fa.gz"),
            os.path.join(dest_dir, "dbsnp155.grch38.slim.vcf.gz"), ref_version)


def _upload(s3, local_path: str, bucket: str, key: str) -> None:
    content_type = "application/json" if key.endswith(".json") else "application/gzip"
    s3.upload_file(local_path, bucket, key, ExtraArgs={"ContentType": content_type})


def _raise_on_sigterm(signum, frame):
    raise RuntimeError("terminated by SIGTERM (Batch timeout or Spot reclaim)")


def run_one(**kwargs) -> None:
    """Munge one file (see ``_run_one``) with SIGTERM turned into an exception, so the row
    is marked FAILED before Batch kills the container. (As PID 1 the worker would
    otherwise ignore SIGTERM and die silently on the follow-up SIGKILL.)"""
    previous = signal.signal(signal.SIGTERM, _raise_on_sigterm)
    try:
        _run_one(**kwargs)
    finally:
        signal.signal(signal.SIGTERM, previous)


def _run_one(*, s3_path: str, column_mapping: dict, bucket: str, file_id: str,
             output_prefix: str, ref_prefix: str, cases: int | None, controls: int | None,
             maf_min: float, info_min: float, batch_job_id: str | None) -> None:
    engine = _get_engine()
    munge_db.update_sgc_munge_result(engine, file_id, status="RUNNING", batch_job_id=batch_job_id,
                                     clear_error=True)
    s3 = boto3.client("s3", region_name="us-east-1")
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            ref_dir = os.path.join(tmpdir, "ref")
            out_dir = os.path.join(tmpdir, "out")
            os.makedirs(ref_dir)
            fasta, dbsnp, ref_version = download_refs(s3, ref_prefix, ref_dir)
            local = os.path.join(tmpdir, os.path.basename(s3_path))  # keep .gz suffix
            s3.download_file(bucket, s3_path, local)
            with Reference(fasta, dbsnp) as ref:
                try:
                    result = munge_file(local, column_mapping, ref, out_dir, cases=cases,
                                        controls=controls, maf_min=maf_min, info_min=info_min,
                                        ref_version=ref_version, file_id=file_id)
                except ValueError as e:  # bad mapping / missing columns / wrong genome build
                    munge_db.update_sgc_munge_result(engine, file_id, status="FAILED",
                                                  error_message=str(e)[:1500])
                    raise SystemExit(2)
            keys = {}
            for name, field in (("standardised.tsv.gz", "standardised_s3_key"),
                                ("munge.json", "munge_json_s3_key"),
                                ("ref_mismatch.tsv.gz", "ref_mismatch_s3_key")):
                local_out = os.path.join(out_dir, name)
                if os.path.exists(local_out):
                    keys[field] = f"{output_prefix}/{name}"
                    _upload(s3, local_out, bucket, keys[field])
                else:
                    keys[field] = None
        munge_db.update_sgc_munge_result(
            engine, file_id, status="SUCCEEDED",
            n_in=result["n_in"], n_out=result["n_out"], n_with_rsid=result["n_with_rsid"],
            n_swapped=result["n_swapped"], n_dropped=result["n_dropped"],
            has_eaf=result["has_eaf"], has_info=result["has_info"],
            maf_min=maf_min, info_min=info_min, ref_version=ref_version,
            engine=result["engine"], **keys)
    except SystemExit:
        raise
    except Exception as e:
        munge_db.update_sgc_munge_result(
            engine, file_id, status="FAILED",
            error_message=f"{type(e).__name__}: {e}\n{traceback.format_exc()[:1500]}")
        raise


def _opt_int(value: str | None) -> int | None:
    if value is None or value.strip() in ("", "NA", "None"):
        return None
    return int(float(value))


@click.command()
@click.option("--s3-path", required=True)
@click.option("--column-mapping", required=True, help="JSON string")
@click.option("--bucket", required=True)
@click.option("--file-id", required=True)
@click.option("--output-prefix", required=True)
@click.option("--ref-prefix", required=True, help="s3://bucket/prefix/ holding the reference files")
@click.option("--cases", default=None)
@click.option("--controls", default=None)
@click.option("--maf-min", type=float, default=0.005)
@click.option("--info-min", type=float, default=0.3)
def main(s3_path, column_mapping, bucket, file_id, output_prefix, ref_prefix,
         cases, controls, maf_min, info_min):
    run_one(s3_path=s3_path, column_mapping=json.loads(column_mapping), bucket=bucket,
            file_id=file_id, output_prefix=output_prefix, ref_prefix=ref_prefix,
            cases=_opt_int(cases), controls=_opt_int(controls), maf_min=maf_min,
            info_min=info_min, batch_job_id=os.environ.get("AWS_BATCH_JOB_ID"))


if __name__ == "__main__":
    main()
