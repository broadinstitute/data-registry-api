"""Bottom-line meta-analysis worker: METAL over the per-cohort munged files.

Same CLI as sgc_ma/run_ma.py (--run-id --bucket [--local-out]) so the Batch
job-def Command and sgc_ma/submit_ma_batch.py work unchanged. Inputs are the
munge stage's standardised.tsv.gz files (one per cohort, already aligned to
the GRCh38 reference and MAF/INFO-filtered); METAL does the combining, and
sgc_metal.summarize derives plots/top loci/summary from its .tbl.

Work layout (work_dir):
    work/<dataset>_<file_id>.tsv.gz   downloaded munged inputs (deleted after the last pass reading them)
    metal.txt, metal.log              script and combined stdout/stderr
    <prefix>1.tbl, <prefix>1.tbl.info METAL outputs (relative OUTFILE, cwd=work_dir)
    freq/                             frequency pass (mixed EAF only): metal.txt, metal.log,
                                      <prefix>freq_1.tbl[.info]; PROCESS ../work/...
    metal_final.tbl                   pass-1 table with the frequency columns replaced (the raw
                                      pass-1/pass-2 tables are deleted once it is written)

Sizing (METAL 2020-05-05, measured): METAL keeps every distinct marker in memory, ~500-550 B
each. The main pass (ANALYZE HETEROGENEITY, which re-reads every input) costs ~12 us per input
line; the frequency pass is a plain ANALYZE, ~7 us per input line.

Allele frequencies: METAL averages Freq1 over the inverse-variance weights of every
input and reads a missing (NA) frequency as 0, so a cohort without EAF biases
Freq1/MinFreq/MaxFreq. When only some used cohorts report EAF, a second METAL pass over
just those cohorts supplies the four frequency columns (joined on MarkerName; final
rows are in MarkerName order). With no EAF cohort they are NA. Every other column
always comes from pass 1.
"""
import gzip
import json
import math
import os
import re
import resource
import shutil
import signal
import subprocess
import tempfile
import time
from collections import deque

import boto3
import click
import pandas as pd

from sgc_ma import select as sel
from sgc_metal.script import outfile_prefix, render_script, sanitise, tbl_path
from sgc_metal.summarize import (FREQ_NONE, FREQ_PASS1, FREQ_PASS2, build_caveats, build_summary,
                                 summarize_tbl)
from sgc_metal.tbl import TBL_COLUMNS

METAL_BIN = os.environ.get("METAL_BIN", "metal")
# Written by the Dockerfile (git rev-parse HEAD of the METAL checkout the binary was built from).
METAL_COMMIT_FILE = "/usr/local/share/metal_commit.txt"
UNKNOWN_VERSION = "METAL (unknown version)"
DEFAULT_MAF_MIN, DEFAULT_INFO_MIN = 0.005, 0.3
_RELEASED_RE = re.compile(r"released on (\S+)")
LOG_TAIL_LINES = 40
ERROR_MESSAGE_LIMIT = 4000
# METAL exits 0 after skipping an input (missing column, unreadable file); these log
# lines are the only signal that the meta-analysis silently lost a cohort.
_LOG_ERROR_RE = re.compile(r"^## (ERROR|Failed to open)")
# Per-file lines of METAL's first read of each PROCESS file (Main.cpp ProcessFile). After
# MAXWARNINGS (default 20) "Bad alleles" lines per file it prints one summary line instead.
_PROCESSING_RE = re.compile(r"^## Processing file '(.*)'$")
_PROCESSED_RE = re.compile(r"^## Processed (\d+) markers")
_BAD_ALLELES_RE = re.compile(r"^## WARNING: Bad alleles for marker ")
_MORE_BAD_ALLELES_RE = re.compile(r"^## WARNING: Allele names don't match previous occurences at (\d+) additional")
# What follows re-reads the files (heterogeneity) or analyses; no per-file counts there.
_END_OF_FIRST_READ = ("## Running second pass analysis", "## Executing meta-analysis")
_DIRECTION_COL = TBL_COLUMNS.index("Direction")
_A1, _A2 = TBL_COLUMNS.index("Allele1"), TBL_COLUMNS.index("Allele2")
_F1, _FSE, _FMIN, _FMAX = (TBL_COLUMNS.index(c) for c in ("Freq1", "FreqSE", "MinFreq", "MaxFreq"))
_NA = b"NA"
_HEADER_KEY = TBL_COLUMNS[0].encode()  # "MarkerName": the header line sorts in with the data rows

UPLOAD_NAMES = ["metal.tbl.gz", "metal.tbl.info", "metal.log", "metal.txt", "manhattan.png", "qq.png",
                "summary.json", "summary.tsv", "top_loci.tsv",
                # frequency pass; only produced when some (not all) used cohorts report EAF
                "metal_freq.tbl.gz", "metal_freq.tbl.info", "metal_freq.log", "metal_freq.txt"]

# Logs/scripts saved to out_dir even when METAL fails; main uploads them for a FAILED run.
LOG_NAMES = ["metal.log", "metal.txt", "metal_freq.log", "metal_freq.txt"]

# Keyed on the last extension only: "metal.tbl.info" -> ".info".
CONTENT_TYPES = {".png": "image/png", ".json": "application/json", ".tsv": "text/tab-separated-values",
                 ".gz": "application/gzip", ".info": "text/plain", ".log": "text/plain", ".txt": "text/plain"}


class MetalError(RuntimeError):
    pass


def _metal_commit() -> str | None:
    try:
        with open(METAL_COMMIT_FILE) as fh:
            return fh.read().strip() or None
    except OSError:
        return None


def metal_version() -> str:
    """_banner_version() plus " (commit <sha>)" when the image recorded the METAL commit."""
    version = _banner_version()
    commit = _metal_commit()
    return f"{version} (commit {commit})" if commit else version


def _banner_version() -> str:
    """The first banner line mentioning METAL, else "METAL <date>" from the
    "This version released on <date>" line (the 2020-05-05 build's banner never
    says METAL). METAL prints a banner, then reads a script from stdin; stdin is
    closed so it exits straight away. Never raises."""
    try:
        res = subprocess.run([METAL_BIN], stdin=subprocess.DEVNULL, capture_output=True, text=True,
                             errors="replace", timeout=30)
    except (OSError, subprocess.SubprocessError):
        return UNKNOWN_VERSION
    lines = res.stdout.splitlines()
    for line in lines:
        if "METAL" in line:
            return line.strip()
    for line in lines:
        m = _RELEASED_RE.search(line)
        if m:
            return f"METAL {m.group(1)}"
    return UNKNOWN_VERSION


def _log_tail(log_path: str, n: int = LOG_TAIL_LINES) -> str:
    try:
        with open(log_path, errors="replace") as fh:
            return "".join(deque(fh, maxlen=n))
    except OSError:
        return ""


def run_metal_script(script_text: str, work_dir: str) -> str:
    """Write work_dir/metal.txt and run METAL on it with cwd=work_dir (so the
    script's relative PROCESS/OUTFILE paths resolve there). stdout+stderr go to
    work_dir/metal.log, whose path is returned."""
    os.makedirs(work_dir, exist_ok=True)
    with open(os.path.join(work_dir, "metal.txt"), "w") as fh:
        fh.write(script_text)
    log_path = os.path.join(work_dir, "metal.log")
    with open(log_path, "w") as log:
        try:
            rc = subprocess.run([METAL_BIN, "metal.txt"], cwd=work_dir, stdin=subprocess.DEVNULL,
                                stdout=log, stderr=subprocess.STDOUT).returncode
        except OSError as e:
            raise MetalError(f"could not run {METAL_BIN}: {e}") from e
    if rc != 0:
        raise MetalError(f"metal exited {rc}; log tail:\n{_log_tail(log_path)}")
    return log_path


def check_metal_output(log_path: str, tbl: str, n_inputs: int) -> None:
    """Raise MetalError unless METAL used every input: no ERROR/Failed-to-open log
    line, both .tbl and .tbl.info written, and a Direction one char per input."""
    with open(log_path, errors="replace") as fh:
        bad = [line.rstrip("\n") for line in fh if _LOG_ERROR_RE.match(line)]
    if bad:
        raise MetalError(f"metal dropped input(s): {'; '.join(bad)}; log tail:\n{_log_tail(log_path)}")
    missing = [os.path.basename(p) for p in (tbl, tbl + ".info") if not os.path.exists(p)]
    if missing:
        raise MetalError(f"metal produced no {', '.join(missing)}; log tail:\n{_log_tail(log_path)}")
    with open(tbl) as fh:
        fh.readline()  # header
        first = fh.readline().rstrip("\r\n")
    if first:
        width = len(first.split("\t")[_DIRECTION_COL])
        if width != n_inputs:
            raise MetalError(f"Direction width {width} != {n_inputs} inputs; log tail:\n{_log_tail(log_path)}")


def parse_process_log(log_path: str) -> dict[str, dict]:
    """{PROCESS path: {"n_markers_processed", "n_allele_mismatch"}} from METAL's first read of
    each file. n_markers_processed is METAL's "Processed N markers" (rows it kept);
    n_allele_mismatch counts the rows it dropped as "Bad alleles" (allele strings differing
    from the marker's first occurrence, e.g. an indel reported as TA/T instead of AT/A)."""
    out, current = {}, None
    with open(log_path, errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line.startswith(_END_OF_FIRST_READ):
                break
            m = _PROCESSING_RE.match(line)
            if m:
                current = out.setdefault(m.group(1), {"n_markers_processed": None, "n_allele_mismatch": 0})
            elif current is None:
                continue
            elif _BAD_ALLELES_RE.match(line):
                current["n_allele_mismatch"] += 1
            elif m := _MORE_BAD_ALLELES_RE.match(line):
                current["n_allele_mismatch"] += int(m.group(1))
            elif m := _PROCESSED_RE.match(line):
                current["n_markers_processed"] = int(m.group(1))
                current = None
    return out


def truncate_error(msg: str, limit: int = ERROR_MESSAGE_LIMIT) -> str:
    """Cap msg at limit chars keeping head and tail (METAL's error line is usually at the end)."""
    if len(msg) <= limit:
        return msg
    marker = f"\n...[{len(msg) - limit} chars omitted]...\n"
    keep = limit - len(marker)
    head = keep // 2
    return msg[:head] + marker + msg[len(msg) - (keep - head):]


def _gzip_copy(src: str, dst: str) -> None:
    with open(src, "rb") as fin, gzip.open(dst, "wb", compresslevel=6) as fout:
        shutil.copyfileobj(fin, fout)


def _set_freqs(fields: list, freqs) -> None:
    fields[_F1], fields[_FSE], fields[_FMIN], fields[_FMAX] = freqs


def _flip(v: bytes) -> bytes:
    try:
        return b"%.4f" % (1.0 - float(v))
    except ValueError:
        return _NA


def _pass2_freqs(p1: list, p2: list | None):
    """Pass-2 frequency columns oriented to pass 1's Allele1; NA when pass 2 lacks
    the marker (no EAF cohort carries it) or its alleles do not match."""
    if p2 is None:
        return _NA, _NA, _NA, _NA
    a = (p1[_A1].lower(), p1[_A2].lower())
    b = (p2[_A1].lower(), p2[_A2].lower())
    if b == a:
        return p2[_F1], p2[_FSE], p2[_FMIN], p2[_FMAX]
    if b == a[::-1]:  # swapped: frequency of the other allele; min/max mirror
        return _flip(p2[_F1]), p2[_FSE], _flip(p2[_FMAX]), _flip(p2[_FMIN])
    return _NA, _NA, _NA, _NA


def _split(line: bytes) -> list:
    return line.rstrip(b"\r\n").split(b"\t")


def _sorted_copy(path: str, tmp_dir: str) -> str:
    """LC_ALL=C sort of a .tbl on MarkerName (the header sorts in with the rows; readers
    skip it). Byte order matches Python's bytes comparison, which the merge relies on."""
    out = path + ".sorted"
    res = subprocess.run(["sort", "-t", "\t", "-k1,1", "-T", tmp_dir, "-o", out, path],
                         env={**os.environ, "LC_ALL": "C"}, capture_output=True, text=True, errors="replace")
    if res.returncode != 0:
        raise MetalError(f"sort of {os.path.basename(path)} failed (exit {res.returncode}): {res.stderr.strip()}")
    return out


def join_frequencies(pass1_tbl: str, pass2_tbl: str, out_path: str, tmp_dir: str) -> None:
    """Streaming merge-join: pass-1 rows with Freq1/FreqSE/MinFreq/MaxFreq taken from the
    pass-2 row of the same MarkerName (flipped when its alleles are swapped), NA when absent.
    Pass-2-only markers are dropped. Output keeps the header, rows in MarkerName order."""
    with open(pass1_tbl, "rb") as fh:
        header = fh.readline()
    s1, s2 = _sorted_copy(pass1_tbl, tmp_dir), _sorted_copy(pass2_tbl, tmp_dir)
    try:
        with open(s1, "rb") as f1, open(s2, "rb") as f2, open(out_path, "wb") as out:
            out.write(header)
            rows2 = (r for r in (_split(ln) for ln in f2 if ln.strip()) if r[0] != _HEADER_KEY)
            cur2 = next(rows2, None)
            for line in f1:
                fields = _split(line)
                key = fields[0]
                if not line.strip() or key == _HEADER_KEY:
                    continue
                while cur2 is not None and cur2[0] < key:
                    cur2 = next(rows2, None)
                _set_freqs(fields, _pass2_freqs(fields, cur2 if cur2 is not None and cur2[0] == key else None))
                out.write(b"\t".join(fields) + b"\n")
    finally:
        for p in (s1, s2):
            if os.path.exists(p):
                os.remove(p)


def blank_frequencies(tbl: str, out_path: str) -> None:
    """Copy a .tbl with the four frequency columns set to NA (no cohort reports EAF)."""
    with open(tbl, "rb") as fin, open(out_path, "wb") as out:
        out.write(fin.readline())
        for line in fin:
            if not line.strip():
                continue
            fields = _split(line)
            _set_freqs(fields, (_NA, _NA, _NA, _NA))
            out.write(b"\t".join(fields) + b"\n")


def _timed_metal(script_text: str, work_dir: str) -> tuple[str, dict]:
    """run_metal_script plus its wall time and RUSAGE_CHILDREN's ru_maxrss afterwards. That
    is the peak over every child waited for so far, so a later pass reports
    max(earlier passes, itself)."""
    t0 = time.monotonic()
    log_path = run_metal_script(script_text, work_dir)
    elapsed = time.monotonic() - t0
    rss_kib = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss  # KiB on Linux
    return log_path, {"metal_elapsed_s": round(elapsed, 1), "metal_peak_rss_mb": round(rss_kib / 1024, 1)}


def _run_frequency_pass(eaf_paths: list[str], prefix: str, work_dir: str, out_dir: str) -> tuple[str, dict]:
    """METAL (plain ANALYZE: frequencies need no heterogeneity re-read) over only the EAF
    cohorts in work_dir/freq; keeps its raw table as metal_freq.tbl.gz/.info in out_dir and
    returns its .tbl path and run metrics."""
    freq_dir = os.path.join(work_dir, "freq")
    freq_prefix = prefix + "freq_"
    log_path, metrics = _timed_metal(render_script(["../" + p for p in eaf_paths], freq_prefix,
                                                   heterogeneity=False), freq_dir)
    freq_tbl = tbl_path(freq_dir, freq_prefix)
    check_metal_output(log_path, freq_tbl, len(eaf_paths))
    _gzip_copy(freq_tbl, os.path.join(out_dir, "metal_freq.tbl.gz"))
    shutil.copyfile(freq_tbl + ".info", os.path.join(out_dir, "metal_freq.tbl.info"))
    return freq_tbl, metrics


def _save_logs(work_dir: str, out_dir: str) -> None:
    """Copy whichever METAL scripts/logs exist into out_dir (LOG_NAMES)."""
    sources = {"metal.log": "metal.log", "metal.txt": "metal.txt",
               "metal_freq.log": os.path.join("freq", "metal.log"),
               "metal_freq.txt": os.path.join("freq", "metal.txt")}
    for name, rel in sources.items():
        src = os.path.join(work_dir, rel)
        if os.path.exists(src):
            shutil.copyfile(src, os.path.join(out_dir, name))


def _remove(paths) -> None:
    for p in paths:
        if os.path.exists(p):
            os.remove(p)


def _clear_outputs(out_dir: str) -> None:
    """A reused out_dir must not leak a previous run's artifacts (or upload its logs)."""
    _remove(os.path.join(out_dir, name) for name in UPLOAD_NAMES)


def _fmt(v) -> str:
    return "None" if v is None else f"{float(v):g}"


def _same(a, b) -> bool:
    return a is not None and b is not None and math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-12)


def check_thresholds(cohorts: list[dict], maf_min: float, info_min: float) -> None:
    """Raise MetalError unless every munged cohort was filtered with the run's MAF/INFO
    thresholds (the munged files are the only inputs; METAL applies no filter of its own)."""
    bad = [co for co in sorted(cohorts, key=lambda c: c.get("dataset") or "") if co.get("munge_s3_key")
           and not (_same(co.get("munge_maf_min"), maf_min) and _same(co.get("munge_info_min"), info_min))]
    if bad:
        co = bad[0]
        more = f" and {len(bad) - 1} more" if len(bad) > 1 else ""
        raise MetalError(f"run thresholds maf_min={_fmt(maf_min)} info_min={_fmt(info_min)} differ from munged "
                         f"inputs ({co.get('dataset')}: maf_min={_fmt(co.get('munge_maf_min'))} "
                         f"info_min={_fmt(co.get('munge_info_min'))}{more}); re-munge with --force or launch "
                         "with matching thresholds")


def _input_relpath(co: dict) -> str:
    return f"work/{sanitise(co.get('dataset'))}_{co['file_id']}.tsv.gz"


def _has_data_rows(path: str) -> bool:
    with gzip.open(path, "rt") as fh:
        fh.readline()  # header
        return fh.readline().strip() != ""


def _label(phenotype: str, ancestry: str, sex: str) -> str:
    return " ".join([phenotype, ancestry] + ([sex] if sex and sex != "All" else []))


def meta_analyze(cohorts: list[dict], fetch_fn, work_dir: str, out_dir: str, *, phenotype: str,
                 ancestry: str, sex: str, ignored: list[dict], engine_version: str,
                 maf_min: float = DEFAULT_MAF_MIN, info_min: float = DEFAULT_INFO_MIN) -> dict:
    """Download each cohort's munged file (fetch_fn(co, local_path)), run METAL
    over the non-empty ones, and write every UPLOAD_NAMES artifact to out_dir.
    Raises MetalError when the munged inputs were filtered with other thresholds than
    maf_min/info_min, fewer than two inputs are usable, or METAL fails; METAL's scripts and
    logs are in out_dir (LOG_NAMES) either way."""
    os.makedirs(os.path.join(work_dir, "work"), exist_ok=True)
    os.makedirs(out_dir, exist_ok=True)
    _clear_outputs(out_dir)
    check_thresholds(cohorts, maf_min, info_min)  # before any download

    used, skipped, process_paths = [], [], []
    for co in sorted(cohorts, key=lambda c: c.get("dataset") or ""):
        if not co.get("munge_s3_key"):
            skipped.append({**co, "reason": "not munged"})
            continue
        rel = _input_relpath(co)
        local = os.path.join(work_dir, rel)
        fetch_fn(co, local)
        if not _has_data_rows(local):
            skipped.append({**co, "reason": "empty munged file"})
            continue
        used.append(co)
        process_paths.append(rel)
    if len(used) < 2:
        raise MetalError("fewer than 2 munged inputs")

    prefix = outfile_prefix(phenotype, ancestry, sex)
    tbl = tbl_path(work_dir, prefix)
    eaf_paths = [p for p, co in zip(process_paths, used) if co.get("munge_has_eaf")]
    final_tbl = os.path.join(work_dir, "metal_final.tbl")

    def staged(paths):
        return [os.path.join(work_dir, p) for p in paths]

    metal_runs = {"main": None, "freq": None}
    try:
        log_path, metal_runs["main"] = _timed_metal(render_script(process_paths, prefix), work_dir)
        check_metal_output(log_path, tbl, len(process_paths))
        log_stats = parse_process_log(log_path)
        if len(eaf_paths) == len(used):
            freq_source, final_tbl = FREQ_PASS1, tbl
            _remove(staged(process_paths))
        elif not eaf_paths:
            freq_source = FREQ_NONE
            _remove(staged(process_paths))
            blank_frequencies(tbl, final_tbl)
            _remove([tbl])
        else:
            freq_source = FREQ_PASS2
            _remove(staged(p for p in process_paths if p not in eaf_paths))
            freq_tbl, metal_runs["freq"] = _run_frequency_pass(eaf_paths, prefix, work_dir, out_dir)
            _remove(staged(eaf_paths))
            join_frequencies(tbl, freq_tbl, final_tbl, work_dir)
            _remove([tbl, freq_tbl])
    finally:
        _save_logs(work_dir, out_dir)

    _gzip_copy(final_tbl, os.path.join(out_dir, "metal.tbl.gz"))
    shutil.copyfile(tbl + ".info", os.path.join(out_dir, "metal.tbl.info"))

    used = [{**co, "metal_n_markers_processed": log_stats.get(p, {}).get("n_markers_processed"),
             "metal_n_allele_mismatch": log_stats.get(p, {}).get("n_allele_mismatch")}
            for co, p in zip(used, process_paths)]
    n_allele_mismatch = sum(co["metal_n_allele_mismatch"] or 0 for co in used)
    tbl_stats = summarize_tbl(final_tbl, out_dir, _label(phenotype, ancestry, sex))
    summary = build_summary(cohorts=cohorts, used=used, skipped=skipped, ignored=ignored or [],
                            tbl_stats=tbl_stats, engine_version=engine_version,
                            caveats=build_caveats(freq_source=freq_source, maf_min=maf_min, info_min=info_min,
                                                  n_allele_mismatch=n_allele_mismatch),
                            extra={"freq_source": freq_source, "n_cohorts_with_eaf": len(eaf_paths),
                                   "metal_runs": metal_runs})
    with open(os.path.join(out_dir, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    pd.DataFrame(summary["per_cohort"]).to_csv(os.path.join(out_dir, "summary.tsv"), sep="\t", index=False)
    return summary


def _raise_on_sigterm(signum, frame):
    raise RuntimeError("terminated by SIGTERM (Batch timeout or Spot reclaim)")


def _upload(s3, bucket: str, prefix: str, local_dir: str, names) -> None:
    for name in names:
        p = os.path.join(local_dir, name)
        if os.path.exists(p):
            ctype = CONTENT_TYPES.get(os.path.splitext(name)[1])
            s3.upload_file(p, bucket, f"{prefix}/{name}", ExtraArgs={"ContentType": ctype} if ctype else None)


@click.command()
@click.option("--run-id", required=True)
@click.option("--bucket", required=True)
@click.option("--local-out", default="metal_out", help="local working dir")
def main(run_id, bucket, local_out):
    # SIGTERM (Batch timeout, Spot reclaim) becomes an exception so the run is marked FAILED
    # before the follow-up SIGKILL; as PID 1 the worker would otherwise ignore it.
    previous = signal.signal(signal.SIGTERM, _raise_on_sigterm)
    try:
        _main(run_id, bucket, local_out)
    finally:
        signal.signal(signal.SIGTERM, previous)


def _main(run_id, bucket, local_out):
    # Light DB module, not dataregistry.api.query: keeps fastapi/pegasus out of the image.
    from dataregistry.api import sgc_ma_results as ma_db
    from dataregistry.api.db import DataRegistryReadWriteDB
    engine = DataRegistryReadWriteDB().get_engine()
    run = ma_db.get_sgc_ma_run(engine, run_id)
    if not run:
        raise SystemExit(f"no MA run {run_id}")
    phenotype, ancestry, sex = run["phenotype"], run["ancestry"], run.get("sex", "All")
    file_ids = run.get("dataset_file_ids") or []
    maf_min = DEFAULT_MAF_MIN if run.get("maf_min") is None else float(run["maf_min"])
    info_min = DEFAULT_INFO_MIN if run.get("info_min") is None else float(run["info_min"])
    prefix = f"sgc/ma/{phenotype}/{ancestry}/{run_id}"

    # AWS Batch injects AWS_BATCH_JOB_ID into every job container (None on local
    # runs, where COALESCE then leaves the column untouched).
    ma_db.update_sgc_ma_result(engine, run_id, status="RUNNING",
                               batch_job_id=os.environ.get("AWS_BATCH_JOB_ID"))
    s3 = None
    try:
        if os.path.isdir(local_out):
            _clear_outputs(local_out)
        cohorts = sel.select_cohorts_by_file_ids(engine, file_ids)
        ignored = sel.ignored_cohorts(engine, phenotype, ancestry, sex)
        click.echo(f"run {run_id}: {len(cohorts)} cohorts for {phenotype}/{ancestry}/{sex} "
                   f"(maf_min={maf_min:g} info_min={info_min:g})")
        s3 = boto3.client("s3", region_name="us-east-1")

        def fetch_fn(co, local_path):
            s3.download_file(bucket, co["munge_s3_key"], local_path)

        with tempfile.TemporaryDirectory(prefix="metal_work_") as work_dir:
            summary = meta_analyze(cohorts, fetch_fn, work_dir, local_out, phenotype=phenotype,
                                   ancestry=ancestry, sex=sex, ignored=ignored,
                                   engine_version=metal_version(), maf_min=maf_min, info_min=info_min)
        _upload(s3, bucket, prefix, local_out, UPLOAD_NAMES)
        click.echo(json.dumps(summary, indent=2))

        ma_db.update_sgc_ma_result(
            engine, run_id, status="SUCCEEDED",
            meta_lambda_gc=summary["meta_lambda_gc"],
            n_meta_variants=summary["n_meta_variants"],
            n_genome_wide_sig=summary["n_genome_wide_sig"],
            n_cohorts=summary["n_cohorts"],
            n_cohorts_used=summary["n_cohorts_used"],
            total_cases=summary["total_cases"],
            total_controls=summary["total_controls"],
            manhattan_s3_key=f"{prefix}/manhattan.png",
            qq_s3_key=f"{prefix}/qq.png",
            meta_s3_key=f"{prefix}/metal.tbl.gz",
            summary_json_s3_key=f"{prefix}/summary.json",
            summary_tsv_s3_key=f"{prefix}/summary.tsv",
            top_loci_s3_key=f"{prefix}/top_loci.tsv",
        )
    except Exception as e:
        ma_db.update_sgc_ma_result(engine, run_id, status="FAILED", error_message=truncate_error(str(e)))
        if s3 is not None:  # best effort: METAL's logs (if it got that far) under the run prefix
            try:
                _upload(s3, bucket, prefix, local_out, LOG_NAMES)
            except Exception as upload_error:  # never mask the original failure
                click.echo(f"could not upload METAL logs: {upload_error}", err=True)
        raise


if __name__ == "__main__":
    main()
