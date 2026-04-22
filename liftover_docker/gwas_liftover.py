#!/usr/bin/env python3
"""
GWAS Liftover Worker

Downloads a GWAS summary stats TSV from S3, lifts chromosome/position from one
genome build to another using the UCSC liftOver binary, handles strand flips
(reverse-complement alleles), uploads lifted file back to S3, archives the
original, writes an unmapped-variants TSV, and writes a structured summary JSON.

The container is invoked as an AWS Batch Fargate job. The final stdout line
begins with LIFTOVER_SUMMARY_JSON: followed by a compact JSON object, which
the API callback reads from CloudWatch logs.
"""

import argparse
import gzip
import io
import json
import logging
import os
import subprocess
import sys
import tempfile
import time

import boto3
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Chain file paths (baked into the Docker image)
# ---------------------------------------------------------------------------

CHAIN_FILES = {
    ("hg19", "hg38"): "/opt/chains/hg19ToHg38.over.chain.gz",
    ("hg38", "hg19"): "/opt/chains/hg38ToHg19.over.chain.gz",
}

# Complement table for reverse-complement on strand flips
COMPLEMENT = str.maketrans("ACTGactg", "TGACtgac")


# ---------------------------------------------------------------------------
# Pure functions (unit-testable without liftOver binary)
# ---------------------------------------------------------------------------

def standardize_chromosome(chrom: str) -> str:
    """
    Normalize a chromosome label to the canonical 'chrN' form.

    - Strips leading 'chr' or 'Chr' prefix.
    - Converts numeric aliases: '23' -> 'X', '24' -> 'Y', '25' -> 'M'.
    - Re-adds 'chr' prefix.

    Examples:
        '1'   -> 'chr1'
        'chr1'-> 'chr1'
        '23'  -> 'chrX'
        'X'   -> 'chrX'
        'chrM'-> 'chrM'
    """
    s = str(chrom).strip()
    # Strip chr/Chr prefix
    if s.lower().startswith("chr"):
        s = s[3:]
    # Numeric aliases
    NUMERIC_ALIASES = {"23": "X", "24": "Y", "25": "M"}
    s = NUMERIC_ALIASES.get(s, s)
    return f"chr{s}"


def reverse_complement(allele: str) -> str:
    """
    Reverse-complement a DNA allele string.

    - Handles multi-base alleles for indels (e.g. 'AC' -> 'GT').
    - Case-insensitive; preserves case of output (uppercase).
    - Unknown/special bases ('N', '-', '.') pass through as-is.

    Examples:
        'A'  -> 'T'
        'C'  -> 'G'
        'AC' -> 'GT'
        'N'  -> 'N'
        '-'  -> '-'
        '.'  -> '.'
    """
    # Translate base-by-base, then reverse
    result = []
    for base in allele:
        translated = base.translate(COMPLEMENT)
        # If translate didn't change it (unknown base), keep original
        result.append(translated)
    return "".join(reversed(result)).upper()


def pick_chain_file(source: str, target: str) -> str:
    """
    Return the path to the appropriate UCSC chain file.

    Raises SystemExit with a clear message if the combination is unsupported.
    """
    key = (source.lower(), target.lower())
    chain = CHAIN_FILES.get(key)
    if chain is None:
        log.error(
            "Unsupported build combination: %s -> %s. "
            "Supported: hg19->hg38, hg38->hg19.",
            source, target,
        )
        sys.exit(1)
    return chain


def tsv_to_bed(df: pd.DataFrame, column_mapping: dict) -> list[str]:
    """
    Convert a GWAS DataFrame to BED-format lines for UCSC liftOver input.

    BED format (0-based half-open): chr, start, end, varid, score, strand
    where start = pos - 1 and end = pos (1-based -> 0-based).

    The varid encodes the original chr/pos/ref/alt so we can join back after
    liftover. varid = f"{standardized_chr}_{pos}_{ref}_{alt}".

    Returns a list of tab-separated BED line strings (no newlines).
    Logs a warning and deduplicates if duplicate varids are found (keeps first).
    """
    chr_col = column_mapping["chromosome"]
    pos_col = column_mapping["position"]
    ref_col = column_mapping["ref"]
    alt_col = column_mapping["alt"]

    # Vectorized chromosome standardization
    std_chr = df[chr_col].astype(str).apply(standardize_chromosome)

    pos = df[pos_col].astype(int)

    # Build varid column vectorially
    varid = std_chr + "_" + pos.astype(str) + "_" + df[ref_col].astype(str) + "_" + df[alt_col].astype(str)

    bed = pd.DataFrame({
        "chr": std_chr,
        "start": pos - 1,
        "end": pos,
        "varid": varid,
        "score": ".",
        "strand": "+",
    })

    before = len(bed)
    bed = bed.drop_duplicates(subset=["varid"], keep="first")
    after = len(bed)
    dropped = before - after
    if dropped > 0:
        log.warning("Duplicate varids dropped from BED: %d", dropped)

    # Build list of tab-separated BED line strings
    lines = (
        bed["chr"] + "\t" +
        bed["start"].astype(str) + "\t" +
        bed["end"].astype(str) + "\t" +
        bed["varid"] + "\t" +
        bed["score"] + "\t" +
        bed["strand"]
    ).tolist()

    return lines


def run_liftover(
    bed_path: str,
    chain_path: str,
    lifted_path: str,
    unmapped_path: str,
) -> subprocess.CompletedProcess:
    """
    Execute the UCSC liftOver binary.

    NOTE: liftOver exits with code 1 when there are unmapped records — this is
    normal. Exit codes other than 0 or 1 indicate a real error; we log stderr
    at ERROR level and call sys.exit(1).

    After execution we also verify the output files are consistent: if
    lifted.bed is missing or empty AND there are no unmapped records, something
    went wrong silently — also sys.exit(1).
    """
    cmd = ["liftOver", bed_path, chain_path, lifted_path, unmapped_path]
    log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.stdout:
        log.info("liftOver stdout: %s", result.stdout.strip())
    if result.returncode not in (0, 1):
        # Return code 1 = partial mapping (normal). Anything else is a real error.
        log.error(
            "liftOver returned unexpected exit code %d. stderr: %s",
            result.returncode,
            result.stderr.strip(),
        )
        sys.exit(1)
    if result.stderr:
        log.info("liftOver stderr: %s", result.stderr.strip())

    # Sanity check: if lifted.bed is absent/empty AND unmapped is also absent/empty,
    # liftOver failed silently (e.g. corrupt chain file).
    lifted_empty = not os.path.exists(lifted_path) or os.path.getsize(lifted_path) == 0
    unmapped_empty = not os.path.exists(unmapped_path) or os.path.getsize(unmapped_path) == 0
    if lifted_empty and unmapped_empty:
        log.error(
            "liftOver produced no output: lifted.bed and unmapped.bed are both "
            "absent or empty. The chain file may be corrupt."
        )
        sys.exit(1)

    return result


def _parse_lifted_bed(lifted_path: str) -> pd.DataFrame:
    """
    Parse the lifted BED output file.

    Returns a DataFrame with columns: chr, start, end, varid, score, strand.
    Returns an empty DataFrame if the file is empty.
    """
    if not os.path.exists(lifted_path) or os.path.getsize(lifted_path) == 0:
        return pd.DataFrame(columns=["chr", "start", "end", "varid", "score", "strand"])

    df = pd.read_csv(
        lifted_path,
        sep="\t",
        header=None,
        names=["chr", "start", "end", "varid", "score", "strand"],
        dtype={"chr": str, "start": int, "end": int, "varid": str, "score": str, "strand": str},
    )
    return df


def _parse_unmapped_bed(unmapped_path: str) -> set[str]:
    """
    Parse the unmapped BED output file from liftOver.

    Lines starting with '#' are comments explaining the reason (e.g.
    "# Sequence hg19_chr1_... not found..."). The data lines have varid in col 4.
    Returns a set of varids that could not be mapped.
    """
    varids: set[str] = set()
    if not os.path.exists(unmapped_path) or os.path.getsize(unmapped_path) == 0:
        return varids

    with open(unmapped_path, "r") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) >= 4:
                varids.add(parts[3])

    return varids


def apply_lifted_positions(
    df: pd.DataFrame,
    lifted_bed_df: pd.DataFrame,
    column_mapping: dict,
) -> tuple[pd.DataFrame, set[str]]:
    """
    Join the original GWAS DataFrame with the lifted BED results.

    - Replaces chr/pos columns with lifted values.
    - Reverse-complements ref/alt alleles for strand-flipped variants (strand=='-').
    - Drops variants where the lifted chromosome differs from the original
      (after standardization). These are collected as chr_mismatch varids.
    - Deduplicates on varid (keeps first) to match tsv_to_bed semantics.

    Returns:
        (lifted_df, chr_mismatch_varids)
        where chr_mismatch_varids is a set of varid strings for dropped rows.
    """
    chr_col = column_mapping["chromosome"]
    pos_col = column_mapping["position"]
    ref_col = column_mapping["ref"]
    alt_col = column_mapping["alt"]

    # Build _varid and _orig_std_chr columns vectorially
    working = df.copy()
    std_chr_series = working[chr_col].astype(str).apply(standardize_chromosome)
    pos_series = working[pos_col].astype(int)
    working["_varid"] = (
        std_chr_series + "_" +
        pos_series.astype(str) + "_" +
        working[ref_col].astype(str) + "_" +
        working[alt_col].astype(str)
    )
    working["_orig_std_chr"] = std_chr_series

    # Deduplicate on _varid (keep first) to honour same semantics as tsv_to_bed
    working = working.drop_duplicates(subset=["_varid"], keep="first")

    if lifted_bed_df.empty:
        return df.iloc[0:0].copy(), set()

    # Standardize lifted chromosome column
    lifted = lifted_bed_df.copy()
    lifted["_lifted_std_chr"] = lifted["chr"].astype(str).apply(standardize_chromosome)

    # Merge: inner join keeps only successfully-lifted rows
    merged = working.merge(
        lifted[["varid", "_lifted_std_chr", "end", "strand"]],
        left_on="_varid",
        right_on="varid",
        how="inner",
        suffixes=("", "_lifted"),
    )

    # Identify chr-mismatch rows (lifted to a different chromosome)
    chr_mismatch_mask = merged["_lifted_std_chr"] != merged["_orig_std_chr"]
    chr_mismatch_varids: set[str] = set(merged.loc[chr_mismatch_mask, "_varid"].tolist())

    # Keep only rows without chr mismatch
    merged = merged[~chr_mismatch_mask].copy()

    # Reverse-complement ref/alt for strand-flipped variants
    flip_mask = merged["strand"] == "-"
    if flip_mask.any():
        merged.loc[flip_mask, ref_col] = merged.loc[flip_mask, ref_col].astype(str).apply(reverse_complement)
        merged.loc[flip_mask, alt_col] = merged.loc[flip_mask, alt_col].astype(str).apply(reverse_complement)

    # Update chr and pos with lifted values
    merged[chr_col] = merged["_lifted_std_chr"]
    merged[pos_col] = merged["end"].astype(int)  # BED end = new 1-based position

    # Drop all internal/merge columns
    drop_cols = ["_varid", "_orig_std_chr", "_lifted_std_chr", "varid", "end", "strand"]
    merged = merged.drop(columns=[c for c in drop_cols if c in merged.columns])

    return merged, chr_mismatch_varids


def validate_column_mapping(df: pd.DataFrame, column_mapping: dict) -> None:
    """
    Validate that all values in column_mapping exist as columns in df.

    Raises SystemExit with a clear error message listing missing columns.
    """
    missing_cols = [v for v in column_mapping.values() if v not in df.columns]
    if missing_cols:
        log.error("--column-mapping references columns not in input: %s", missing_cols)
        sys.exit(1)


def compute_summary(
    job_id: str,
    source_build: str,
    target_build: str,
    chain_file: str,
    total_input: int,
    total_lifted: int,
    total_unmapped: int,
    unmapped_breakdown: dict[str, int],
    strand_flips: int,
    per_chromosome: dict[str, dict],
    duration_seconds: float,
) -> dict:
    """Build the structured summary dict."""
    unmapped_pct = round(total_unmapped / total_input * 100, 4) if total_input > 0 else 0.0
    return {
        "job_id": job_id,
        "source_build": source_build,
        "target_build": target_build,
        "total_input_variants": total_input,
        "total_lifted": total_lifted,
        "total_unmapped": total_unmapped,
        "unmapped_pct": unmapped_pct,
        "unmapped_breakdown": unmapped_breakdown,
        "strand_flips": strand_flips,
        "per_chromosome": per_chromosome,
        "duration_seconds": round(duration_seconds, 2),
        "chain_file": os.path.basename(chain_file),
        "tool": "UCSC liftOver",
    }


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def parse_s3_path(s3_path: str) -> tuple[str, str]:
    """Split 's3://bucket/key' into (bucket, key)."""
    path = s3_path.removeprefix("s3://")
    bucket, _, key = path.partition("/")
    return bucket, key


def download_input(s3_client, s3_path: str, local_path: str) -> None:
    bucket, key = parse_s3_path(s3_path)
    log.info("Downloading %s -> %s", s3_path, local_path)
    s3_client.download_file(bucket, key, local_path)


def read_gwas_tsv(local_path: str) -> pd.DataFrame:
    """
    Read the GWAS TSV (or CSV) into a DataFrame.

    Detection strategy:
    - If path ends with .gz, pass compression='gzip'.
    - Default delimiter is tab. If the file extension is .csv, use comma.
    - For tab files, use sep='\t' explicitly.
    """
    is_gzipped = local_path.endswith(".gz")
    base = local_path[:-3] if is_gzipped else local_path
    is_csv = base.endswith(".csv")

    compression = "gzip" if is_gzipped else None
    sep = "," if is_csv else "\t"

    log.info("Reading input file (gzip=%s, sep=%r)", is_gzipped, sep)
    df = pd.read_csv(
        local_path,
        sep=sep,
        compression=compression,
        dtype=str,        # read everything as string to avoid type coercion
        low_memory=False,
    )
    log.info("Loaded %d rows, %d columns", len(df), len(df.columns))
    return df


def upload_bytes(s3_client, body: bytes, s3_path: str, content_type: str = "text/tab-separated-values") -> None:
    bucket, key = parse_s3_path(s3_path)
    log.info("Uploading %d bytes -> %s", len(body), s3_path)
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def df_to_bytes(df: pd.DataFrame, sep: str = "\t", compress: bool = False) -> bytes:
    """Serialize a DataFrame to TSV bytes, optionally gzip-compressed."""
    buf = io.StringIO()
    df.to_csv(buf, sep=sep, index=False)
    raw = buf.getvalue().encode("utf-8")
    if compress:
        gz_buf = io.BytesIO()
        with gzip.GzipFile(fileobj=gz_buf, mode="wb") as gz:
            gz.write(raw)
        return gz_buf.getvalue()
    return raw


def copy_to_archive(s3_client, source_s3_path: str, archive_s3_path: str) -> None:
    """
    Archive the original input file using S3 server-side copy.

    Does NOT delete the source — the lifted file will overwrite it via upload.
    """
    src_bucket, src_key = parse_s3_path(source_s3_path)
    dst_bucket, dst_key = parse_s3_path(archive_s3_path)
    log.info("Archiving s3://%s/%s -> s3://%s/%s", src_bucket, src_key, dst_bucket, dst_key)
    s3_client.copy_object(
        Bucket=dst_bucket,
        Key=dst_key,
        CopySource={"Bucket": src_bucket, "Key": src_key},
    )


# ---------------------------------------------------------------------------
# Statistics helpers
# ---------------------------------------------------------------------------

def compute_per_chromosome_stats(
    original_df: pd.DataFrame,
    lifted_df: pd.DataFrame,
    column_mapping: dict,
    lifted_bed_df: pd.DataFrame,
) -> tuple[dict[str, dict], int]:
    """
    Compute per-chromosome input/lifted/unmapped/strand_flip counts.

    Returns (per_chr_dict, total_strand_flips).
    """
    chr_col = column_mapping["chromosome"]

    # Count input per original chromosome (vectorized)
    input_counts = (
        original_df[chr_col].astype(str)
        .apply(standardize_chromosome)
        .value_counts()
        .to_dict()
    )

    # Count lifted per new chromosome (vectorized)
    lifted_counts = (
        lifted_df[chr_col].astype(str)
        .apply(standardize_chromosome)
        .value_counts()
        .to_dict()
    ) if not lifted_df.empty else {}

    # Count strand flips from lifted BED (vectorized)
    strand_flip_counts: dict[str, int] = {}
    total_strand_flips = 0
    if not lifted_bed_df.empty:
        flipped = lifted_bed_df[lifted_bed_df["strand"] == "-"]
        if not flipped.empty:
            total_strand_flips = len(flipped)
            # Derive original chromosome from varid prefix (chr_pos_ref_alt)
            orig_chrs = (
                flipped["varid"].astype(str)
                .str.split("_", n=1)
                .str[0]
                .apply(standardize_chromosome)
            )
            strand_flip_counts = orig_chrs.value_counts().to_dict()

    # Build combined per-chromosome dict
    all_chrs = set(input_counts) | set(lifted_counts)
    per_chr: dict[str, dict] = {}
    for chrom in sorted(all_chrs):
        inp = input_counts.get(chrom, 0)
        lft = lifted_counts.get(chrom, 0)
        unm = inp - lft  # anything that didn't make it to lifted
        per_chr[chrom] = {
            "input": inp,
            "lifted": lft,
            "unmapped": max(unm, 0),
            "strand_flips": strand_flip_counts.get(chrom, 0),
        }

    return per_chr, total_strand_flips


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lift GWAS summary stats between genome builds using UCSC liftOver."
    )
    parser.add_argument("--input-s3-path", required=True,
                        help="s3://bucket/path/to/input.tsv[.gz]")
    parser.add_argument("--output-s3-path", required=True,
                        help="s3://bucket/path/to/lifted.tsv (will overwrite input path)")
    parser.add_argument("--archive-s3-path", required=True,
                        help="s3://bucket/path/to/archive/original.tsv[.gz]")
    parser.add_argument("--unmapped-s3-path", required=True,
                        help="s3://bucket/path/to/unmapped.tsv")
    parser.add_argument("--summary-s3-path", required=True,
                        help="s3://bucket/path/to/summary.json")
    parser.add_argument("--source-build", required=True, choices=["hg19", "hg38"],
                        help="Source genome build")
    parser.add_argument("--target-build", required=True, choices=["hg19", "hg38"],
                        help="Target genome build (must differ from source)")
    parser.add_argument("--column-mapping", required=True,
                        help='JSON string with keys chromosome/position/ref/alt, '
                             'values are actual column names in the TSV. '
                             'Example: \'{"chromosome":"chr","position":"pos","ref":"a1","alt":"a2"}\'')
    parser.add_argument("--job-id", required=True,
                        help="UUID string for this job (logged for traceability)")
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main(argv=None) -> None:
    start_time = time.monotonic()

    args = parse_args(argv)

    log.info("Job ID: %s", args.job_id)
    log.info("Source build: %s  Target build: %s", args.source_build, args.target_build)
    log.info("Input:   %s", args.input_s3_path)
    log.info("Output:  %s", args.output_s3_path)
    log.info("Archive: %s", args.archive_s3_path)

    # Validate builds differ
    if args.source_build == args.target_build:
        log.error("source-build and target-build must differ.")
        sys.exit(1)

    # Parse column mapping
    try:
        column_mapping = json.loads(args.column_mapping)
    except json.JSONDecodeError as exc:
        log.error("Invalid --column-mapping JSON: %s", exc)
        sys.exit(1)

    required_keys = {"chromosome", "position", "ref", "alt"}
    missing = required_keys - set(column_mapping)
    if missing:
        log.error("--column-mapping is missing required keys: %s", sorted(missing))
        sys.exit(1)

    # Pick chain file
    chain_file = pick_chain_file(args.source_build, args.target_build)
    log.info("Chain file: %s", chain_file)

    s3_client = boto3.client("s3")

    with tempfile.TemporaryDirectory() as tmpdir:
        # 1. Archive original before any modification
        copy_to_archive(s3_client, args.input_s3_path, args.archive_s3_path)

        # 2. Download input
        input_filename = os.path.basename(args.input_s3_path)
        local_input = os.path.join(tmpdir, input_filename)
        download_input(s3_client, args.input_s3_path, local_input)

        # Detect if input is gzipped (for output compression)
        input_is_gzipped = local_input.endswith(".gz")
        # Detect delimiter from extension
        base_name = local_input[:-3] if input_is_gzipped else local_input
        input_sep = "," if base_name.endswith(".csv") else "\t"

        # 3. Read input TSV
        original_df = read_gwas_tsv(local_input)
        total_input = len(original_df)
        log.info("Total input variants: %d", total_input)

        # 4. Validate column mapping against the loaded DataFrame
        validate_column_mapping(original_df, column_mapping)

        # 5. Build BED
        bed_lines = tsv_to_bed(original_df, column_mapping)
        bed_path = os.path.join(tmpdir, "input.bed")
        with open(bed_path, "w") as fh:
            fh.write("\n".join(bed_lines))
            if bed_lines:
                fh.write("\n")
        log.info("Wrote %d BED lines to %s", len(bed_lines), bed_path)

        # 6. Run liftOver
        lifted_bed_path = os.path.join(tmpdir, "lifted.bed")
        unmapped_bed_path = os.path.join(tmpdir, "unmapped.bed")
        run_liftover(bed_path, chain_file, lifted_bed_path, unmapped_bed_path)

        # 7. Parse lifted BED
        lifted_bed_df = _parse_lifted_bed(lifted_bed_path)
        log.info("Lifted BED rows: %d", len(lifted_bed_df))

        # 8. Parse unmapped BED
        liftover_unmapped_varids = _parse_unmapped_bed(unmapped_bed_path)
        log.info("Unmapped by liftOver: %d", len(liftover_unmapped_varids))

        # 9. Apply lifted positions to original DataFrame
        lifted_df, chr_mismatch_varids = apply_lifted_positions(
            original_df, lifted_bed_df, column_mapping
        )
        log.info(
            "Lifted variants: %d  Chr-mismatch dropped: %d",
            len(lifted_df), len(chr_mismatch_varids),
        )

        # 10. Build unmapped output (vectorized varid construction + boolean masks)
        chr_col = column_mapping["chromosome"]
        pos_col = column_mapping["position"]
        ref_col = column_mapping["ref"]
        alt_col = column_mapping["alt"]

        orig_varid_series = (
            original_df[chr_col].astype(str).apply(standardize_chromosome) + "_" +
            original_df[pos_col].astype(int).astype(str) + "_" +
            original_df[ref_col].astype(str) + "_" +
            original_df[alt_col].astype(str)
        )

        # Deduplicate: keep first occurrence of each varid (matching tsv_to_bed)
        first_occurrence_mask = ~orig_varid_series.duplicated(keep="first")
        deduped_df = original_df[first_occurrence_mask].copy()
        deduped_varids = orig_varid_series[first_occurrence_mask]

        # Rows whose varid appears in liftover_unmapped_varids
        lo_unmapped_mask = deduped_varids.isin(liftover_unmapped_varids)
        lo_unmapped_df = deduped_df[lo_unmapped_mask].copy()
        lo_unmapped_df["_unmapped_reason"] = "liftover_unmapped"

        # Rows whose varid appears in chr_mismatch_varids
        mismatch_mask = deduped_varids.isin(chr_mismatch_varids)
        mismatch_df = deduped_df[mismatch_mask].copy()
        mismatch_df["_unmapped_reason"] = "chr_mismatch"

        if len(lo_unmapped_df) > 0 or len(mismatch_df) > 0:
            unmapped_df = pd.concat([lo_unmapped_df, mismatch_df], ignore_index=True)
            # Put _unmapped_reason first
            cols = ["_unmapped_reason"] + [c for c in unmapped_df.columns if c != "_unmapped_reason"]
            unmapped_df = unmapped_df[cols]
        else:
            unmapped_df = pd.DataFrame(columns=["_unmapped_reason"] + list(original_df.columns))

        # 11. Compute statistics
        per_chr, total_strand_flips = compute_per_chromosome_stats(
            original_df, lifted_df, column_mapping, lifted_bed_df,
        )

        total_lifted = len(lifted_df)
        total_unmapped = total_input - total_lifted
        unmapped_breakdown = {
            "liftover_unmapped": len(liftover_unmapped_varids),
            "chr_mismatch": len(chr_mismatch_varids),
        }

        # 12. Write lifted TSV back to output S3 path
        lifted_bytes = df_to_bytes(lifted_df, sep=input_sep, compress=input_is_gzipped)
        content_type = "application/gzip" if input_is_gzipped else "text/tab-separated-values"
        upload_bytes(s3_client, lifted_bytes, args.output_s3_path, content_type)

        # 13. Write unmapped TSV
        unmapped_bytes = df_to_bytes(unmapped_df, sep="\t", compress=False)
        upload_bytes(s3_client, unmapped_bytes, args.unmapped_s3_path, "text/tab-separated-values")

        # 14. Write summary JSON
        duration = time.monotonic() - start_time
        summary = compute_summary(
            job_id=args.job_id,
            source_build=args.source_build,
            target_build=args.target_build,
            chain_file=chain_file,
            total_input=total_input,
            total_lifted=total_lifted,
            total_unmapped=total_unmapped,
            unmapped_breakdown=unmapped_breakdown,
            strand_flips=total_strand_flips,
            per_chromosome=per_chr,
            duration_seconds=duration,
        )
        summary_json = json.dumps(summary, separators=(",", ":"))
        upload_bytes(
            s3_client,
            summary_json.encode("utf-8"),
            args.summary_s3_path,
            "application/json",
        )

    # 15. Print summary as final stdout line for CloudWatch log parsing
    print(f"LIFTOVER_SUMMARY_JSON: {summary_json}", flush=True)
    log.info("Liftover complete. Lifted: %d / %d  Unmapped: %d  Strand flips: %d",
             total_lifted, total_input, total_unmapped, total_strand_flips)


if __name__ == "__main__":
    main()
