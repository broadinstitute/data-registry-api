"""bcftools-based bulk dbSNP rsID lookup for the munge worker: sort the target positions,
query dbSNP once with ``bcftools query --targets-file`` and load the per-contig hits."""
import os
import shutil
import subprocess


def bcftools_path() -> str:
    return os.environ.get("SGC_BCFTOOLS", "bcftools")


def check_bcftools() -> str:
    """Return bcftools' version line, or raise RuntimeError when the binary is absent."""
    path = bcftools_path()
    if shutil.which(path) is None:
        raise RuntimeError(f"bcftools not found at {path!r}; install bcftools or set SGC_BCFTOOLS")
    out = subprocess.run([path, "--version"], capture_output=True, text=True, check=True).stdout
    return out.splitlines()[0].strip()


# (REF, ALTs, ID) -- the shape reference.pick_rsid consumes; ID is None when dbSNP has ".".
HitRecord = tuple[str, tuple[str, ...], str | None]


def sort_targets(src: str, dst: str, tmp_dir: str) -> int:
    """Sort and de-duplicate a CHROM<TAB>POS targets file; return the number of unique positions."""
    subprocess.run(
        ["sort", "-u", "-k1,1", "-k2,2n", "-S", "512M", "-T", tmp_dir, src, "-o", dst],
        check=True, env={**os.environ, "LC_ALL": "C"})
    with open(dst) as f:
        return sum(1 for _ in f)


def query_dbsnp(dbsnp_path: str, targets_sorted: str, hits_dir: str) -> dict[str, int]:
    """One bcftools pass over dbSNP; write <hits_dir>/<contig>.tsv and return {contig: n_records}."""
    if os.path.getsize(targets_sorted) == 0:
        return {}
    cmd = [bcftools_path(), "query", "--targets-file", targets_sorted,
           "-f", "%CHROM\t%POS\t%ID\t%REF\t%ALT\n", dbsnp_path]
    stderr_path = os.path.join(hits_dir, "bcftools.stderr")
    counts: dict[str, int] = {}
    writers = {}
    try:
        with open(stderr_path, "w") as err:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=err, text=True)
            try:
                for line in proc.stdout:
                    contig = line.split("\t", 1)[0]
                    w = writers.get(contig)
                    if w is None:
                        w = writers[contig] = open(os.path.join(hits_dir, f"{contig}.tsv"), "w")
                    w.write(line)
                    counts[contig] = counts.get(contig, 0) + 1
            finally:
                proc.stdout.close()
                rc = proc.wait()
    finally:
        for w in writers.values():
            w.close()
    if rc != 0:
        with open(stderr_path) as f:
            tail = f.read()[-2000:]
        raise RuntimeError(f"bcftools query failed (exit {rc}): {tail}")
    return counts


def load_hits(path: str) -> dict[int, list[HitRecord]]:
    """Parse one per-contig hits file into {pos: [(ref, alts, id)]}; a missing file is {}."""
    result: dict[int, list[HitRecord]] = {}
    if not os.path.exists(path):
        return result
    with open(path) as f:
        for line in f:
            _, pos, rs_id, ref, alt = line.rstrip("\n").split("\t")
            result.setdefault(int(pos), []).append(
                (ref, tuple(alt.split(",")), None if rs_id == "." else rs_id))
    return result
