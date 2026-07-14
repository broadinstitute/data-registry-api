"""Bounded-memory k-way merge of per-cohort sumstats -> meta results."""
import heapq
import os
import subprocess

from sgc_ma.harmonize import canonicalize
from sgc_ma.compute import finalize_one


def extract_sorted(chunks, out_path):
    """Canonicalize each row, write per-row sufficient stats keyed by variant,
    sort by key (LC_ALL=C), then keep only the FIRST row per key (within-cohort
    dedup so one cohort contributes a variant at most once). Returns
    {"n_in": rows read, "n_kept": rows after dedup, "sum_n": sum of n over kept}."""
    n_in = 0
    with open(out_path, "w") as fh:
        for df in chunks:
            for chrom, pos, ea, oa, beta, se, eaf, nn in zip(
                    df["chromosome"], df["position"], df["ea"], df["oa"],
                    df["beta"], df["se"], df["eaf"], df["n"]):
                n_in += 1
                c = canonicalize(chrom, pos, ea, oa, beta, eaf)
                if c is None:
                    continue
                key, ch, po, refA, refB, b, _f = c
                w = 1.0 / (se * se)
                fh.write(f"{key}\t{w}\t{w*b}\t{w*b*b}\t{nn}\t{1 if b > 0 else 0}\t{ch}\t{po}\t{refA}\t{refB}\n")
    subprocess.run(["sort", "-k1,1", "-o", out_path, out_path], check=True,
                   env={**os.environ, "LC_ALL": "C"})
    deduped = out_path + ".dedup"
    n_kept, sum_n, prev = 0, 0.0, None
    with open(out_path) as fin, open(deduped, "w") as fout:
        for line in fin:
            parts = line.split("\t")
            if parts[0] != prev:
                fout.write(line)
                prev = parts[0]
                n_kept += 1
                sum_n += float(parts[4])
    os.replace(deduped, out_path)
    return {"n_in": n_in, "n_kept": n_kept, "sum_n": sum_n}


def merge_and_combine(sorted_paths, min_cohorts: int = 2):
    files = [open(p) for p in sorted_paths]
    try:
        streams = (( (ln.split("\t")[0], ln) for ln in f) for f in files)
        cur_key = None
        acc = None
        for key, ln in heapq.merge(*streams, key=lambda t: t[0]):
            parts = ln.rstrip("\n").split("\t")
            w, wb, wb2, nn, ps = float(parts[1]), float(parts[2]), float(parts[3]), float(parts[4]), int(parts[5])
            ch, po, refA, refB = parts[6], int(parts[7]), parts[8], parts[9]
            if key != cur_key:
                if acc is not None:
                    r = finalize_one(*acc[:6], min_cohorts=min_cohorts)
                    if r is not None:
                        r.update({"chromosome": acc[6], "position": acc[7], "ref": acc[8], "alt": acc[9]})
                        yield r
                acc = [w, wb, wb2, nn, ps, 1, ch, po, refA, refB]
                cur_key = key
            else:
                acc[0]+=w; acc[1]+=wb; acc[2]+=wb2; acc[3]+=nn; acc[4]+=ps; acc[5]+=1
        if acc is not None:
            r = finalize_one(*acc[:6], min_cohorts=min_cohorts)
            if r is not None:
                r.update({"chromosome": acc[6], "position": acc[7], "ref": acc[8], "alt": acc[9]})
                yield r
    finally:
        for f in files:
            f.close()
