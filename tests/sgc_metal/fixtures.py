import gzip

from sgc_munge.munge import OUTPUT_COLUMNS


def write_munged(path: str, rows: list[dict]) -> str:
    """Write a gzipped TSV in the munge output format; missing keys become NA."""
    with gzip.open(path, "wt") as fh:
        fh.write("\t".join(OUTPUT_COLUMNS) + "\n")
        for row in rows:
            fh.write("\t".join(str(row.get(c, "NA")) for c in OUTPUT_COLUMNS) + "\n")
    return path
