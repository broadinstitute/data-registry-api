import os
import re

_HEAD = [
    "SCHEME STDERR",
    "AVERAGEFREQ ON",
    "MINMAXFREQ ON",
    "CUSTOMVARIABLE Ncases",
    "CUSTOMVARIABLE Ncontrols",
    "MARKER SNP_ID",
    "ALLELE Effect_allele Non-effect_allele",
    "EFFECT Beta",
    "STDERR SE",
    "PVALUE P-value",
    "FREQLABEL Effect_AF",
    "LABEL Ncases AS N_CAS",
    "LABEL Ncontrols AS N_CON",
]

# METAL splits script lines on whitespace, and OUTFILE's prefix is a path.
_UNSAFE = re.compile(r"[^A-Za-z0-9_.-]")


def sanitise(s) -> str:
    return _UNSAFE.sub("_", str(s or ""))


def outfile_prefix(phenotype: str, ancestry: str, sex: str) -> str:
    return f"{sanitise(phenotype)}_{sanitise(ancestry)}_{sanitise(sex)}_"


def render_script(process_paths: list[str], out_prefix: str, *, heterogeneity: bool = True) -> str:
    """METAL script over process_paths. heterogeneity=False (the frequency pass) runs a
    plain ANALYZE, which reads every input once instead of twice."""
    lines = list(_HEAD)
    lines += [f"PROCESS {p}" for p in process_paths]
    lines += [f"OUTFILE {out_prefix} .tbl", "ANALYZE HETEROGENEITY" if heterogeneity else "ANALYZE", "QUIT"]
    return "\n".join(lines) + "\n"


def tbl_path(work_dir: str, out_prefix: str) -> str:
    return os.path.join(work_dir, out_prefix + "1.tbl")
