import pathlib
import re

import yaml


def test_munge_stack_shape():
    t = yaml.safe_load(re.sub(r"!(Sub|GetAtt|Ref)\s", "", pathlib.Path("sgc_munge/sgc_gwas_munge.yml").read_text()))
    jd = t["Resources"]["MungeJobDefinition"]["Properties"]
    assert jd["JobDefinitionName"] == "sgc-gwas-munge-job" and jd["RetryStrategy"]["Attempts"] == 2
    cp = jd["ContainerProperties"]
    assert cp["EphemeralStorage"]["SizeInGiB"] == 40
    assert {r["Type"]: r["Value"] for r in cp["ResourceRequirements"]} == {"VCPU": "2", "MEMORY": "8192"}
    assert cp["Command"][cp["Command"].index("--ref-prefix") + 1] == "Ref::ref-prefix"
    assert t["Resources"]["MungeEcrRepository"]["Properties"]["RepositoryName"] == "sgc-gwas-munge-repo"


def test_prep_script_is_strict_and_names_outputs():
    s = pathlib.Path("sgc_munge/prep_reference.sh").read_text()
    assert "set -euo pipefail" in s
    for f in ["GRCh38.primary.fa.gz", "dbsnp155.grch38.slim.vcf.gz", "MANIFEST.json", "--rename-chrs", "-x INFO"]:
        assert f in s


def test_prep_script_indexes_raw_dbsnp_before_rename_and_checks_output():
    s = pathlib.Path("sgc_munge/prep_reference.sh").read_text()
    # the b155 header has no ##contig lines: --rename-chrs only works on an indexed input
    assert s.index("tabix -p vcf GCF_000001405.39.bgz") < s.index("bcftools annotate --rename-chrs chr_map.txt")
    assert "tabix -l \"$VCF\"" in s and "rs1570391677" in s and "1:10001-10001" in s
    assert "dbsnp_index" not in s   # no such remote file for .39


def test_dockerfile_installs_bcftools():
    s = pathlib.Path("sgc_munge/Dockerfile").read_text()
    assert "apt-get install -y --no-install-recommends bcftools" in s
    assert "rm -rf /var/lib/apt/lists/*" in s
    assert s.index("apt-get install") < s.index("pip install")
