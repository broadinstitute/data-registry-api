import pathlib
import re

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]


def test_metal_stack_shape():
    text = (ROOT / "sgc_metal/sgc_gwas_metal.yml").read_text()
    t = yaml.safe_load(re.sub(r"!(Sub|GetAtt|Ref)\s", "", text))
    jd = t["Resources"]["MetalJobDefinition"]["Properties"]
    assert jd["JobDefinitionName"] == "sgc-gwas-metal-job" and jd["RetryStrategy"]["Attempts"] == 2
    cp = jd["ContainerProperties"]
    assert cp["Command"] == ["--run-id", "Ref::run-id", "--bucket", "Ref::bucket"]
    # METAL holds ~550 B per distinct marker; 30 GiB is the 4-vCPU Fargate maximum
    assert {r["Type"]: r["Value"] for r in cp["ResourceRequirements"]} == {"VCPU": "4", "MEMORY": "30720"}
    assert jd["Timeout"]["AttemptDurationSeconds"] == 21600
    assert cp["EphemeralStorage"]["SizeInGiB"] == 100
    assert "80M rows" not in text and "2.2 GB" not in text  # stale sizing notes from the old worker


def test_dockerfile_builds_metal_and_sets_entrypoint():
    d = (ROOT / "sgc_metal/Dockerfile").read_text()
    assert "statgen/METAL" in d and "/usr/local/bin/metal" in d
    assert 'ENTRYPOINT["python","-m","sgc_metal.run_metal"]' in d.replace(" ", "")


def test_dockerfile_pins_bases_and_records_metal_commit():
    from sgc_metal import run_metal
    d = (ROOT / "sgc_metal/Dockerfile").read_text()
    froms = re.findall(r"^FROM (\S+)", d, re.M)
    assert froms == ["debian:bookworm-slim", "python:3.12-slim-bookworm"]
    assert "git -C METAL rev-parse HEAD" in d
    assert run_metal.METAL_COMMIT_FILE in d  # where metal_version() reads it
