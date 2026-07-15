# QC pipeline env image

The environment image is built from the `qc-pipeline` repo, not this one:

    cd /path/to/qc-pipeline
    docker build -f docker/default/Dockerfile -t <acct>.dkr.ecr.us-east-1.amazonaws.com/qc-pipeline-env-repo:<tag> .

It bakes only the environment (deps + git + `orchestrator/`) with `aws_wrapper` as the
entrypoint; scripts/pipelines/resources come from the git clone at run time.
Pin the deployed API to a specific image digest and a specific qc-pipeline commit.
