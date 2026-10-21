# Build context is the repo root:
#   docker build -f sgc_munge/Dockerfile -t sgc-gwas-munge .
FROM python:3.12-slim
WORKDIR /app
COPY sgc_munge/requirements.txt /app/requirements.txt
# pysam ships manylinux wheels for 3.12, so no build deps are needed. Only if the wheel
# install ever fails, apt-get install -y --no-install-recommends
#   build-essential zlib1g-dev libbz2-dev liblzma-dev libcurl4-openssl-dev
# bcftools streams the slim dbSNP once per file (sgc_munge/rsid.py); Debian bookworm ships 1.16
RUN apt-get update && apt-get install -y --no-install-recommends bcftools \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY dataregistry /app/dataregistry
COPY sgc_qc_plots /app/sgc_qc_plots
COPY sgc_ma /app/sgc_ma
COPY sgc_munge /app/sgc_munge
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "-m", "sgc_munge.munge"]
