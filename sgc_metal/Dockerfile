# SGC bottom-line meta-analysis worker (METAL). Build context is the repo root:
#   docker build -f sgc_metal/Dockerfile -t sgc-gwas-metal .
FROM debian:bookworm-slim AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git cmake g++ make zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 --branch 2020-05-05 https://github.com/statgen/METAL.git \
    && git -C METAL rev-parse HEAD | tee /metal_commit.txt
RUN cmake -S METAL -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j
# The binary is produced at build/metal/metal (the install rule is not usable here).

FROM python:3.12-slim-bookworm
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends zlib1g \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/metal/metal /usr/local/bin/metal
# metal_version() appends this commit to the engine string in summary.json
COPY --from=builder /metal_commit.txt /usr/local/share/metal_commit.txt
COPY sgc_ma/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY dataregistry /app/dataregistry
COPY sgc_qc_plots /app/sgc_qc_plots
COPY sgc_ma /app/sgc_ma
COPY sgc_munge /app/sgc_munge
COPY sgc_metal /app/sgc_metal
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "-m", "sgc_metal.run_metal"]
