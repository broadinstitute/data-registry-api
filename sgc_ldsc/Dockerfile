FROM python:3.11-slim

WORKDIR /usr/src/app

# Build context must be the repo root so the COPY dataregistry below works.
# Build with: docker build -f sgc_ldsc/Dockerfile -t sgc-gwas-ldsc .

COPY sgc_ldsc/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Include both the worker package and the dataregistry helpers it imports:
# DataRegistryReadWriteDB for DB access, and query for the ldsc_* writers.
COPY sgc_ldsc ./sgc_ldsc
COPY dataregistry ./dataregistry

ENV PYTHONPATH=/usr/src/app
ENTRYPOINT ["python3", "-m", "sgc_ldsc.ldsc_worker"]
