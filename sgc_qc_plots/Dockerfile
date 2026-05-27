FROM python:3.11-slim

WORKDIR /usr/src/app

# Build context must be the repo root so the COPY dataregistry below works.
# Build with: docker build -f sgc_qc_plots/Dockerfile -t sgc-qc-plots .

COPY sgc_qc_plots/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Include both the worker package and the dataregistry helpers it imports
# for DB access (DataRegistryReadWriteDB).
COPY sgc_qc_plots ./sgc_qc_plots
COPY dataregistry ./dataregistry

ENV PYTHONPATH=/usr/src/app
ENTRYPOINT ["python3", "-m", "sgc_qc_plots.qc_plots"]
