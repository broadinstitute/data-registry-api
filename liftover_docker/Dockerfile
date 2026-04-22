FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget ca-certificates libcurl4 && \
    rm -rf /var/lib/apt/lists/*

# Download UCSC liftOver binary
RUN wget -q https://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64/liftOver \
    -O /usr/local/bin/liftOver && \
    chmod +x /usr/local/bin/liftOver

# Download chain files
RUN mkdir -p /opt/chains && \
    wget -q https://hgdownload.soe.ucsc.edu/goldenPath/hg38/liftOver/hg38ToHg19.over.chain.gz \
        -O /opt/chains/hg38ToHg19.over.chain.gz && \
    wget -q https://hgdownload.soe.ucsc.edu/goldenPath/hg19/liftOver/hg19ToHg38.over.chain.gz \
        -O /opt/chains/hg19ToHg38.over.chain.gz

WORKDIR /usr/src/app

# Install Python dependencies (separate layer for caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gwas_liftover.py .

ENTRYPOINT ["python3", "gwas_liftover.py"]
