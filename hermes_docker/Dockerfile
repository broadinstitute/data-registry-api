FROM rocker/r-ver:4
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y liblzma-dev libbz2-dev libcurl4-openssl-dev libssl-dev libxml2-dev zlib1g-dev python3 python3-pip python3-venv pandoc


# Reset DEBIAN_FRONTEND to avoid affecting other commands and containers based on this image.
ENV DEBIAN_FRONTEND=
ARG GITHUB_PAT
ENV GITHUB_PAT=${GITHUB_PAT}

RUN R -e "install.packages(c('devtools', 'data.table', 'furrr', 'R.utils', 'fst', 'stringi', 'BiocManager', 'argparse', 'rmarkdown'))" && \
    R -e 'BiocManager::install(c("GenomicRanges", "IRanges", "rtracklayer"))'


WORKDIR /usr/src/app
RUN R -e "devtools::install_github('nicksunderland/genepi.utils')"
RUN apt-get install -y git && \
    git clone --depth 1 -b main https://github.com/nicksunderland/heRmes.git && \
    cd heRmes && \
    git pull origin main


COPY requirements.txt hermes_qc.py ./

ENV PATH="/opt/venv/bin:$PATH"
RUN python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

