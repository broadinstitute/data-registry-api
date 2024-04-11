FROM rocker/r-ver:4
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y liblzma-dev libbz2-dev libcurl4-openssl-dev libssl-dev libxml2-dev zlib1g-dev


# Reset DEBIAN_FRONTEND to avoid affecting other commands and containers based on this image.
ENV DEBIAN_FRONTEND=
ARG GITHUB_PAT
ENV GITHUB_PAT=${GITHUB_PAT}

RUN R -e "install.packages('devtools')" && R -e "install.packages('data.table')" && R -e "install.packages('furrr')" \
    && R -e "install.packages('fst')" && R -e "install.packages('stringi')"

RUN R -e "devtools::install_github('nicksunderland/genepi.utils')" > /tmp/install_genepi.utils.log 2>&1


WORKDIR /usr/src/app

COPY requirements.txt hermes_qc.py ./

RUN apt-get install -y python3 python3-pip python3-venv

ENV PATH="/opt/venv/bin:$PATH"
RUN pip3 install --no-cache-dir -r requirements.txt

