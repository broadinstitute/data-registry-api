FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    # Preconfigure the tzdata package before installing r-base
    apt-get install -y --no-install-recommends tzdata && \
    # Set your timezone if needed; for example, use Etc/UTC or your specific timezone
    ln -fs /usr/share/zoneinfo/Etc/UTC /etc/localtime && \
    dpkg-reconfigure --frontend noninteractive tzdata && \
    apt-get install -y r-base libssl-dev libcurl4-openssl-dev libfontconfig1-dev libxml2-dev libharfbuzz-dev \
    libfribidi-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev libgmp-dev cmake libudunits2-dev libgdal-dev

# Reset DEBIAN_FRONTEND to avoid affecting other commands and containers based on this image.
ENV DEBIAN_FRONTEND=
ARG GITHUB_PAT
ENV GITHUB_PAT=${GITHUB_PAT}

RUN R -e "install.packages('devtools')" && R -e "install.packages('data.table')" && R -e "install.packages('furrr')" \
    && R -e "install.packages('fst')" && R -e "Sys.setenv(GITHUB_PAT = Sys.getenv('GITHUB_PAT'))" \
    && R -e "devtools::install_github('nicksunderland/genepi.utils')"


WORKDIR /usr/src/app

COPY requirements.txt hermes_qc.py ./

RUN apt-get install -y python3-pip python3.12-venv && python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip3 install --no-cache-dir -r requirements.txt

