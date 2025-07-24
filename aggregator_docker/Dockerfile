FROM hseeberger/scala-sbt:17.0.2_1.6.2_3.1.1

WORKDIR /app
ENV AWS_REGION=us-east-1

RUN apt-get update && apt-get install -y \
    git \
    zstd \
    python3 \
    python3-pip

RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install numpy pandas boto3 statsmodels matplotlib awscli

RUN git clone https://github.com/broadinstitute/dig-aws.git /app/dig-aws && cd dig-aws && sbt publishLocal
RUN cd /app
RUN git clone https://github.com/broadinstitute/dig-aggregator-core.git /app/dig-aggregator-core && cd dig-aggregator-core && sbt publishLocal
COPY entry.sh /app
COPY plotMetaAnalysis.py /app
RUN chmod +x /app/entry.sh
ENTRYPOINT ["/app/entry.sh"]
CMD ["dig-data-registry-qa", "test-ma", "master", "intake bottom-line:PartitionStage bottom-line:NaiveStage", "--no-insert-runs --yes"]


