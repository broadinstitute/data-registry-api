FROM hseeberger/scala-sbt:17.0.2_1.6.2_3.1.1

WORKDIR /app
ENV AWS_REGION=us-east-1

RUN apt-get update && apt-get install -y git
RUN git clone https://github.com/broadinstitute/dig-aws.git /app/dig-aws && cd dig-aws && sbt publishLocal
RUN cd /app
RUN git clone https://github.com/broadinstitute/dig-aggregator-core.git /app/dig-aggregator-core && cd dig-aggregator-core && sbt publishLocal
COPY entry.sh /app
RUN chmod +x /app/entry.sh
ENTRYPOINT ["/app/entry.sh"]
CMD ["master", "intake"]


