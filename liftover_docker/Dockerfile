FROM python:3.9-slim

RUN pip install pyliftover pandas argparse boto3

WORKDIR /app

COPY gwas_liftover.py /app/

RUN chmod +x /app/gwas_liftover.py

