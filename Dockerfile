FROM python:3.8-slim
WORKDIR /usr/src/app
COPY requirements.txt .
COPY dataregistry ./dataregistry
RUN apt-get update && \
    apt-get install -y git && \
    pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
CMD ["python", "-m", "dataregistry.main", "-e", "dataregistry/.env",  "serve"]

