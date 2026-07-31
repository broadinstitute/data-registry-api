FROM python:3.12-slim
WORKDIR /app
COPY hcm_ma/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY dataregistry /app/dataregistry
COPY sgc_qc_plots /app/sgc_qc_plots
COPY sgc_ma /app/sgc_ma
COPY hcm_ma /app/hcm_ma
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "-m", "hcm_ma.run_ma"]
