FROM python:3.12-slim
WORKDIR /app
COPY sgc_ma/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY dataregistry /app/dataregistry
COPY sgc_qc_plots /app/sgc_qc_plots
COPY sgc_ma /app/sgc_ma
ENV PYTHONPATH=/app
ENTRYPOINT ["python", "-m", "sgc_ma.run_ma"]
