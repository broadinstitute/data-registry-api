FROM python:3.9-slim

# Install dependencies
RUN pip install liftover pandas argparse boto3

# Create app directory
WORKDIR /app

# Create script to download chain files
COPY download_chains.py /app/

# Download chain files during build
RUN python /app/download_chains.py

# Copy liftover script
COPY gwas_liftover.py /app/

# Set permissions
RUN chmod +x /app/gwas_liftover.py

# Set entrypoint
ENTRYPOINT ["python", "/app/gwas_liftover.py"]
