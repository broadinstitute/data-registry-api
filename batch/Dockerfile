# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /usr/src/app

COPY requirements.txt sort_file.py ./

RUN pip install --no-cache-dir -r requirements.txt


