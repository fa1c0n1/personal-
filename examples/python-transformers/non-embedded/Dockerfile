FROM docker.apple.com/base-images/ubi8/python3.9-builder:latest

WORKDIR /app/sampleapp

ENV PYTHONPATH="/app/sampleapp"

COPY examples/python-transformers/non-embedded/requirements.txt .

RUN pip install -r requirements.txt

COPY examples/python-transformers/non-embedded/src .