FROM docker.apple.com/base-images/ubi8/python3.9-builder:latest

ENV STARGATE_USE_SUPPLIED_PYTHON_RUNNER="true"

WORKDIR /app/sampleapp

ENV PYTHONPATH="/app/sampleapp"

COPY examples/python-transformers/embedded/requirements.txt .

RUN pip install -r requirements.txt

COPY examples/python-transformers/embedded/src .