FROM apache/airflow:slim-3.0.1a1-python3.9

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    requests \
    langchain>=0.3.24 \
    langchain-core>=0.1.30 \
    langchain-community>=0.3.21 \
    langchain-qdrant>=0.2.0 \
    qdrant-client>=1.13.3 \
    tqdm \
    sentence-transformers \
    langchain-huggingface \
    fastembed