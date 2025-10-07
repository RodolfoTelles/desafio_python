FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y build-essential \
        python3 \
        python3-pip \
        python3-dev \
        python3-venv \
        libpq-dev \
        && \
    apt-get clean

RUN useradd -ms /bin/bash desafio_python
USER desafio_python
WORKDIR /home/desafio_python
