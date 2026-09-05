FROM python:3.11-slim-bookworm

USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/opt/project/code

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/project
CMD ["sleep", "infinity"]
