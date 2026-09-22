FROM apache/airflow:2.7.3-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends msopenjdk-17 && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/msopenjdk-17-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

COPY requirements/runtime.txt /requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt && pip check
