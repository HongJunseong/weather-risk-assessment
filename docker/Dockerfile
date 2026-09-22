FROM apache/airflow:2.7.3-python3.11

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

COPY requirements/runtime.txt /requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt && pip check
