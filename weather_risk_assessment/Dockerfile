FROM apache/airflow:2.7.3-python3.11

USER root

COPY requirements.txt /requirements.txt
RUN python -m pip install --upgrade pip

USER airflow

RUN pip install apache-airflow==${AIRFLOW_VERSION} -r /requirements.txt