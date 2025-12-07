# dags/weather_risk_streaming_dag.py
from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

KST = pendulum.timezone("Asia/Seoul")

# 프로젝트 루트 설정 (기존 DAG와 동일한 방식)
PROJECT_ROOT = Path(os.getenv("PROJECT_DRE_ROOT", "/opt/airflow")).resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from weather_risk_assessment.streaming.kafka_producer_risk_latest import main as push_risk_to_kafka


default_args = {
    "owner": "junseong",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


with DAG(
    dag_id="weather_risk_streaming",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 8, 10, tz=KST),
    schedule="*/30 * * * *",   # 30분마다 risk_latest → Kafka 로 전송
    catchup=False,
    max_active_runs=1,
    tags=["weather", "kafka", "streaming"],
) as dag:

    t_push_risk_latest = PythonOperator(
        task_id="push_risk_latest_to_kafka",
        python_callable=push_risk_to_kafka,
    )
