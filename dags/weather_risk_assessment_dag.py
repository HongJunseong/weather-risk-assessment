# dags/weather_risk_assessment_dag.py

from __future__ import annotations
import os
from datetime import timedelta
from pathlib import Path
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from airflow.operators.bash import BashOperator
from weather_risk_assessment.scripts.upload_bronze_to_s3 import main as upload_bronze_main

from weather_risk_assessment.paths import DATA_ROOT, SINK_DIR
import weather_risk_assessment

JOB_ROOT = Path(weather_risk_assessment.__file__).resolve().parent / "jobs"
SILVER_RISK_JOB = JOB_ROOT / "build_silver_from_bronze.py"
GOLD_LATEST_JOB = JOB_ROOT / "build_gold_risk_latest.py"
GOLD_DAILY_JOB = JOB_ROOT / "build_gold_risk_daily.py"

KST = pendulum.timezone("Asia/Seoul")

# spark-submit 공통
SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)
DELTA_CONF = (
    '--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" '
    '--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" '
    '--conf "spark.sql.session.timeZone=Asia/Seoul" '
)
SPARK_SUBMIT = f'spark-submit --packages "{SPARK_PACKAGES}" {DELTA_CONF}'


# ===== 네트워크 기본값 =====
os.environ.setdefault("KMA_FORCE_HTTP", "1")
os.environ.setdefault("KMA_LOG_LEVEL", "WARNING")


# ===== Scripts =====
from weather_risk_assessment.scripts.make_admin_list import main as make_admin_list_main


def make_admin_centroids_main():
    from weather_risk_assessment.scripts.build_admin_centroids_from_shp import main

    return main()


# ===== Collectors =====
from weather_risk_assessment.collectors.ultra_nowcast_shortfcst import run_once as collect_run_once
from weather_risk_assessment.collectors.short_forecast import collect_short_fcst
from weather_risk_assessment.collectors.typhoon_forecast import fetch_typhoon_forecast_wide
from weather_risk_assessment.collectors.uv_forecast import fetch_and_save_uv_wide
from weather_risk_assessment.alerts.slack_alert import (
    send_high_risk_alerts,
    send_task_failure_alert,
)


default_args = {
    "owner": "junseong",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_task_failure_alert,
}

with DAG(
    dag_id="weather_risk_assessment",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 8, 10, tz=KST),
    # 기상청의 매시 자료 게시 시간을 고려해 정각보다 10분 늦게 실행한다.
    schedule="10 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=55),
    tags=["weather","kma","risk"],
) as dag:

    RUN_DT = "{{ data_interval_end.in_timezone('Asia/Seoul').strftime('%Y%m%d%H') }}"
    RUN_DIR = f"{SINK_DIR}/dt={RUN_DT}"
    
    # 1) 행정 구역 중심점 생성 (lat, lon)
    t_make_admin_centroids = PythonOperator(
        task_id="make_admin_centroids_from_shp", # 행정 구역 경계선 shp 파일이 필요
        python_callable=make_admin_centroids_main # admin_centroids.csv 생성
    )

    # 3) (nx, ny) 중복을 제거한 admin_list 생성
    t_make_admin_list = PythonOperator(
        task_id="make_unique_admin_list",
        python_callable=make_admin_list_main,
    )

    # 4) nowcast/shortfcst collector
    t_collect_kma = PythonOperator(
        task_id="collect_kma_legacy",
        python_callable=collect_run_once,
        op_kwargs={
            "admin_csv": (DATA_ROOT/"unique_admin_centroids.csv").as_posix(),
            "out_dir": RUN_DIR,
            "sample_n": 0,
            "run_dt": RUN_DT,
        },
    )

    t_collect_short_fcst = PythonOperator(
        task_id="collect_short_fcst",
        python_callable=collect_short_fcst,
        op_kwargs={
            "call_list_csv": DATA_ROOT / "unique_admin_centroids.csv",
            "out_path": f"{RUN_DIR}/short_fcst.parquet",
            "run_dt": RUN_DT,
        },
    )

    # 5) typhoon collector
    @task(task_id="collect_typhoon_forecast_wide")
    def collect_typhoon_forecast_wide(run_dt: str, run_dir: str):
        out = fetch_typhoon_forecast_wide(out_path = f"{run_dir}/typhoon.parquet"
                                          ,grid_path = DATA_ROOT / "grid_latlon.parquet"
                                          ,run_dt = run_dt
                                          ,source_dir = run_dir)  # compute_risk_wide는 여기서 읽음
        return str(out)

    # 6) uv collector
    @task(task_id="collect_uv_wide")
    def collect_uv_wide(run_dt: str, run_dir: str):
        # UV API 실패 시 초단기/단기 parquet을 사용해 추정하므로,
        # nowcast/shortfcst 이후에 실행되어야 함
        out = fetch_and_save_uv_wide(
            out_path=Path(run_dir) / "uv.parquet",
            run_dt=run_dt,
            source_dir=Path(run_dir),
        )
        return str(out)

    # 7 Bronze 업로드 (수집된 원천 parquet들을 S3 bronze로)
    t_upload_bronze = PythonOperator(
        task_id="upload_bronze_to_s3",
        python_callable=upload_bronze_main,
        op_kwargs={
            "run_dt": RUN_DT,
            "sink_dir": RUN_DIR,
        },
    )

    # 8) Silver 변환 (S3 bronze -> S3 silver/risk_features Delta)

    t_build_silver = BashOperator(
        task_id="build_silver_risk_enriched",
        bash_command=(
            f'{SPARK_SUBMIT} {SILVER_RISK_JOB} '
            f'--run_dt {RUN_DT} '
            f'--mode overwrite'
        ),
    )

    build_gold_risk_latest = BashOperator(
        task_id="build_gold_risk_latest",
        bash_command=(
            f'{SPARK_SUBMIT} {GOLD_LATEST_JOB}'
        ),
    )

    build_gold_risk_daily = BashOperator(
        task_id="build_gold_risk_daily",
        bash_command=(
            f'{SPARK_SUBMIT} {GOLD_DAILY_JOB}'
        ),
    )

    export_gold_parquet = BashOperator(
        task_id="export_gold_parquet",
        bash_command=(
            f"{SPARK_SUBMIT} {JOB_ROOT / 'export_gold_parquet.py'} --run_dt {RUN_DT}"
        ),
    )

    # HIGH 이상 지역 Slack 알림 (기존 task_id는 실행 이력 호환을 위해 유지)
    t_send_alerts = PythonOperator(
        task_id="send_high_risk_alerts_to_kafka",
        python_callable=send_high_risk_alerts,
    )

    # ===== DAG Task 연결 =====
    typhoon_task = collect_typhoon_forecast_wide(RUN_DT, RUN_DIR)
    uv_task = collect_uv_wide(RUN_DT, RUN_DIR)

    t_make_admin_centroids >> t_make_admin_list >> t_collect_kma >> t_collect_short_fcst\
    >> [typhoon_task, uv_task] >> t_upload_bronze >> t_build_silver >> build_gold_risk_latest \
    >> build_gold_risk_daily >> export_gold_parquet >> t_send_alerts
