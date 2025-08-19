# dags/weather_risk_assessment_dag.py

from __future__ import annotations
import os
from pathlib import Path
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

import sys
# ===== 프로젝트 루트 & 데이터 경로 =====
PROJECT_ROOT = Path(os.getenv("PROJECT_DRE_ROOT", "/opt/airflow")).resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


DATA_ROOT = PROJECT_ROOT / "weather_risk_assessment" / "data"
DATA_ROOT.mkdir(parents=True, exist_ok=True)
SINK_DIR = Path(os.getenv("DRE_SINK_DIR", (DATA_ROOT / "live").as_posix()))

KST = pendulum.timezone("Asia/Seoul")


# ===== 네트워크 기본값 =====
os.environ.setdefault("KMA_FORCE_HTTP", "1")
os.environ.setdefault("KMA_LOG_LEVEL", "WARNING")


# ===== Scripts =====
from weather_risk_assessment.scripts.build_admin_centroids_from_shp import main as make_admin_centroids_main
from weather_risk_assessment.scripts.make_admin_list import main as make_admin_list_main
from weather_risk_assessment.scripts.make_kepler_geojson import main as make_geojson_main
from weather_risk_assessment.scripts.qa_snapshot import main as qa_snapshot_main


# ===== Collectors =====
from weather_risk_assessment.collectors.ultra_nowcast_shortfcst import run_once as collect_run_once
from weather_risk_assessment.collectors.short_forecast import collect_short_fcst
from weather_risk_assessment.collectors.typhoon_forecast import fetch_typhoon_forecast_wide
from weather_risk_assessment.collectors.uv_forecast import fetch_and_save_uv_wide

# ===== Warehouse =====
from weather_risk_assessment.warehouse.risk_history import main as load_risk_history_main


import logging
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, TableName
import tableauserverclient as TSC
import pandas as pd

TABLEAU_SERVER   = os.environ["TABLEAU_SERVER"]
TABLEAU_SITE_ID  = os.environ.get("TABLEAU_SITE_ID", "")
TABLEAU_PAT_NAME = os.environ["TABLEAU_PAT_NAME"]
TABLEAU_PAT_SECRET = os.environ["TABLEAU_PAT_SECRET"]
TABLEAU_PROJECT_NAME = os.environ.get("TABLEAU_PROJECT_NAME", "Default")
TABLEAU_DS_NAME  = os.environ.get("TABLEAU_DS_NAME", "risk_latest")
CSV_PATH   = os.environ.get("CSV_PATH", "/opt/airflow/weather_risk_assessment/data/risk_latest.csv")
HYPER_PATH = os.environ.get("HYPER_PATH", "/opt/airflow/weather_risk_assessment/data/risk_latest.hyper")



default_args = {
    "owner": "junseong",
    "retries": 0,
    #"retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="weather_risk_assessment",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 8, 10, tz=KST),
    #schedule="*/30 * * * *",
    schedule = None,
    catchup=False,
    max_active_runs=1,
    tags=["weather","kma","risk"],
) as dag:
    
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
            "out_dir": SINK_DIR.as_posix(),
            "sample_n": 0,
        },
    )

    t_collect_short_fcst = PythonOperator(
        task_id="collect_short_fcst",
        python_callable=collect_short_fcst,
        op_kwargs={
            "admin_csv": (DATA_ROOT/"unique_admin_centroids.csv").as_posix(),
            "out_dir": SINK_DIR.as_posix(),
            "sample_n": 0,
        },
    )

    # 5) typhoon collector
    @task(task_id="collect_typhoon_forecast_wide")
    def collect_typhoon_forecast_wide():
        out = fetch_typhoon_forecast_wide(out_path = SINK_DIR / "typhoon.parquet"
                                          ,grid_path = DATA_ROOT / "grid_latlon.parquet")  # compute_risk_wide는 여기서 읽음
        return str(out)

    # 6) uv collector
    @task(task_id="collect_uv_wide")
    def collect_uv_wide():
        # UV API 실패 시 초단기/단기 parquet을 사용해 추정하므로,
        # nowcast/shortfcst 이후에 실행되어야 함
        out = fetch_and_save_uv_wide(out_path=SINK_DIR / "uv.parquet")
        return str(out)



    # 7) 위험도 계산 (wide → risk_latest.*)
    @task(task_id="compute_risk_wide")
    def compute_risk_wide():
        import os, sys, subprocess
        from pathlib import Path

        root = Path(os.getenv("PROJECT_DRE_ROOT", "/opt/airflow")).resolve()
        env = os.environ.copy()
        env["PYTHONPATH"] = (env.get("PYTHONPATH", "") + f":{root.as_posix()}").lstrip(":")

        subprocess.run(
            [sys.executable, "-u", str(root / "weather_risk_assessment" / "scripts" / "compute_risk.py")],
            cwd=root.as_posix(),
            env=env,
            check=True,  # 실패 시 예외 발생 → 태스크 실패
        )

    # 8) GeoJSON & QA
    t_geojson = PythonOperator(
        task_id="make_geojson",
        python_callable=make_geojson_main,
        op_kwargs={"run_dir": SINK_DIR.as_posix()},
    )
    t_qa = PythonOperator(
        task_id="qa_snapshot",
        python_callable=qa_snapshot_main,
        op_kwargs={"run_dir": SINK_DIR.as_posix()},
    )

    t_load_risk_hisotry = PythonOperator(
        task_id="load_risk_history",
        python_callable=load_risk_history_main,
    )

    # 9) HYPER로 변환 후 Tableau Cloud에 게시
    @task(task_id="csv_to_hyper")
    def csv_to_hyper(csv_path: str = CSV_PATH, hyper_path: str = HYPER_PATH) -> str:
        df = pd.read_csv(csv_path)

        def infer_sqltype(s: pd.Series):
            if pd.api.types.is_integer_dtype(s): return SqlType.big_int()
            if pd.api.types.is_float_dtype(s):   return SqlType.double()
            if pd.api.types.is_bool_dtype(s):    return SqlType.bool()
            return SqlType.text()

        table = TableName("Extract", "Extract")
        cols = [TableDefinition.Column(str(c), infer_sqltype(df[c])) for c in df.columns]
        tdef = TableDefinition(table_name=table, columns=cols)

        if os.path.exists(hyper_path):
            os.remove(hyper_path)

        with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(hp.endpoint, database=hyper_path, create_mode=CreateMode.CREATE_AND_REPLACE) as conn:
                conn.catalog.create_schema("Extract")
                conn.catalog.create_table(tdef)
                with Inserter(conn, tdef) as ins:
                    ins.add_rows(df.itertuples(index=False, name=None))
                    ins.execute()
        logging.info("HYPER created: %s", hyper_path)
        return hyper_path
    
    @task(task_id="publish_overwrite")
    def publish_overwrite(hyper_path: str):
        server = TSC.Server(TABLEAU_SERVER, use_server_version=True)
        auth = TSC.PersonalAccessTokenAuth(TABLEAU_PAT_NAME, TABLEAU_PAT_SECRET, site_id=TABLEAU_SITE_ID)
        server.auth.sign_in(auth)
        try:
            # 프로젝트 찾기
            project_id = None
            for p in TSC.Pager(server.projects):
                if p.name == TABLEAU_PROJECT_NAME:
                    project_id = p.id; break
            if not project_id:
                raise RuntimeError(f"Project not found: {TABLEAU_PROJECT_NAME}")

            # 이름으로 데이터소스 탐색
            exists = None
            for ds in TSC.Pager(server.datasources):
                if ds.name == TABLEAU_DS_NAME:
                    exists = ds; break

            item = TSC.DatasourceItem(project_id=project_id, name=TABLEAU_DS_NAME)
            mode = TSC.Server.PublishMode.Overwrite if exists else TSC.Server.PublishMode.CreateNew
            server.datasources.publish(item, hyper_path, mode=mode)
            logging.info("Published to Tableau Cloud: %s (mode=%s)", TABLEAU_DS_NAME, mode)
        finally:
            server.auth.sign_out()

    # ===== DAG Task 연결 =====
    typhoon_task  = collect_typhoon_forecast_wide()
    uv_task = collect_uv_wide()
    compute = compute_risk_wide()

    hyper = csv_to_hyper()
    pub = publish_overwrite(hyper)

    t_make_admin_centroids >> t_make_admin_list >> t_collect_kma >> t_collect_short_fcst\
    >> [typhoon_task, uv_task] >> compute >> t_load_risk_hisotry \
    >> [t_geojson, t_qa] >> hyper >> pub