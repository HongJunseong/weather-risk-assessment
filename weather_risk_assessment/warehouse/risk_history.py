# warehouse/risk_history.py
from __future__ import annotations
import os
import pandas as pd
import pendulum
import re
from sqlalchemy import create_engine, text

KST = pendulum.timezone("Asia/Seoul")

# --- DB 연결 URL (.env에서 읽음) ---
def pg_url() -> str:
    host = os.getenv("PGHOST", "postgres")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER", "dre_user")
    pwd  = os.getenv("PGPASSWORD", "dre_pass")
    db   = os.getenv("PGDATABASE", "dre_db")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

# --- 테이블 보장 ---
def ensure_table(engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS risk_history_wide (
      nx            INTEGER NOT NULL,
      ny            INTEGER NOT NULL,
      admin_names   TEXT,
      base_date     DATE,
      fcst_date     DATE,
      fcst_time     TIMESTAMPTZ NOT NULL,
      source_run_at TIMESTAMPTZ NOT NULL,
      RN1           DOUBLE PRECISION,
      WSD           DOUBLE PRECISION,
      UUU           DOUBLE PRECISION,
      VVV           DOUBLE PRECISION,
      T1H           DOUBLE PRECISION,
      REH           DOUBLE PRECISION,
      PTY           DOUBLE PRECISION,
      SKY           DOUBLE PRECISION,
      UVI           DOUBLE PRECISION,
      R_rain        DOUBLE PRECISION,
      R_heat        DOUBLE PRECISION,
      R_wind        DOUBLE PRECISION,
      R_uv          DOUBLE PRECISION,
      R_typhoon     DOUBLE PRECISION,
      R_total       DOUBLE PRECISION,
      CONSTRAINT pk_rhw PRIMARY KEY (nx, ny, fcst_time)
    );
    CREATE INDEX IF NOT EXISTS idx_rhw_fcst ON risk_history_wide (fcst_time);
    """
    with engine.begin() as conn:
        for stmt in ddl.strip().split(";"):
            s = stmt.strip()
            if s:
                conn.execute(text(s))

# --- 입력 스키마 ---
EXPECTED_COLS = [
    "nx","ny","admin_names","baseDate","fcstDate","fcstTime",
    "RN1","WSD","UUU","VVV","T1H","REH","PTY","SKY","UVI",
    "R_rain","R_heat","R_wind","R_uv","R_typhoon","R_total"
]

# --- 최신 산출물 로드 (csv/parquet) ---
def load_latest(path: str) -> pd.DataFrame:
    if path.endswith(".parquet"):
        df = pd.read_parquet(path)
    elif path.endswith(".csv"):
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported file type: {path}")
    df.columns = [c.strip() for c in df.columns]

    # 필수 키 체크
    req = {"nx","ny","fcstDate","fcstTime"}
    missing = req - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # 누락 컬럼은 생성 후 원하는 순서로 정리
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = None
    return df[EXPECTED_COLS].copy()

# --- fcstDate/fcstTime → UTC 타임스탬프 ---
def parse_fcst_ts(fcst_date: str, fcst_time: str) -> pendulum.DateTime:
    # 허용 포맷: 'YYYYMMDD'/'YYYY-MM-DD', 'H'/'HH'/'HHm'/'HHmm'/'HH:mm'
    d = str(fcst_date).replace("-", "")
    t = str(fcst_time).replace(":", "")
    if len(t) == 1:      t = f"0{t}00"
    elif len(t) == 2:    t = f"{t}00"
    elif len(t) == 3:    t = f"0{t}"
    elif len(t) >= 4:    t = t[:4]
    ts = f"{d} {t[:2]}:{t[2:]}"
    dt = pendulum.from_format(ts, "YYYYMMDD HH:mm", tz=KST)
    return dt.in_timezone("UTC")

# --- 문자열 수치 정규화 ---
def normalize_numeric_with_units(x):
    """'3.2mm', '1 m/s', 'NaN', '' → float 또는 None."""
    if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip() == ""):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if "없음" in s or s.lower() in {"none", "no", "null"}:
        return 0.0
    m = re.search(r'[-+]?\d*\.?\d+', s)
    return float(m.group()) if m else None

def normalize_rn1(x):
    """강수량 전처리: '강수 없음' 계열은 0.0, 나머지는 숫자 추출."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    if "강수없음" in s or "강수 없음" in s:
        return 0.0
    return normalize_numeric_with_units(s)

# --- 메인 파이프라인 ---
def main(risk_latest_path: str | None = None):
    # 입력 경로(.env의 RISK_LATEST_PATH 우선)
    risk_latest_path = risk_latest_path or os.getenv(
        "RISK_LATEST_PATH",
        "/opt/airflow/weather_risk_assessment/data/risk_latest.parquet"
    )

    # 1) 연결/테이블 준비
    engine = create_engine(pg_url())
    ensure_table(engine)

    # 2) 최신 산출물 로드
    df = load_latest(risk_latest_path)

    # 3) 시간 컬럼 생성
    src_run_at = pendulum.now(tz=KST).in_timezone("UTC")
    df["fcst_time"] = [parse_fcst_ts(fd, ft) for fd, ft in zip(df["fcstDate"], df["fcstTime"])]
    df["base_date"]   = pd.to_datetime(df["baseDate"], errors="coerce").dt.date
    df["fcst_date"]   = pd.to_datetime(df["fcstDate"], errors="coerce").dt.date
    df["source_run_at"] = src_run_at

    # 4) 값 정규화/캐스팅
    if "RN1" in df.columns:
        df["RN1"] = df["RN1"].apply(normalize_rn1)

    num_cols = ["RN1","WSD","UUU","VVV","T1H","REH","PTY","SKY","UVI",
                "R_rain","R_heat","R_wind","R_uv","R_typhoon","R_total"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 5) UPSERT 페이로드
    payload_cols = [
        "nx","ny","admin_names","base_date","fcst_date","fcst_time","source_run_at",
        "RN1","WSD","UUU","VVV","T1H","REH","PTY","SKY","UVI",
        "R_rain","R_heat","R_wind","R_uv","R_typhoon","R_total"
    ]
    recs = df[payload_cols].to_dict(orient="records")

    # 6) UPSERT (source_run_at 최신 것만 반영)
    upsert_sql = text("""
    INSERT INTO risk_history_wide
      (nx, ny, admin_names, base_date, fcst_date, fcst_time, source_run_at,
       RN1, WSD, UUU, VVV, T1H, REH, PTY, SKY, UVI,
       R_rain, R_heat, R_wind, R_uv, R_typhoon, R_total)
    VALUES
      (:nx, :ny, :admin_names, :base_date, :fcst_date, :fcst_time, :source_run_at,
       :RN1, :WSD, :UUU, :VVV, :T1H, :REH, :PTY, :SKY, :UVI,
       :R_rain, :R_heat, :R_wind, :R_uv, :R_typhoon, :R_total)
    ON CONFLICT ON CONSTRAINT pk_rhw
    DO UPDATE SET
    base_date = EXCLUDED.base_date,
    fcst_date = EXCLUDED.fcst_date,
    source_run_at = EXCLUDED.source_run_at,
    RN1 = EXCLUDED.RN1, WSD = EXCLUDED.WSD, UUU = EXCLUDED.UUU, VVV = EXCLUDED.VVV,
    T1H = EXCLUDED.T1H, REH = EXCLUDED.REH, PTY = EXCLUDED.PTY, SKY = EXCLUDED.SKY, UVI = EXCLUDED.UVI,
    R_rain = EXCLUDED.R_rain, R_heat = EXCLUDED.R_heat, R_wind = EXCLUDED.R_wind,
    R_uv = EXCLUDED.R_uv, R_typhoon = EXCLUDED.R_typhoon, R_total = EXCLUDED.R_total,
    admin_names = EXCLUDED.admin_names
    WHERE risk_history_wide.source_run_at IS NULL
    OR EXCLUDED.source_run_at >= risk_history_wide.source_run_at;
    """)

    with engine.begin() as conn:
        conn.execute(upsert_sql, recs)

    print(f"[OK] wide history upserted: rows={len(recs)}")

if __name__ == "__main__":
    main()
