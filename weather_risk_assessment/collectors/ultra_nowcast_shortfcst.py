# collectors/kma_nowcast_shortfcst.py
from __future__ import annotations

import os
import sys
import time
import logging
from pathlib import Path
from typing import Dict, Iterable, Tuple
from datetime import datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

# 루트 경로 추가 + .env 로드
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from utils.latlon_to_grid import latlon_to_grid
from utils.time_kst import now_kst

# 불필요한 requests 로그 끔
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
logging.getLogger("urllib3.util.retry").setLevel(logging.ERROR)

# 로그 레벨 (.env에서 조정 가능)
LOG_LEVEL = os.getenv("KMA_LOG_LEVEL", "WARNING").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(message)s")
logger = logging.getLogger("kma")

# API 설정
API_KEY  = os.getenv("KMA_API_KEY", "").strip()
BASE_URL = os.getenv("KMA_BASE_URL", "https://apis.data.go.kr/1360000").rstrip("/")
SVC_PATH = os.getenv("KMA_SVC_PATH", "VilageFcstInfoService_2.0").strip("/")
EP_NCST  = os.getenv("KMA_ULTRA_SRT_NCST", "getUltraSrtNcst")   # 초단기실황
EP_FCST  = os.getenv("KMA_ULTRA_SRT_FCST", "getUltraSrtFcst")   # 초단기예보

# 네트워크 옵션
FORCE_HTTP = os.getenv("KMA_FORCE_HTTP", "0") == "1"
SKIP_SSL   = os.getenv("KMA_SKIP_SSL_VERIFY", "0") == "1"

def build_url(ep: str) -> str:
    return f"{BASE_URL}/{SVC_PATH}/{ep}"

# 인코딩된 서비스키 경고
if "%" in API_KEY:
    logger.warning("[WARN] API_KEY가 인코딩된 값일 수 있음. 일반 서비스키 사용 권장.")

# 세션 + 재시도
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})
retries = Retry(total=2, backoff_factor=0.4, status_forcelist=[502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://",  HTTPAdapter(max_retries=retries))

# 응답 파싱
def _parse_response(r: requests.Response):
    ct = (r.headers.get("content-type") or "").lower()
    head = (r.text or "")[:200].replace("\n", " ")
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"KMA HTTP {r.status_code}: {head}") from e
    if "json" not in ct:
        raise RuntimeError(f"KMA NON_JSON: {ct} head={head}")
    try:
        js = r.json()
    except requests.exceptions.JSONDecodeError as e:
        raise RuntimeError(f"KMA JSON_DECODE head={head}") from e

    header = js.get("response", {}).get("header", {})
    if header.get("resultCode") != "00":
        raise RuntimeError(f"KMA API error: {header}")
    return js["response"]["body"]["items"]["item"]

# baseDate/baseTime 후보 (실황)
def _iter_ncst_candidates(now: datetime | None = None, tries: int = 4) -> Iterable[Tuple[str, str]]:
    now = now or now_kst()
    t = now.replace(minute=0, second=0, microsecond=0)
    if now.minute < 10:
        t -= timedelta(hours=1)
    for _ in range(tries):
        yield t.strftime("%Y%m%d"), t.strftime("%H") + "00"
        t -= timedelta(hours=1)

# baseDate/baseTime 후보 (예보)
def _iter_fcst_candidates(now: datetime | None = None, tries: int = 4) -> Iterable[Tuple[str, str]]:
    now = now or now_kst()
    t = now.replace(minute=30, second=0, microsecond=0)
    if now.minute < 45:
        t -= timedelta(hours=1)
    for _ in range(tries):
        yield t.strftime("%Y%m%d"), t.strftime("%H") + "30"
        t -= timedelta(hours=1)

# API 호출 (https → ssl off → http fallback)
@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=0.6, max=3))
def call_kma(url: str, params: Dict):
    if FORCE_HTTP:
        url = url.replace("https://", "http://")
        r = session.get(url, params=params, timeout=15)
        r.raise_for_status()
        return _parse_response(r)

    try:
        r = session.get(url, params=params, timeout=15)
        r.raise_for_status()
        return _parse_response(r)
    except requests.exceptions.SSLError:
        pass

    if SKIP_SSL:
        try:
            r = session.get(url, params=params, timeout=15, verify=False)
            r.raise_for_status()
            return _parse_response(r)
        except requests.exceptions.SSLError:
            pass

    url_http = url.replace("https://", "http://")
    r = session.get(url_http, params=params, timeout=15)
    r.raise_for_status()
    return _parse_response(r)

# 좌표 보정
def _ensure_coords(df: pd.DataFrame, nx: int, ny: int) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "nx" not in df.columns:
        df["nx"] = nx
    if "ny" not in df.columns:
        df["ny"] = ny
    return df

# 초단기실황 조회
def fetch_ultra_srt_nc(nx: int, ny: int, base_date: str | None = None, base_time: str | None = None) -> pd.DataFrame:
    url = build_url(EP_NCST)
    candidates = [(base_date, base_time)] if (base_date and base_time) else _iter_ncst_candidates()
    for bd, bt in candidates:
        params = {
            "serviceKey": API_KEY, "dataType": "JSON",
            "numOfRows": 1000, "pageNo": 1,
            "base_date": bd, "base_time": bt,
            "nx": nx, "ny": ny,
        }
        try:
            items = call_kma(url, params)
            df = pd.DataFrame(items)
            df = _ensure_coords(df, nx, ny)
            if not df.empty:
                return df
        except RuntimeError as e:
            if "NO_DATA" in str(e):
                continue
            raise
    return pd.DataFrame()

# 초단기예보 조회
def fetch_ultra_srt_fc(nx: int, ny: int, base_date: str | None = None, base_time: str | None = None) -> pd.DataFrame:
    url = build_url(EP_FCST)
    candidates = [(base_date, base_time)] if (base_date and base_time) else _iter_fcst_candidates()
    for bd, bt in candidates:
        params = {
            "serviceKey": API_KEY, "dataType": "JSON",
            "numOfRows": 1000, "pageNo": 1,
            "base_date": bd, "base_time": bt,
            "nx": nx, "ny": ny,
        }
        try:
            items = call_kma(url, params)
            df = pd.DataFrame(items)
            df = _ensure_coords(df, nx, ny)
            if not df.empty:
                return df
        except RuntimeError as e:
            if "NO_DATA" in str(e):
                continue
            raise
    return pd.DataFrame()

# 실황/예보 wide pivot
def normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    val_col = "obsrValue" if "obsrValue" in df.columns else "fcstValue"
    idx_cols = [c for c in ["baseDate","baseTime","nx","ny","fcstDate","fcstTime"] if c in df.columns]
    piv = df.pivot_table(index=idx_cols, columns="category", values=val_col, aggfunc="last").reset_index()
    piv.columns.name = None
    return piv

# admin csv 읽기
def _read_admin_csv(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.read_csv(path, encoding="utf-8-sig")

# 실행: 수집 후 parquet 저장
def run_once(admin_csv: str, out_dir: str = None, sample_n: int = 20, save_every: int = 100):
    out_dir = out_dir or (ROOT / "data").as_posix()
    cent = _read_admin_csv(admin_csv)
    if sample_n:
        cent = cent.head(sample_n)

    out_nc, out_fc = [], []
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    pbar = tqdm(cent.itertuples(index=False), total=len(cent), desc="KMA collect", unit="cell")
    for i, row in enumerate(pbar, 1):
        nx, ny = int(getattr(row, "nx")), int(getattr(row, "ny"))

        df_nc = fetch_ultra_srt_nc(nx, ny)
        df_fc = fetch_ultra_srt_fc(nx, ny)

        if len(df_nc): out_nc.append(normalize(df_nc))
        if len(df_fc): out_fc.append(normalize(df_fc))

        pbar.set_postfix(nx=nx, ny=ny, nc=len(df_nc), fc=len(df_fc))
        time.sleep(0.15)

        if save_every and i % save_every == 0:
            if out_nc:
                pd.concat(out_nc, ignore_index=True).to_parquet(Path(out_dir, "ultra_nowcast.parquet"), index=False)
            if out_fc:
                pd.concat(out_fc, ignore_index=True).to_parquet(Path(out_dir, "ultra_shortfcst.parquet"), index=False)

    n_nc = n_fc = 0
    if out_nc:
        df_nc_all = pd.concat(out_nc, ignore_index=True); n_nc = len(df_nc_all)
        df_nc_all.to_parquet(Path(out_dir, "ultra_nowcast.parquet"), index=False)
    if out_fc:
        df_fc_all = pd.concat(out_fc, ignore_index=True); n_fc = len(df_fc_all)
        df_fc_all.to_parquet(Path(out_dir, "ultra_shortfcst.parquet"), index=False)

    print(f"[SAVE] {out_dir}  nowcast={n_nc}, shortfcst={n_fc}")

if __name__ == "__main__":
    run_once((ROOT / "data" / "unique_admin_centroids.csv").as_posix())
