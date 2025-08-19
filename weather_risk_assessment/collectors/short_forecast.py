# collectors/short_forecast.py

from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import os, json, time, logging, random

import numpy as np
import pandas as pd
import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------- paths & env ----------------
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
LIVE = DATA / "live"
LIVE.mkdir(parents=True, exist_ok=True)

CALL_LIST   = DATA / "unique_admin_centroids.csv"   # columns: nx,ny[,admin_code|areaNo]
DEFAULT_OUT = LIVE / "short_fcst.parquet"

# ---------------- ENV ----------------
BASE_URL   = os.getenv("KMA_BASE_URL", "https://apis.data.go.kr/1360000").strip()
SVC_PATH   = os.getenv("KMA_VILAGE_SVC_PATH", "VilageFcstInfoService_2.0").strip("/")
EP_VILAGE  = os.getenv("KMA_VILAGE_EP", "getVilageFcst").strip()
API_KEY    = os.getenv("KMA_API_KEY", "")  # URL-encoded serviceKey (필수)
DATA_TYPE  = os.getenv("KMA_DATA_TYPE", "JSON").upper()  # JSON 권장
FORCE_HTTP = os.getenv("KMA_FORCE_HTTP", "0") == "1"

# 성능/안정성 설정
HORIZON_HOURS    = int(os.getenv("KMA_HORIZON_HOURS", "6"))       # 예보 +H시간만
ISSUE_BUFFER_MIN = int(os.getenv("KMA_ISSUE_BUFFER_MIN", "25"))   # 발표 직후 버퍼(분)
MAX_WORKERS      = int(os.getenv("KMA_MAX_WORKERS", "8"))         # 동시 요청 수(권장 4~10)
REQ_TIMEOUT_SEC  = float(os.getenv("KMA_REQ_TIMEOUT_SEC", "12"))  # 요청 타임아웃

# numOfRows를 "고정 1000"으로 사용
NUM_ROWS_SINGLE  = int(os.getenv("KMA_NUM_ROWS_SINGLE", "1000"))

# 카테고리(필요한 것만 남기면 더 빨라짐)
CATEGORIES = os.getenv("KMA_VILAGE_CATEGORIES",
                       "POP,PCP,PTY,TMP,REH,WSD,SKY").replace(" ", "").split(",")

# 단기예보 발표시각(KST)
ISSUE_HOURS = [2, 5, 8, 11, 14, 17, 20, 23]

log = logging.getLogger(__name__)
if not log.handlers:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")


# ---------------- utils ----------------
def _session() -> requests.Session:
    """스레드(워커) 전용 세션 생성."""
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS))
    s.mount("http://",  HTTPAdapter(max_retries=retries, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS))
    s.headers.update({"User-Agent": "weather-risk-assessment/short-forecast"})
    return s

def _normalize_base_url() -> str:
    u = BASE_URL.strip().rstrip("/")
    if u.startswith("http://") or u.startswith("https://"):
        if FORCE_HTTP and u.startswith("https://"):
            u = "http://" + u[len("https://"):]
        return u
    scheme = "http" if FORCE_HTTP else "https"
    return f"{scheme}://{u}"

def _latest_issue_time(now_kst: pendulum.DateTime) -> Tuple[str, str]:
    """
    발표 직후 ISSUE_BUFFER_MIN 분 동안은 직전 발표본 사용(지연/누락 회피).
    """
    now_kst = now_kst.in_timezone("Asia/Seoul").replace(second=0, microsecond=0)
    prev_hours = [h for h in ISSUE_HOURS if h <= now_kst.hour]
    if prev_hours:
        base_dt = now_kst.replace(minute=0, hour=max(prev_hours))
    else:
        base_dt = now_kst.subtract(days=1).replace(minute=0, hour=ISSUE_HOURS[-1])

    if (now_kst - base_dt).in_minutes() < ISSUE_BUFFER_MIN:
        idx = ISSUE_HOURS.index(base_dt.hour)
        if idx == 0:
            base_dt = base_dt.subtract(days=1).replace(hour=ISSUE_HOURS[-1])
        else:
            base_dt = base_dt.replace(hour=ISSUE_HOURS[idx-1])

    return base_dt.format("YYYYMMDD"), base_dt.format("HHmm")

def _target_times_df(baseDate: str, baseTime: str, horizon_h: int,
                     now_kst: Optional[pendulum.DateTime] = None) -> Tuple[pd.DataFrame, pendulum.DateTime]:
    """
    타깃 시각 생성:
    - 기본은 base_ts + 1h 시작
    - now_kst가 더 뒤라면 now.floor('H')부터 시작(다른 지표들과 fcstTime 정렬)
    """
    base_ts = pendulum.from_format(baseDate + baseTime, "YYYYMMDDHHmm", tz="Asia/Seoul")
    start_ts = base_ts.add(hours=1)
    if now_kst is not None:
        now_floor = now_kst.in_timezone("Asia/Seoul").replace(minute=0, second=0, microsecond=0)
        if now_floor > start_ts:
            start_ts = now_floor

    rows = [{"fcstDate": start_ts.add(hours=h).format("YYYYMMDD"),
             "fcstTime": start_ts.add(hours=h).format("HHmm")}
            for h in range(horizon_h)]
    return pd.DataFrame(rows), start_ts

def _build_params(nx: int, ny: int, baseDate: str, baseTime: str,
                  pageNo: int = 1, numOfRows: Optional[int] = None) -> Dict[str, Any]:
    # 고정 1000 사용
    numOfRows = NUM_ROWS_SINGLE if numOfRows is None else numOfRows
    return {
        "serviceKey": API_KEY,
        "pageNo": pageNo,
        "numOfRows": numOfRows,
        "dataType": DATA_TYPE,
        "base_date": baseDate,
        "base_time": baseTime,
        "nx": nx,
        "ny": ny,
    }

def _parse_json_safely(r: requests.Response) -> Optional[dict]:
    txt = r.text or ""
    if not txt.strip():
        return None
    try:
        return r.json()
    except Exception:
        try:
            return json.loads(txt)
        except Exception:
            return None

def _pcp_to_float(val: Any) -> float:
    s = str(val).strip()
    if s in ("", "None", "nan"):
        return np.nan
    if any(k in s for k in ["강수없음", "적설없음", "없음"]):
        return 0.0
    if "미만" in s:
        digits = "".join(ch for ch in s if (ch.isdigit() or ch == "."))
        return 0.5 if not digits else max(0.1, float(digits) * 0.5)
    s2 = s.replace("mm", "")
    try:
        return float(s2)
    except Exception:
        return np.nan

def _postprocess_wide(df_raw: pd.DataFrame, admin_code: Optional[int]) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame()
    df = df_raw[df_raw["category"].isin(CATEGORIES)].copy()
    df["value"] = df["fcstValue"]
    pcp_mask = df["category"].eq("PCP")
    df.loc[pcp_mask, "value"] = df.loc[pcp_mask, "value"].map(_pcp_to_float)
    df.loc[~pcp_mask, "value"] = pd.to_numeric(df.loc[~pcp_mask, "value"], errors="coerce")

    idx_cols = ["nx", "ny", "fcstDate", "fcstTime"]
    df_pvt = (df.pivot_table(index=idx_cols, columns="category", values="value", aggfunc="mean")
                .reset_index())

    if admin_code is not None:
        df_pvt["admin_code"] = int(admin_code)

    ts = pd.to_datetime(df_pvt["fcstDate"] + df_pvt["fcstTime"], format="%Y%m%d%H%M", errors="coerce")
    df_pvt["valid_ts_kst"] = ts.dt.tz_localize("Asia/Seoul", nonexistent="shift_forward", ambiguous="NaT")

    for c in ("PTY", "SKY"):
        if c in df_pvt.columns:
            df_pvt[c] = np.floor(pd.to_numeric(df_pvt[c], errors="coerce")).astype("Int64")
    for c in ("POP", "TMP", "REH", "WSD", "PCP"):
        if c in df_pvt.columns:
            df_pvt[c] = pd.to_numeric(df_pvt[c], errors="coerce")

    lead_cols = ["admin_code", "nx", "ny", "fcstDate", "fcstTime", "valid_ts_kst"]
    lead_cols = [c for c in lead_cols if c in df_pvt.columns]
    ordered = lead_cols + sorted([c for c in df_pvt.columns if c not in lead_cols])
    sort_cols = lead_cols if "admin_code" in df_pvt.columns else ["nx","ny","fcstDate","fcstTime"]
    return df_pvt[ordered].sort_values(sort_cols)


# ---------------- worker (per location) ----------------
def _fetch_vilage_singlepage(nx: int, ny: int, baseDate: str, baseTime: str) -> pd.DataFrame:
    """
    워커에서 호출: 단일 페이지(고정 numOfRows=1000)로 받고, 필요 시 1회 소프트 재시도.
    """
    s = _session()
    base = _normalize_base_url()
    url = f"{base}/{SVC_PATH}/{EP_VILAGE}"
    params = _build_params(nx, ny, baseDate, baseTime, pageNo=1)

    for attempt in range(2):  # 소프트 재시도 1회
        try:
            r = s.get(url, params=params, timeout=REQ_TIMEOUT_SEC)
        except Exception:
            if attempt == 0:
                time.sleep(0.4 + random.random() * 0.4)
                continue
            return pd.DataFrame()

        if r.status_code != 200:
            if attempt == 0:
                time.sleep(0.4 + random.random() * 0.4)
                continue
            return pd.DataFrame()

        data = _parse_json_safely(r)
        if not data:
            if attempt == 0:
                time.sleep(0.4 + random.random() * 0.4)
                continue
            return pd.DataFrame()

        try:
            body = data["response"]["body"]
            items = body["items"]["item"]
            total = body.get("totalCount")
        except Exception:
            return pd.DataFrame()

        # totalCount 감시: numOfRows(1000)보다 크면 경고만 로그
        if isinstance(total, int) and total > NUM_ROWS_SINGLE:
            log.warning("[SHORT] totalCount=%s > numOfRows=%s nx=%s ny=%s (정상일 수 있음; +%sh만 사용)",
                        total, NUM_ROWS_SINGLE, nx, ny, HORIZON_HOURS)

        if not items:
            return pd.DataFrame()

        df = pd.DataFrame(items)
        df["nx"] = pd.to_numeric(df["nx"], errors="coerce").astype("Int64")
        df["ny"] = pd.to_numeric(df["ny"], errors="coerce").astype("Int64")
        df["fcstDate"] = df["fcstDate"].astype(str)
        df["fcstTime"] = df["fcstTime"].astype(str).str.zfill(4)
        df["baseDate"] = df["baseDate"].astype(str)
        df["baseTime"] = df["baseTime"].astype(str).str.zfill(4)
        return df

    return pd.DataFrame()


# ---------------- main collector ----------------
def collect_short_fcst(call_list_csv: Path = CALL_LIST,
                       out_path: Path = DEFAULT_OUT,
                       categories: Optional[List[str]] = None) -> str:
    """
    여러 지점을 병렬 수집하여 +H시간(기본 6h)만 wide parquet 저장.
    """
    cats = categories or CATEGORIES
    log.info("[SHORT] target categories = %s", ",".join(cats))

    if not API_KEY:
        raise RuntimeError("KMA_API_KEY 환경변수가 비어있습니다. (URL-encoded serviceKey 필요)")

    cl = pd.read_csv(call_list_csv)
    if "admin_code" not in cl.columns and "areaNo" in cl.columns:
        cl = cl.rename(columns={"areaNo": "admin_code"})
    req_cols = {"nx", "ny"}
    if not req_cols.issubset(cl.columns):
        raise ValueError(f"call_list.csv에 nx, ny 컬럼이 필요합니다. got={cl.columns.tolist()}")

    now_kst = pendulum.now("Asia/Seoul")
    baseDate, baseTime = _latest_issue_time(now_kst)

    # 타깃 +Hh 시각 프레임(조인용): now 정시와 정렬되도록 시작
    tgt_df, start_ts = _target_times_df(baseDate, baseTime, HORIZON_HOURS, now_kst=now_kst)
    log.info("[SHORT] baseDate=%s baseTime=%s startFrom=%s (now=%s)",
             baseDate, baseTime, start_ts.to_datetime_string(), now_kst.to_datetime_string())

    def _worker(row: pd.Series) -> Optional[pd.DataFrame]:
        nx = int(row["nx"]); ny = int(row["ny"])
        admin_code = int(row["admin_code"]) if "admin_code" in row and not pd.isna(row["admin_code"]) else None
        try:
            raw = _fetch_vilage_singlepage(nx, ny, baseDate, baseTime)
            if raw.empty:
                log.warning("[SHORT] empty nx=%s ny=%s", nx, ny)
                return None

            # 카테고리 필터
            raw = raw[raw["category"].isin(cats)]
            if raw.empty:
                return None

            # +Hh 타깃만 조인으로 필터 (현재 정시 기준)
            raw_sel = raw.merge(tgt_df, on=["fcstDate", "fcstTime"], how="inner")

            # 폴백: 해당 슬롯이 아직 응답에 없으면 base+1h 시작으로 재시도
            if raw_sel.empty:
                tgt_base_df, _ = _target_times_df(baseDate, baseTime, HORIZON_HOURS, now_kst=None)
                raw_sel = raw.merge(tgt_base_df, on=["fcstDate", "fcstTime"], how="inner")
                if raw_sel.empty:
                    log.warning("[SHORT] no target slots nx=%s ny=%s", nx, ny)
                    return None

            wide = _postprocess_wide(raw_sel, admin_code=admin_code)
            if wide is None or wide.empty:
                log.warning("[SHORT] no pivot rows nx=%s ny=%s", nx, ny)
                return None

            log.info("[SHORT] nx=%s ny=%s -> %d rows", nx, ny, len(wide))
            return wide

        except Exception as e:
            log.exception("[SHORT] failed nx=%s ny=%s: %s", nx, ny, e)
            return None

    # 병렬 수집
    parts: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(_worker, r) for _, r in cl.iterrows()]
        for fut in as_completed(futures):
            df_part = fut.result()
            if df_part is not None and not df_part.empty:
                parts.append(df_part)

    if not parts:
        log.error("[SHORT] no data collected; skip write")
        return str(out_path)

    df = pd.concat(parts, ignore_index=True)

    # 중복 제거
    dedup_keys = ["admin_code", "nx", "ny", "fcstDate", "fcstTime"]
    dedup_keys = [k for k in dedup_keys if k in df.columns]
    if dedup_keys:
        df = df.drop_duplicates(subset=dedup_keys)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    log.info("[SHORT] saved: %s (rows=%d, cols=%d)", out_path, len(df), df.shape[1])
    return str(out_path)


def main():
    try:
        collect_short_fcst()
    except Exception as e:
        log.exception("[SHORT] aborted: %s", e)

if __name__ == "__main__":
    main()
