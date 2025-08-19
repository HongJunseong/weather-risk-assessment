# collectors/typhoon_forecast_wide.py
from __future__ import annotations
import os, logging
from pathlib import Path
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===== 기본 설정 =====
YEAR_MIN, YEAR_MAX = 2000, 2100
WINDOW_HOURS = 6        # 정확히 6개 시각
EXTRAP_HOURS = 6        # 트랙 범위 밖 ±6시간 외삽 허용

# ===== ENV =====
API_KEY   = os.getenv("KMA_API_KEY", "").strip()
BASE_URL  = os.getenv("KMA_BASE_URL", "https://apis.data.go.kr/1360000").rstrip("/")
TYPH_SVC  = os.getenv("KMA_TYPH_SVC_PATH", "TyphoonInfoService").strip("/")
EP_TY_INFO  = os.getenv("KMA_TYPH_INFO_EP",  "getTyphoonInfo").strip()
EP_TY_INFO_LIST = os.getenv("KMA_TYPH_TRACK_EP", "getTyphoonInfoList").strip()
EP_TY_FCST  = os.getenv("KMA_TYPH_LIST_EP",  "getTyphoonFcst").strip()
FORCE_HTTP = os.getenv("KMA_FORCE_HTTP", "0") == "1"
SKIP_SSL   = os.getenv("KMA_SKIP_SSL_VERIFY", "0") == "1"


LOG_LEVEL = os.getenv("KMA_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(message)s")
log = logging.getLogger("typhoon_forecast_wide")

# ===== HTTP 세션(간단) =====
session = requests.Session()
session.headers.update({"User-Agent": "weather_risk_assessment/typhoon-forecast-wide"})
retries = Retry(total=2, backoff_factor=0.4, status_forcelist=[429,502,503,504], raise_on_status=False)
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://",  HTTPAdapter(max_retries=retries))

def _url(ep: str) -> str:
    u = f"{BASE_URL}/{TYPH_SVC}/{ep}"
    return u.replace("https://", "http://") if FORCE_HTTP else u

def _debug_head(ep, r):
    head = (r.text or "")[:400].replace("\n"," ")
    log.warning("[ty][%s] 200 but empty items. content-type=%s head=%s",
                ep, r.headers.get("content-type",""), head)

def _call(ep: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        r = session.get(_url(ep), params=params, timeout=20, verify=not SKIP_SSL)
    except requests.exceptions.SSLError:
        r = session.get(_url(ep).replace("https://","http://"), params=params, timeout=20)
    except Exception as e:
        log.warning("[ty] HTTP error: %s", e); return None
    if r.status_code != 200:
        log.warning("[ty] non-200 %s %s", r.status_code, (r.text or "")[:200]); return None
    ct = (r.headers.get("content-type","").lower())
    if "json" not in ct and "xml" not in ct:
        log.warning("[ty] non-JSON/XML head=%s", (r.text or "")[:160]); return None
    # 빈 items 디버그를 위해 JSON 파싱 전 헤더를 남길 수 있도록 호출부에서 사용
    try:
        js = r.json()
        # items 비면 한 번 찍기
        try:
            items = js.get("response",{}).get("body",{}).get("items",{}).get("item",[])
            if not items:
                _debug_head(ep, r)
        except Exception:
            _debug_head(ep, r)
        return js
    except Exception:
        # XML일 수 있으니 머리만 찍고 None
        log.warning("[ty] JSON decode fail head=%s", (r.text or "")[:200])
        return None

def _items(js: Dict[str,Any]) -> List[dict]:
    try: return js.get("response",{}).get("body",{}).get("items",{}).get("item",[]) or []
    except Exception: return []

# ===== 트랙 파싱(필요 최소) =====
def _pick_first_rowwise(df: pd.DataFrame, cols: List[str]) -> pd.Series:
    cols = [c for c in cols if c in df.columns]
    if not cols: return pd.Series([pd.NA]*len(df), index=df.index)
    return df[cols].bfill(axis=1).iloc[:,0]

def _parse_track_items(items: List[dict]) -> pd.DataFrame:
    if not items: return pd.DataFrame()
    df = pd.DataFrame(items)
    lat = pd.to_numeric(_pick_first_rowwise(df, ["typLat","lat","latitude"]), errors="coerce")
    lon = pd.to_numeric(_pick_first_rowwise(df, ["typLon","lon","longitude"]), errors="coerce")
    tm  = _pick_first_rowwise(df, ["fcstTime","typTm","tm","tmFc"])
    t1  = pd.to_datetime(tm.astype(str), format="%Y%m%d%H%M", errors="coerce")
    if t1.isna().all(): t1 = pd.to_datetime(tm, errors="coerce")
    wind = pd.to_numeric(_pick_first_rowwise(df, ["typWs","ws","wind","maxWind"]), errors="coerce")
    out  = pd.DataFrame({"time": t1, "lat": lat, "lon": lon, "wind": wind})
    out  = out.dropna(subset=["time","lat","lon"]).sort_values("time")
    yr   = out["time"].dt.year
    return out[(yr >= YEAR_MIN) & (yr <= YEAR_MAX)]

def _resample_hourly(track: pd.DataFrame) -> pd.DataFrame:
    if track.empty: return pd.DataFrame(columns=["lat","lon","wind"]).set_index(pd.DatetimeIndex([]))
    tr = track.sort_values("time").set_index("time")
    if tr.index.nunique() < 2: return tr
    idx = pd.date_range(tr.index.min(), tr.index.max(), freq="1H")
    tr  = (tr.reindex(tr.index.union(idx)).sort_index()
             .interpolate(method="time", limit_area="inside")
             .reindex(idx))
    return tr

# ===== 타깃 시각: 항상 6개 보장 =====
def _extract_dt_from_forecast(path: Path) -> pd.Series:
    try: df = pd.read_parquet(path)
    except Exception: return pd.Series([], dtype="datetime64[ns]")
    dt = None
    if {"fcstDate","fcstTime"}.issubset(df.columns):
        dt = pd.to_datetime(df["fcstDate"].astype(str)+df["fcstTime"].astype(str).str.zfill(4),
                            format="%Y%m%d%H%M", errors="coerce")
    elif "time" in df.columns:
        dt = pd.to_datetime(df["time"], errors="coerce")
    if dt is None: return pd.Series([], dtype="datetime64[ns]")
    dt = dt[dt.notna()]
    return dt[(dt.dt.year >= YEAR_MIN) & (dt.dt.year <= YEAR_MAX)]

def _derive_target_times(trH: Optional[pd.DataFrame]) -> pd.DatetimeIndex:
    # ultra_nowcast, shortfcst 등에서 fcstDate, fcstTime을 모두 읽어서 target_times로 사용
    roots = [Path(os.getenv("DRE_SINK_DIR","")),
             Path(__file__).resolve().parents[1]/"data",
             Path(__file__).resolve().parents[1]/"data"/"live"]
    names = ["ultra_shortfcst.parquet","ultra_nowcast.parquet"]
    times = []
    for r in roots:
        if not r: continue
        for n in names:
            p = r / n
            if p.exists():
                dt = _extract_dt_from_forecast(p)
                if not dt.empty:
                    times.extend(dt.tolist())
    # 중복 제거 및 정렬
    if times:
        times = pd.to_datetime(sorted(set(times)))
        return pd.DatetimeIndex(times)
    # 예보 파일이 없으면 현재 시각 기준 6개 시각
    now_kst = pendulum.now("Asia/Seoul").start_of("hour")
    end_dt = pd.Timestamp(str(now_kst))
    return pd.date_range(end=end_dt, periods=WINDOW_HOURS, freq="1H")

# ===== 정렬/보간/외삽 =====
def _align_on_targets(trH: pd.DataFrame, target: pd.DatetimeIndex) -> pd.DataFrame:
    if trH is None or trH.empty:
        return pd.DataFrame(index=target, columns=["lat","lon","wind"], dtype=float)
    if trH.index.nunique() == 1:
        one = trH.iloc[-1]
        out = pd.DataFrame(index=target)
        for c in ["lat","lon","wind"]:
            out[c] = float(one[c]) if c in trH.columns and pd.notna(one[c]) else np.nan
        return out

    aligned = (trH.reindex(trH.index.union(target)).sort_index())
    for c in ["lat","lon","wind"]:
        if c in aligned.columns:
            aligned[c] = aligned[c].interpolate(method="time", limit_direction="both")
    aligned = aligned.reindex(target)

    # ±EXTRAP_HOURS 외삽
    tmin, tmax = trH.index.min(), trH.index.max()
    aft = (target > tmax) & (target <= tmax + pd.Timedelta(hours=EXTRAP_HOURS))
    bef = (target < tmin) & (target >= tmin - pd.Timedelta(hours=EXTRAP_HOURS))

    def _lin_extrap_last(col: pd.Series, q_idx: pd.DatetimeIndex) -> pd.Series:
        x = pd.Index(trH.index[-2:].asi8.astype(float) / 1e9)
        y = col.iloc[-2:].astype(float)
        m = (y.iloc[1]-y.iloc[0]) / (x[1]-x[0] + 1e-9)
        tq = pd.Index(q_idx.asi8.astype(float) / 1e9)
        return pd.Series(y.iloc[1] + m*(tq - x[1]), index=q_idx)

    def _lin_extrap_first(col: pd.Series, q_idx: pd.DatetimeIndex) -> pd.Series:
        x = pd.Index(trH.index[:2].asi8.astype(float) / 1e9)
        y = col.iloc[:2].astype(float)
        m = (y.iloc[1]-y.iloc[0]) / (x[1]-x[0] + 1e-9)
        tq = pd.Index(q_idx.asi8.astype(float) / 1e9)
        return pd.Series(y.iloc[0] + m*(tq - x[0]), index=q_idx)

    if aft.any():
        q = target[aft]
        for c in ["lat","lon","wind"]:
            if c in trH.columns:
                aligned.loc[q, c] = aligned.loc[q, c].where(
                    aligned.loc[q, c].notna(), _lin_extrap_last(trH[c], q)
                )
    if bef.any():
        q = target[bef]
        for c in ["lat","lon","wind"]:
            if c in trH.columns:
                aligned.loc[q, c] = aligned.loc[q, c].where(
                    aligned.loc[q, c].notna(), _lin_extrap_first(trH[c], q)
                )
    return aligned

# ===== 거리 계산 =====
def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1 = np.radians(lat1); lon1 = np.radians(lon1)
    lat2 = np.radians(lat2); lon2 = np.radians(lon2)
    dlat = lat2 - lat1; dlon = lon2 - lon1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2.0)**2
    return 2*R*np.arcsin(np.sqrt(a))

# ===== 메인 =====
def fetch_typhoon_forecast_wide(out_path: str | Path,
                                grid_path: str | Path,
                                target_times: Optional[pd.Series] = None) -> Path:
    out_path = Path(out_path); grid_path = Path(grid_path)

    # 0) grid
    if not grid_path.exists():
        log.warning("[ty] grid missing -> write empty schema")
        pd.DataFrame(columns=["nx","ny","fcstDate","fcstTime","TY_DISTANCE_KM","TY_MAX_WIND","TY_WARNING"]).to_parquet(out_path, index=False)
        return out_path
    grid = pd.read_parquet(grid_path)[["nx","ny","lat","lon"]].dropna(subset=["lat","lon"])
    grid["nx"] = pd.to_numeric(grid["nx"], errors="coerce").astype("Int64")
    grid["ny"] = pd.to_numeric(grid["ny"], errors="coerce").astype("Int64")

    # 1) 트랙 수집(실패해도 진행)
    track = pd.DataFrame()
    try:
        if API_KEY:
            now_kst = pendulum.now("Asia/Seoul")
            frm = now_kst.subtract(days=7).format("YYYYMMDD")
            to  = now_kst.format("YYYYMMDD")  

            # 1A) getTyphoonInfo: 기간 조회 (YYYYMMDD ~ YYYYMMDD)
            js = _call(EP_TY_INFO, {
                "serviceKey": API_KEY,
                "dataType": "JSON",
                "NumOfRows": 500,
                "pageNo": 1,
                "fromTmFc": frm,
                "toTmFc": to,
            })
            track = _parse_track_items(_items(js) if js else [])

            # 1B) 비었으면 InfoList → Fcst 폴백 (정확한 tmFc/typSeq 확보)
            if track.empty:
                for tmfc_day in [now_kst.format("YYYYMMDD"),
                                now_kst.subtract(days=1).format("YYYYMMDD")]:
                    js_list = _call(EP_TY_INFO_LIST, {
                        "serviceKey": API_KEY,
                        "dataType": "JSON",
                        "numOfRows": 100,
                        "pageNo": 1,
                        "tmFc": tmfc_day,  # YYYYMMDD
                    })
                    lst = _items(js_list) if js_list else []
                    if not lst:
                        continue

                    def _get_typseq(it):
                        return (str(it.get("typSeq") or it.get("typhoonSeq") or it.get("tmSeq") or "")).strip()

                    def _get_tmfc_full(it):
                        v = str(it.get("tmFc") or it.get("announceTime") or "").strip()
                        return v if (len(v) == 12 and v.isdigit()) else None  # YYYYMMDDHHMM

                    typSeq = _get_typseq(lst[0])
                    tmFc_full = _get_tmfc_full(lst[0])

                    # 목록이 8자리만 줄 때: 같은 날짜로 Info 재조회해서 12자리 발표시각 중 최신 선택
                    if not tmFc_full:
                        js_info_day = _call(EP_TY_INFO, {
                            "serviceKey": API_KEY,
                            "dataType": "JSON",
                            "NumOfRows": 500,
                            "pageNo": 1,
                            "fromTmFc": tmfc_day,
                            "toTmFc": tmfc_day,
                        })
                        items_day = _items(js_info_day) if js_info_day else []
                        cand = []
                        for it in items_day:
                            v = str(it.get("tmFc") or it.get("announceTime") or "").strip()
                            if len(v) == 12 and v.isdigit():
                                cand.append(v)
                        tmFc_full = max(cand) if cand else None

                    if typSeq and tmFc_full:
                        js_trk = _call(EP_TY_FCST, {
                            "serviceKey": API_KEY,
                            "dataType": "JSON",
                            "numOfRows": 2000,
                            "pageNo": 1,
                            "tmFc": tmFc_full,  # YYYYMMDDHHMM
                            "typSeq": typSeq,
                        })
                        tr = _parse_track_items(_items(js_trk) if js_trk else [])
                        if not tr.empty:
                            track = tr
                            break
        else:
            log.info("[ty] KMA_API_KEY empty -> skip API")
    except Exception as e:
        log.warning("[ty] track fetch error: %s", e)
        track = pd.DataFrame()

    # 2) 리샘플
    trH = _resample_hourly(track)
    log.info("트랙 데이터:\n%s", trH)

    # 3) 타깃 시각(정확히 6개)
    if target_times is None:
        target = _derive_target_times(trH)
    else:
        tt = pd.to_datetime(pd.Series(target_times), errors="coerce").dropna().sort_values().unique()
        if len(tt) == 0:
            target = _derive_target_times(trH)
        else:
            end_dt = pd.Timestamp(tt[-1]).floor("H")
            target = pd.date_range(end=end_dt, periods=WINDOW_HOURS, freq="1H")
    target = pd.DatetimeIndex(target)

    # 4) 정렬/보간/외삽
    aligned = _align_on_targets(trH, target)
    log.info("보간 후 aligned:\n%s", aligned)

    # 5) 격자별 산출
    alat = np.radians(grid["lat"].values); alon = np.radians(grid["lon"].values)
    rows = []
    for t, row in aligned.iterrows():
        lat_c, lon_c = row.get("lat", np.nan), row.get("lon", np.nan)
        wind_c = row.get("wind", np.nan)
        if pd.isna(lat_c) or pd.isna(lon_c):
            df1 = pd.DataFrame({
                "nx": grid["nx"].values, "ny": grid["ny"].values,
                "fcstDate": t.strftime("%Y%m%d"), "fcstTime": t.strftime("%H%M"),
                "TY_DISTANCE_KM": np.nan, "TY_MAX_WIND": np.nan, "TY_WARNING": np.nan,
            })
        else:
            blat = np.radians(float(lat_c)); blon = np.radians(float(lon_c))
            d = 2*6371*np.arcsin(np.sqrt(np.sin((blat-alat)/2.0)**2 +
                                         np.cos(alat)*np.cos(blat)*np.sin((blon-alon)/2.0)**2))
            df1 = pd.DataFrame({
                "nx": grid["nx"].values, "ny": grid["ny"].values,
                "fcstDate": t.strftime("%Y%m%d"), "fcstTime": t.strftime("%H%M"),
                "TY_DISTANCE_KM": d, "TY_MAX_WIND": float(wind_c) if pd.notna(wind_c) else np.nan,
                "TY_WARNING": 0.0,
            })
        if df1 is not None:
            rows.append(df1)
    # None이 아닌 DataFrame만 concat
    rows = [r for r in rows if isinstance(r, pd.DataFrame)]
    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
        columns=["nx","ny","fcstDate","fcstTime","TY_DISTANCE_KM","TY_MAX_WIND","TY_WARNING"]
    )
    out.to_parquet(out_path, index=False)
    log.info("[ty] saved -> %s | hours=%d | range=%s~%s",
             out_path, out["fcstTime"].nunique() if not out.empty else 0,
             out["fcstTime"].min() if not out.empty else None,
             out["fcstTime"].max() if not out.empty else None)
    return out_path
