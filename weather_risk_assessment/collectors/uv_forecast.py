# collectors/uv_forecast.py
from __future__ import annotations
import os, logging, json, re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
import pandas as pd
import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from xml.etree import ElementTree as ET

# ---------- Paths & ENV ----------
# 프로젝트 루트/데이터 경로 설정
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
LIVE = DATA / "live"; LIVE.mkdir(parents=True, exist_ok=True)

# 행정구역별 중심점 목록 (nx, ny, 행정코드)
CALL_LIST = DATA / "unique_admin_centroids.csv"
# 기본 출력 위치 (실시간 parquet 저장)
DEFAULT_OUT = LIVE / "uv.parquet"

# 환경변수 (없으면 기본값 사용)
BASE_URL   = os.getenv("KMA_BASE_URL", "https://apis.data.go.kr/1360000").rstrip("/")
SVC_PATH   = os.getenv("KMA_UV_SVC_PATH", "LivingWthrIdxServiceV4").strip("/")
EP_UV      = os.getenv("KMA_UV_EP", "getUVIdxV4").strip()
API_KEY    = os.getenv("KMA_API_KEY", "").strip()
FORCE_HTTP = os.getenv("KMA_FORCE_HTTP", "0") == "1"             # HTTP 강제 여부
SKIP_SSL   = os.getenv("KMA_SKIP_SSL_VERIFY", "0") == "1"        # SSL 검증 생략 여부
LOG_LEVEL  = os.getenv("KMA_LOG_LEVEL", "INFO").upper()
HORIZON_H  = int(os.getenv("UV_HORIZON_HOURS", "6"))             # 예보 범위 (기본 6시간)

logging.basicConfig(level=LOG_LEVEL, format="%(message)s")
log = logging.getLogger("uv_forecast")

# ---------- HTTP ----------
# KMA API 호출 세션 설정 (재시도 로직 포함)
session = requests.Session()
session.headers.update({"User-Agent": "weather_risk_assessment-uv-align"})
retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retry))
session.mount("http://",  HTTPAdapter(max_retries=retry))

def _url() -> str:
    """API 엔드포인트 URL 생성 (HTTP 강제 여부 반영)."""
    u = f"{BASE_URL}/{SVC_PATH}/{EP_UV}"
    return u.replace("https://", "http://") if FORCE_HTTP else u

# ---------- Helpers ----------
def _read_call_list() -> pd.DataFrame:
    """행정구역 중심점 CSV 읽기 (nx, ny, admin_code 필수)."""
    df = pd.read_csv(CALL_LIST, dtype={"nx": "Int64", "ny": "Int64", "admin_code": str})
    need = {"nx", "ny", "admin_code"}
    miss = need - set(df.columns)
    if miss:
        raise ValueError(f"call_list.csv에 {sorted(miss)} 컬럼이 없습니다.")
    return df.dropna(subset=["admin_code"]).assign(
        admin_code=lambda d: d["admin_code"].astype(str).str.strip()
    )

def _to_naive_index(idx_like) -> pd.DatetimeIndex:
    """모든 DatetimeIndex를 timezone 없는 naive 형태로 강제 변환."""
    dt = pd.to_datetime(idx_like, errors="coerce")
    try:
        return pd.DatetimeIndex(dt.tz_localize(None))
    except Exception:
        return pd.DatetimeIndex(dt)

def _load_targets_from_ultra_shortfcst(horizon_hours: int = 6) -> pd.DatetimeIndex:
    """
    ultra_shortfcst.parquet에서 예측 대상 시각들을 로드.
    - horizon_hours 만큼의 최근 시각을 추출
    - parquet 없거나 비정상일 경우 현재 시각(now)으로 폴백
    """
    p = LIVE / "ultra_shortfcst.parquet"
    now_kst = pendulum.now("Asia/Seoul").start_of("hour")

    if not p.exists():
        log.warning("[UV] ultra_shortfcst.parquet 없음 → now 1개로 폴백")
        return _to_naive_index([now_kst.to_datetime_string()])

    try:
        # fcstDate+fcstTime → datetime 변환
        d = pd.read_parquet(p, columns=["fcstDate", "fcstTime"])
        dt = pd.to_datetime(
            d["fcstDate"].astype(str) + d["fcstTime"].astype(str).str.zfill(4),
            format="%Y%m%d%H%M", errors="coerce"
        ).dropna()

        if len(dt) == 0:
            log.warning("[UV] ultra_shortfcst.parquet에 유효한 fcstDate/fcstTime 없음 → now 1개 폴백")
            return _to_naive_index([now_kst.to_datetime_string()])

        # 최근 horizon_hours 개만 추출
        dt = pd.DatetimeIndex(pd.to_datetime(dt)).sort_values().unique()
        sel = dt[-max(1, horizon_hours):]

        # 정시로 맞추고 naive index로 변환
        sel = pd.DatetimeIndex(sel).floor("H").unique().sort_values()
        out = _to_naive_index(sel)

        log.info("[UV] ultra_shortfcst available = %s",
                 ", ".join(pd.Index(dt).strftime("%Y%m%d %H:%M").tolist()[:24]))
        log.info("[UV] selected targets = %s",
                 ", ".join(pd.Index(out).strftime("%Y%m%d %H:%M").tolist()))
        return out if len(out) else _to_naive_index([now_kst.to_datetime_string()])

    except Exception as e:
        log.warning(f"[UV] ultra_shortfcst.parquet 읽기 실패({e}) → now 1개 폴백")
        return _to_naive_index([now_kst.to_datetime_string()])

def _items_from_text(text: str) -> List[dict]:
    """응답 텍스트(JSON 또는 XML)에서 item 리스트 추출."""
    # JSON 파싱 우선
    try:
        js = json.loads(text or "")
        it = js.get("response", {}).get("body", {}).get("items", {}).get("item", [])
        return [it] if isinstance(it, dict) else (it or [])
    except Exception:
        pass
    # XML 파싱 (쿼터 초과 탐지 포함)
    try:
        root = ET.fromstring(text or "")
        ret = (root.findtext(".//returnAuthMsg") or
               root.findtext(".//returnReasonCode") or "")
        if "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR" in ret:
            raise RuntimeError("KMA quota exceeded")
        out = []
        for it in root.findall(".//items/item"):
            out.append({c.tag: (c.text or "") for c in it})
        return out
    except RuntimeError:
        raise
    except Exception:
        return []

def _http(area_no: str, ymdh: str) -> List[dict]:
    """KMA API 호출 → 해당 행정구역(area_no)의 특정 발표시각 데이터 반환."""
    if not API_KEY:
        log.error("KMA_API_KEY 비어있음")
        return []
    params = {
        "serviceKey": API_KEY,
        "dataType": "JSON",
        "pageNo": 1, "numOfRows": 100,
        "areaNo": area_no,
        "time": ymdh,   # YYYYMMDDHH
    }
    url = _url()
    if FORCE_HTTP:
        url = url.replace("https://", "http://")
    try:
        r = session.get(url, params=params, timeout=15, verify=not SKIP_SSL)
        return _items_from_text(r.text or "")
    except RuntimeError:
        raise
    except Exception as e:
        log.warning(f"[UV] HTTP error ({area_no},{ymdh}): {e}")
        return []

def _parse_h_offsets(item: dict) -> Dict[int, float]:
    """
    API 응답에서 h0, h3, h6 ... → {offset_hour: UVI} 딕셔너리 변환.
    단일 uvi/idx 값만 있으면 h0으로 처리.
    """
    out: Dict[int, float] = {}
    for k, v in item.items():
        m = re.fullmatch(r"[hH](\d{1,2})", str(k))
        if not m:
            continue
        val = pd.to_numeric(v, errors="coerce")
        if pd.notna(val):
            out[int(m.group(1))] = float(val)
    if not out:
        for vk in ("uvi", "uvIndex", "idx", "value"):
            val = pd.to_numeric(item.get(vk), errors="coerce")
            if pd.notna(val):
                out[0] = float(val); break
    return out

def _find_base(area_no: str, anchor: pendulum.DateTime, back_hours: int = 48) -> Tuple[Optional[pendulum.DateTime], Dict[int, float]]:
    """
    anchor 시각부터 최대 back_hours 시간 전까지 역으로 탐색하며,
    실제 발표시각(base_dt)과 상대오프셋 데이터 반환.
    """
    for h in range(0, back_hours + 1):
        cand = anchor.subtract(hours=h).replace(minute=0, second=0, microsecond=0)
        items = _http(area_no, cand.format("YYYYMMDDHH"))
        rel: Dict[int, float] = {}
        for it in items:
            rel.update(_parse_h_offsets(it))
        if rel:
            return cand, rel
    return None, {}

def _rel_to_series(base_dt: pendulum.DateTime, rel: Dict[int, float]) -> pd.Series:
    """
    발표시각(base_dt) 기준 상대오프셋(h0/h3/h6/…)을
    실제 DatetimeIndex 시계열로 변환.
    """
    idx = [pd.Timestamp(str(base_dt.add(hours=k))) for k in sorted(rel)]
    vals = [rel[k] for k in sorted(rel)]
    s = pd.Series(vals, index=pd.DatetimeIndex(idx), dtype="float64")
    s.index = _to_naive_index(s.index)  # tz-naive 강제
    return s

def _interp_to_targets(s_anchor: pd.Series, targets: pd.DatetimeIndex) -> pd.Series:
    """
    anchor 시계열 → target 시각에 맞게 보간.
    - 시간 단위 보간
    - 06~20시 외 값은 0으로 마스킹
    - 반올림 후 정수 처리
    """
    s_anchor.index = _to_naive_index(s_anchor.index)
    targets = _to_naive_index(targets)

    s = (s_anchor.reindex(s_anchor.index.union(targets))
                  .sort_index()
                  .interpolate(method="time", limit_direction="both")
                  .reindex(targets))

    # 낮 시간대만 유지
    h = pd.Index(s.index.hour)
    s = s.where((h >= 6) & (h <= 20), 0.0)
    return s.round().clip(lower=0)

# ---------- Main ----------
def fetch_and_save_uv_wide(out_path: Path | None = None, horizon_hours: Optional[int] = None) -> Path:
    """
    UV 지수 수집 및 저장 파이프라인.
    1) ultra_shortfcst 기준 target 시각 로드
    2) KMA API에서 발표시각 데이터 탐색 및 상대오프셋 추출
    3) target 시각에 보간
    4) (nx, ny, fcstDate, fcstTime, UVI) 형태 parquet 저장
    """
    call = _read_call_list()
    hh = int(horizon_hours) if horizon_hours is not None else HORIZON_H
    targets = _load_targets_from_ultra_shortfcst(horizon_hours=hh)  # tz-naive 보장
    anchor = pendulum.instance(targets.min().to_pydatetime(), tz="Asia/Seoul")

    log.info("[UV] target hours = %s", ", ".join(pd.Index(targets).strftime("%Y%m%d %H:%M").tolist()))

    rows: List[Tuple[int, int, str, str, int]] = []

    # 각 행정구역(admin_code)별로 데이터 수집
    for area_no, sub in call.groupby("admin_code"):
        try:
            base, rel = _find_base(str(area_no), anchor, back_hours=48)
        except RuntimeError as e:
            log.error(f"[UV] quota/critical error: {e}")
            raise
        if not base:
            log.warning(f"[UV] area={area_no} 데이터 없음 (anchor {anchor.format('YYYYMMDDHH')})")
            continue

        # 상대 오프셋 → 실제 시계열 변환 후 보간
        s = _interp_to_targets(_rel_to_series(base, rel), targets)
        for ts, v in s.items():
            ymd = ts.strftime("%Y%m%d"); hhmm = ts.strftime("%H%M")
            for nx, ny in sub[["nx", "ny"]].itertuples(index=False):
                rows.append((int(nx), int(ny), ymd, hhmm, int(v)))

    # DataFrame 구성
    out = pd.DataFrame(rows, columns=["nx", "ny", "fcstDate", "fcstTime", "UVI"])
    if not out.empty:
        out["fcstTime"] = out["fcstTime"].astype(str).str.zfill(4)
        out = out.sort_values(["fcstDate", "fcstTime", "nx", "ny"]).reset_index(drop=True)
    else:
        out = pd.DataFrame(columns=["nx", "ny", "fcstDate", "fcstTime", "UVI"])

    # 저장
    target = Path(out_path) if out_path else DEFAULT_OUT
    out.to_parquet(target, index=False)

    log.info("[UV] saved -> %s | rows=%d | times=%s",
             target, len(out),
             ", ".join(out['fcstTime'].unique().tolist()) if not out.empty else "[]")
    return target

if __name__ == "__main__":
    fetch_and_save_uv_wide()
