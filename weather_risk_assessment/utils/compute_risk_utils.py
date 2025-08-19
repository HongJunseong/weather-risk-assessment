# utils/compute_risk_utils.py
from pathlib import Path
import numpy as np
import pandas as pd
import re

# ---------- 기본 수치 처리 ----------
def _num(s): 
    return pd.to_numeric(s, errors="coerce")

def _minmax(s: pd.Series, p_low=5, p_high=95) -> pd.Series:
    s = _num(s).replace([np.inf, -np.inf], np.nan)
    if s.dropna().empty:
        return pd.Series(np.zeros(len(s)), index=s.index)
    vmin = np.nanpercentile(s.dropna(), p_low)
    vmax = np.nanpercentile(s.dropna(), p_high)
    if not np.isfinite(vmin) or not np.isfinite(vmax) or abs(vmax - vmin) < 1e-9:
        # 분산 거의 없을 때: 0/양수 구분만
        return (s > vmin).astype(float)
    return ((s - vmin) / (vmax - vmin + 1e-6)).clip(0, 1)

def _logistic(x, k=5.0, x0=0.5):
    return 1.0 / (1.0 + np.exp(-k * (x - x0)))

def _ensure_time_str(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["fcstDate","baseDate"]:
        if c in df.columns: df[c] = df[c].astype(str)
    for c in ["fcstTime","baseTime"]:
        if c in df.columns: df[c] = df[c].astype(str).str.zfill(4)
    return df

# ---------- 행정구역 매핑 ----------
def load_admin_map(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"WARN: admin map not found: {path}", flush=True)
        return pd.DataFrame(columns=["nx","ny","admin_code","admin_name"])

    m = pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)

    # 컬럼 표준화
    rename = {}
    for c in m.columns:
        lc = c.lower()
        if lc == "nx": rename[c] = "nx"
        if lc == "ny": rename[c] = "ny"
        if lc in {"admin_name","adm_name","name"}: rename[c] = "admin_name"
    m = m.rename(columns=rename)

    if not {"nx","ny"}.issubset(m.columns):
        print("WARN: admin map needs nx, ny columns.", flush=True)
        return pd.DataFrame(columns=["nx","ny","admin_name"])

    m["nx"] = pd.to_numeric(m["nx"], errors="coerce").astype("Int64")
    m["ny"] = pd.to_numeric(m["ny"], errors="coerce").astype("Int64")
    if "admin_name" in m.columns:
        m["admin_name"] = m["admin_name"].astype(str).str.strip()
    else:
        m["admin_name"] = None

    # 완전 중복만 제거
    m = m.drop_duplicates(subset=["nx","ny","admin_name"])
    return m[["nx","ny","admin_name"]]

def _list_to_str(xs):
    """['종로구','중구'] -> '종로구, 중구'"""
    if isinstance(xs, list):
        xs = sorted(set(x for x in xs if isinstance(x, str)))
        return ", ".join(xs) if xs else None
    return None

# ---------- 강수량 문자열 파서 ----------
def _parse_mm_series(s: pd.Series) -> pd.Series:
    """'1.0mm', '1.0mm미만', '10~20mm', '-', '강수없음' → mm(float)"""
    if s is None or len(s) == 0:
        return pd.Series([], dtype=float)

    x = s.astype(str).str.strip()
    x = x.replace({"강수없음": "0", "-": "0", "": "0"})
    x = x.str.replace("㎜", "mm", regex=False).str.replace("mm", "", regex=False).str.replace(" ", "")
    out = pd.to_numeric(x, errors="coerce")

    def _range_mean(v: str):
        m = re.fullmatch(r"(\d+(?:\.\d+)?)~(\d+(?:\.\d+)?)", v)
        return (float(m.group(1)) + float(m.group(2))) / 2.0 if m else np.nan

    def _ltgte(v: str):
        m = re.fullmatch(r"(\d+(?:\.\d+)?)(미만|이상)", v)
        if not m: return np.nan
        val = float(m.group(1))
        return val * 0.5 if m.group(2) == "미만" else val

    mask = out.isna()
    if mask.any():
        out = out.fillna(x[mask].apply(_range_mean))
    mask = out.isna()
    if mask.any():
        out = out.fillna(x[mask].apply(_ltgte))

    return out.fillna(0.0).clip(lower=0.0)

# ---------- 후보 컬럼 결합 ----------
def _pick_mm(df: pd.DataFrame, base_names: list[str]) -> pd.Series:
    """RN1/RN1_nc, PCP/PCP_nc 등 mm 단위 컬럼 결합"""
    cand = []
    for n in base_names:
        cand += [c for c in df.columns if (c == n) or c.startswith(n + "_")]
    cand = list(dict.fromkeys(cand))
    if not cand:
        return pd.Series(np.nan, index=df.index)
    parts = [_parse_mm_series(df[c]) for c in cand]
    return pd.concat(parts, axis=1).max(axis=1)

def _pick_num(df: pd.DataFrame, base_names: list[str]) -> pd.Series:
    cand = []
    for n in base_names:
        cand += [c for c in df.columns if (c == n) or c.startswith(n + "_")]
    cand = list(dict.fromkeys(cand))
    if not cand:
        return pd.Series(np.nan, index=df.index)
    parts = [_num(df[c]) for c in cand]
    return pd.concat(parts, axis=1).max(axis=1)
