# risk/heat_risk.py

import pandas as pd
import numpy as np
from weather_risk_assessment.utils.compute_risk_utils import _pick_num

# --- 유틸 ---
def _smoothstep(x):
    # 0~1 구간 부드러운 보간 함수
    x = np.clip(x, 0.0, 1.0)
    return x * x * (3.0 - 2.0 * x)

# 온도(T) → 위험도(R) 기준선 앵커
DEFAULT_TK = np.array([16, 20, 24, 26, 28, 31, 34, 37, 40], dtype=float)
DEFAULT_RK = np.array([0.10,0.16,0.24,0.30,0.42,0.60,0.76,0.88,0.96], dtype=float)

def _interp_by_anchors(T: pd.Series,
                       Tk: np.ndarray = DEFAULT_TK,
                       Rk: np.ndarray = DEFAULT_RK) -> pd.Series:
    # 주어진 온도를 Tk-Rk 앵커 기준으로 보간
    return pd.Series(np.interp(T.values.astype(float), Tk, Rk), index=T.index)

# --- 본 함수 ---
def compute_heat_risk(df: pd.DataFrame) -> pd.Series:
    """
    Heat Risk 산출
    - 입력: T1H/TMP, REH, WSD
    - 절차: 온도 기반 기준선 → 습도 보정(+), 바람 보정(−) → 안정화
    - 출력: 0.12~0.96 범위의 위험도
    """
    # 1) 입력값 확보
    T  = pd.to_numeric(df.get("T1H"), errors="coerce")
    if T.isna().all() and "TMP" in df:  # 예외적 폴백
        T = pd.to_numeric(df["TMP"], errors="coerce")
    T  = T.fillna(24.0).astype(float)

    RH = pd.to_numeric(df.get("REH"), errors="coerce").clip(0, 100).fillna(60.0).astype(float)
    W  = pd.to_numeric(df.get("WSD"), errors="coerce").fillna(1.5).astype(float).clip(lower=0)

    # 2) 온도 기준선 (단조·0.12~0.96)
    r0 = _interp_by_anchors(T)
    r0_min, r0_max = 0.12, 0.96
    r01 = ((r0 - r0_min) / (r0_max - r0_min)).clip(0.0, 1.0)
    softness = r01 * (1.0 - r01)  # 중앙부 보정 민감도

    # 3) 습도 보정
    # (a) 저·중온: 18→27℃, RH 75→95%일 때 아주 작게(+)
    g_cool_T  = _smoothstep((T - 18.0) / 5.0) * (1.0 - _smoothstep((T - 27.0) / 2.0))
    rh_muggy  = _smoothstep((RH - 75.0) / 20.0)
    cool_add  = 0.020 * rh_muggy * g_cool_T * (1.0 - r01)

    # (b) 고온: 31.5→35.5℃, RH 60%↑에서 약하게(+)
    rh_excess = ((RH - 60.0) / 40.0).clip(0.0, 1.0)
    g_hot_T   = _smoothstep((T - 31.5) / 4.0)
    hot_add   = 0.060 * rh_excess * g_hot_T * softness

    # 4) 바람 보정 (26℃↑, 중·고온에서만 약하게 −)
    w_norm   = np.log1p(W) / np.log1p(10.0)
    g_w      = _smoothstep((T - 26.0) / 4.0)
    wind_sub = 0.040 * w_norm * g_w * softness

    # 5) 합산 & 안정화
    risk = r0 + cool_add + hot_add - wind_sub
    return pd.to_numeric(risk, errors="coerce").clip(r0_min, r0_max)
