# risk/typhoon_risk.py

import numpy as np
import pandas as pd
from weather_risk_assessment.utils.compute_risk_utils import _num, _minmax

def compute_typhoon_risk(df: pd.DataFrame) -> pd.Series:
    """
    태풍 위험도 산출
    - 거리 → 근접도(지수 감쇠 + 소프트 컷오프)
    - 근접도^γ로 바람/경보 가중
    - 시그모이드 정규화 후 eps_floor 부여
    """
    if not any(c in df.columns for c in ["TY_DISTANCE_KM","TY_MAX_WIND","TY_WARNING"]):
        return pd.Series(np.zeros(len(df)), index=df.index)

    dist = _num(df.get("TY_DISTANCE_KM"))
    vmax = _num(df.get("TY_MAX_WIND"))
    warn = _num(df.get("TY_WARNING")).clip(0,1).fillna(0)

    # 하이퍼파라미터
    d_scale   = 300.0   # 거리 감쇠 스케일
    d_soft    = 700.0   # 소프트 컷오프 중심
    d_width   = 80.0    # 컷오프 완만함
    gamma_w   = 1.2     # 바람 게이트 지수
    gamma_a   = 1.5     # 경보 게이트 지수
    eps_floor = 0.010   # 최소값(0 방지)

    # 1) 거리 → 근접도(0~1)
    prox_core = np.exp(-(dist.fillna(np.inf) / d_scale))        # 지수 감쇠
    soft_gate = 1.0 / (1.0 + np.exp((dist - d_soft) / d_width)) # 시그모이드 컷오프
    prox = (prox_core * soft_gate).clip(0, 1).fillna(0)

    # 2) 바람/경보: prox^γ로 게이팅
    wind_norm = _minmax(vmax).fillna(0)
    wind_eff  = (prox ** gamma_w) * wind_norm
    warn_eff  = (prox ** gamma_a) * warn

    # 3) 가중 결합 (0~1)
    raw = (0.75*prox + 0.20*wind_eff + 0.05*warn_eff).clip(0, 1).fillna(0)

    # 4) 시그모이드 정규화 + eps_floor 적용
    k = 6.0; x0 = 0.35
    sig = 1.0 / (1.0 + np.exp(-k * (raw - x0)))
    s0  = 1.0 / (1.0 + np.exp(-k * (0.0 - x0)))   # raw=0일 때 값
    z   = ((sig - s0) / (1.0 - s0)).clip(0, 1)
    out = eps_floor + (1.0 - eps_floor) * z

    return out.fillna(eps_floor)