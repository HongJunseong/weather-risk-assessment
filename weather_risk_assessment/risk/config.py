from __future__ import annotations

import pandas as pd

# 지표별 위험도 가중치 (합계 = 1.0)
RISK_WEIGHTS: dict[str, float] = {
    "rain":    0.28,
    "heat":    0.18,
    "wind":    0.22,
    "uv":      0.12,
    "typhoon": 0.20,
}

# 종합 점수 공식: R_total = PEAK_W * peak + AVG_W * weighted_avg
PEAK_WEIGHT: float = 0.7
AVG_WEIGHT:  float = 0.3

_RISK_COLS = ["R_rain", "R_heat", "R_wind", "R_uv", "R_typhoon"]
_WEIGHT_KEYS = ["rain",  "heat",  "wind",  "uv",  "typhoon"]


def compute_r_total(df: pd.DataFrame) -> pd.Series:
    """
    개별 위험도 컬럼(R_rain, R_heat, R_wind, R_uv, R_typhoon)으로부터
    종합 위험도 R_total을 계산해 반환.

    R_total = PEAK_WEIGHT * peak + AVG_WEIGHT * weighted_avg
    """
    weighted = sum(
        RISK_WEIGHTS[k] * df[col]
        for k, col in zip(_WEIGHT_KEYS, _RISK_COLS)
    ).clip(0, 1)

    peak = df[_RISK_COLS].max(axis=1)

    return (PEAK_WEIGHT * peak + AVG_WEIGHT * weighted).clip(0, 1)
