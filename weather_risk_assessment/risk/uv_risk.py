# risk/uv_risk.py

import numpy as np
import pandas as pd
from weather_risk_assessment.utils.compute_risk_utils import _num, _minmax, _logistic, _pick_num, _pick_mm

def compute_uv_risk(df: pd.DataFrame) -> pd.Series:
    # 1) 직접 UVI 지표가 있을 경우
    for u in ["UVI","UV_INDEX","UV"]:
        if u in df.columns:
            uvi = _num(df[u])
            score = (uvi / 12.0).clip(0, 1)                  # WHO scale 기준 정규화
            return _logistic(score, k=5, x0=0.4).fillna(0)   # 시그모이드 보정

    # 2) 대체 추정 (없을 경우: 하늘/시간/기온/강수로 추정)
    # SKY: 1=맑음, 4=흐림 → 맑을수록 1.0
    sky = _pick_num(df, ["SKY"])
    sky_factor = (4 - sky.fillna(3.0)) / 3.0

    # 기온: 정규화
    temp = _pick_num(df, ["T1H","TMP"])
    temp_factor = _minmax(temp).fillna(0.5)

    # 시간대: 10~15시 최대, 8~17시 중간, 그 외 낮음
    try:
        hh = df["fcstTime"].astype(str).str[:2].astype(int)
    except Exception:
        hh = pd.Series(12, index=df.index)
    hour_factor = pd.Series(
        np.where((hh>=10)&(hh<=15), 1.0,
        np.where((hh>=8)&(hh<=17), 0.7, 0.3)),
        index=df.index
    )

    # 강수: 있을 경우 감쇠
    rain_factor = pd.Series(1.0, index=df.index)
    if "PCP" in df.columns or any(c.startswith("PCP_") for c in df.columns):
        pcp = _pick_mm(df, ["PCP"])
        is_rain = pcp.fillna(0) > 0
        rain_factor = rain_factor.where(~is_rain, 0.4)
    if "POP" in df.columns or any(c.startswith("POP_") for c in df.columns):
        pop = _pick_num(df, ["POP"]).fillna(0) / 100.0
        rain_factor = rain_factor * (1 - 0.5*pop.clip(0,1))

    # 3) 합산 (가중 평균 × 강수 보정)
    raw = (0.45*sky_factor + 0.35*temp_factor + 0.20*hour_factor) * rain_factor

    # 4) 시그모이드 보정
    return _logistic(raw.clip(0,1), k=5, x0=0.5).fillna(0)
