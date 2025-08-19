# risk/wind_risk.py

import numpy as np
import pandas as pd
from weather_risk_assessment.utils.compute_risk_utils import _pick_num, _num

def compute_wind_risk(df: pd.DataFrame) -> pd.Series:
    """
    풍속 기반 위험도
    - 입력: WSD(지속풍) 우선, 없으면 UUU/VVV로 벡터속도 사용
    - 보정: 보퍼트 감각에 맞춘 구간 보간
    - 출력: 0~1
    """
    # 1) 풍속 확보 (WSD → 없으면 √(U^2+V^2))
    s = _pick_num(df, ["WSD"])
    if "UUU" in df.columns and "VVV" in df.columns:
        spd_uv = np.sqrt(_num(df["UUU"])**2 + _num(df["VVV"])**2)
        s = pd.concat([s, spd_uv], axis=1).max(axis=1)

    # 2) 구간 보간 (m/s → 0~1), 보퍼트 체감에 맞춘 점들
    xp = [0, 4, 8, 12, 16, 22, 30]                 # 풍속 경계(m/s)
    fp = [0.0, 0.02, 0.15, 0.45, 0.70, 0.90, 1.00] # 체감 위험도

    risk = pd.Series(
        np.interp(s.fillna(0).to_numpy(float), xp, fp),
        index=s.index, dtype=float
    )
    return risk.clip(0, 1).fillna(0.0)
