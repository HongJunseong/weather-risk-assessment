# risk/rain_risk.py

import numpy as np
import pandas as pd
from dataclasses import dataclass
from weather_risk_assessment.utils.compute_risk_utils import _pick_mm, _pick_num, _logistic

import re
import numpy as np
import pandas as pd
from dataclasses import dataclass
from weather_risk_assessment.utils.compute_risk_utils import _pick_mm, _pick_num, _logistic

# 강수 문자열 파서
_MM_RX = re.compile(r'(-?\d+(?:\.\d+)?)\s*mm', re.I)

def _coerce_mm_series(s: pd.Series, default_below1: float = 0.5) -> pd.Series:
    """
    Series를 mm(float)로 강제 변환.
    - '강수없음' -> 0.0
    - '1mm 미만', '<1', '<1mm' -> default_below1(기본 0.5mm)
    - '5.0mm' 등 단위 포함 -> 수치 추출
    - 순수 숫자 문자열/숫자 -> float 변환
    - 실패 시 0.0
    """
    def parse_one(x):
        if pd.isna(x):
            return 0.0
        s = str(x).strip()

        # 자주 쓰이는 표현 방어
        if '강수없음' in s:
            return 0.0
        if '1mm 미만' in s or s.startswith('<1') or '<1mm' in s.lower():
            return float(default_below1)

        m = _MM_RX.search(s)
        if m:
            try:
                return float(m.group(1))
            except:
                return 0.0

        # 단위가 없고 그냥 숫자일 수도 있음
        try:
            return float(s)
        except:
            return 0.0

    out = s.map(parse_one).astype(float)
    return out.clip(lower=0.0).fillna(0.0)

# 지수 곡선 함수
def _exp_rise(x: pd.Series, a: float, p: float = 1.0) -> pd.Series:
    """x(mm) -> [0,1) 완만 상승. a↑: 약한 비에 둔감, p<1: 초반 민감도↑."""
    x = pd.to_numeric(x, errors="coerce").fillna(0).clip(lower=0)
    a = max(float(a), 1e-9)
    return 1.0 - np.exp(-np.power(x / a, p))

# 설정 파라미터
@dataclass
class RainRiskCfg:
    intens_a: float = 30.0    # 실황 민감도 (작을수록 초반 상승 빠름)
    intens_p: float = 0.95    # 실황 곡선 지수
    tail_start: float = 22.0  # 폭우 테일 시작(mm)
    tail_scale: float = 16.0  # 테일 완만 정도
    tail_max: float = 0.12    # 테일 최대 가산치
    pop_pow: float = 1.7      # POP 지수
    pop_w_dry: float = 0.012  # Dry 시 POP 가중
    pop_w_wet: float = 0.004  # Wet 시 POP 가중
    pcp_a: float = 38.0       # PCP 민감도
    pcp_w: float = 0.03       # PCP 최대 가중
    dry_cap: float = 0.07     # Dry 구간 상한
    k: float = 3.2            # 로지스틱 k
    x0: float = 0.56          # 로지스틱 중심
    out_lo: float = 0.010     # 최저 출력값
    out_hi: float = 0.97      # 최고 출력값
    drizzle_eps: float = 0.2  # '비 없음' 판정 기준(mm)


# main
def compute_rain_risk(df: pd.DataFrame, cfg: RainRiskCfg = RainRiskCfg()) -> pd.Series:
    """
    강수 위험도 계산
    - RN1/PCP 파싱 보강
    - Dry/Wet 레짐 분리
    - Wet: 실황 + 테일 + POP/PCP 반영
    - Dry: POP/PCP만 반영, 상한 적용
    - 로지스틱 앵커링 후 [0,1] 정규화
    """
    idx = df.index

    # 1) 입력 파싱 (util + 로컬 파서)
    rn1_pick = _pick_mm(df, ["RN1"]).fillna(0)
    pcp_pick = _pick_mm(df, ["PCP"]).fillna(0)
    pop      = _pick_num(df, ["POP"]).fillna(0).clip(0, 100)

    rn1 = _coerce_mm_series(df["RN1"]) if "RN1" in df.columns else _coerce_mm_series(rn1_pick.astype(str))
    pcp = _coerce_mm_series(df["PCP"]) if "PCP" in df.columns else _coerce_mm_series(pcp_pick.astype(str))
    pop01 = (pop / 100.0).clip(0, 1)

    # 2) Dry/Wet 레짐 판별
    is_dry = rn1 <= cfg.drizzle_eps

    # 3) 실황 강도 (Wet 전용)
    intens = _exp_rise(rn1, a=cfg.intens_a, p=cfg.intens_p)

    # 4) 폭우 테일 (Wet 전용)
    tail_mm = (rn1 - cfg.tail_start).clip(lower=0)
    tail = cfg.tail_max * (1.0 - np.exp(-tail_mm / max(cfg.tail_scale, 1e-9)))

    # 5) POP/PCP 보정
    pop_w = np.where(is_dry, cfg.pop_w_dry, cfg.pop_w_wet)
    pop_term = (pop_w * np.power(pop01, cfg.pop_pow)).astype(float)

    pcp_r = _exp_rise(pcp, a=cfg.pcp_a)
    pcp_gate = np.where(is_dry, 1.0, 0.4).astype(float)
    pcp_term = (cfg.pcp_w * pcp_gate * pcp_r).astype(float)

    # 6) Dry/Wet 합성
    wet_base = (0.985 * intens + tail + pop_term + pcp_term).clip(0, 1.3)
    dry_base = np.minimum(pop_term + pcp_term, cfg.dry_cap)
    base = pd.Series(np.where(is_dry, dry_base, wet_base), index=idx, dtype=float)

    # 7) 로지스틱 앵커링
    raw = _logistic(base, k=cfg.k, x0=cfg.x0)
    lo  = _logistic(pd.Series(0.0, index=idx), k=cfg.k, x0=cfg.x0)
    hi  = _logistic(pd.Series(1.0, index=idx), k=cfg.k, x0=cfg.x0)
    out = ((raw - lo) / (hi - lo + 1e-9)).clip(0, 1)

    # 8) 최종 출력 (클립 + 결측 보정)
    return out.clip(cfg.out_lo, cfg.out_hi).fillna(cfg.out_lo)