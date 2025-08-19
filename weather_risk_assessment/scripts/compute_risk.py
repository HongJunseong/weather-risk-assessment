# scripts/compute_risk.py
from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd

from weather_risk_assessment.utils.compute_risk_utils import (
    _ensure_time_str, load_admin_map, _list_to_str, _pick_mm
)
from weather_risk_assessment.risk.heat_risk import compute_heat_risk
from weather_risk_assessment.risk.rain_risk import compute_rain_risk
from weather_risk_assessment.risk.typhoon_risk import compute_typhoon_risk
from weather_risk_assessment.risk.wind_risk import compute_wind_risk
from weather_risk_assessment.risk.uv_risk import compute_uv_risk


import sys, traceback
sys.excepthook = lambda et, ev, tb: traceback.print_exception(et, ev, tb)

# 경로 설정
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SINK_DIR = DATA / "live"

# 입력 데이터
NOWCAST = SINK_DIR / "ultra_nowcast.parquet"
SHORTFC = SINK_DIR / "ultra_shortfcst.parquet"
SHORTFC_VIL = SINK_DIR / "short_fcst.parquet"
TYPHOON = SINK_DIR / "typhoon.parquet"
UVPATH  = SINK_DIR / "uv.parquet"
ADMIN_MAP = DATA / "admin_centroids.csv"


def main():
    # ---------------- 입력 데이터 로드 ----------------
    if not NOWCAST.exists() and not SHORTFC.exists():
        raise SystemExit("no nowcast/shortfcst inputs in data/.")

    df_nc = pd.read_parquet(NOWCAST) if NOWCAST.exists() else pd.DataFrame()
    df_fc = pd.read_parquet(SHORTFC) if SHORTFC.exists() else pd.DataFrame()

    df_nc = _ensure_time_str(df_nc)
    df_fc = _ensure_time_str(df_fc)

    # shortfcst를 기본 데이터로 사용
    df = df_fc.copy()

    # nowcast 병합 (nx, ny, fcstDate, fcstTime 기준)
    if not df_nc.empty:
        keys = [k for k in ["nx", "ny", "fcstDate", "fcstTime"] if k in df.columns and k in df_nc.columns]
        if keys:
            df = df.merge(df_nc, on=keys, how="left", suffixes=("", "_nc"))

    # ---------------- 단기예보(PCP/POP) 병합 ----------------
    try:
        if SHORTFC_VIL.exists():
            df_vil = pd.read_parquet(SHORTFC_VIL)
            df_vil = _ensure_time_str(df_vil)

            # 필요한 컬럼만 (있을 때만)
            need = [c for c in ["nx","ny","fcstDate","fcstTime","PCP","POP","WSD"] if c in df_vil.columns]
            if not need:
                print("INFO: short_fcst has no PCP/POP/WSD columns; skip", flush=True)
            else:
                df_vil = df_vil[need].copy()

                # 키/타입 정합성: nx,ny Int64, fcstTime 4자리 문자열
                for col in ("nx","ny"):
                    if col in df_vil.columns:
                        df_vil[col] = pd.to_numeric(df_vil[col], errors="coerce").astype("Int64")
                for col in ("nx","ny"):
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

                if "fcstTime" in df_vil.columns:
                    df_vil["fcstTime"] = df_vil["fcstTime"].astype(str).str.zfill(4)
                if "fcstTime" in df.columns:
                    df["fcstTime"] = df["fcstTime"].astype(str).str.zfill(4)

                # 같은 시각 중 마지막 값 사용
                key_subset = [c for c in ["nx","ny","fcstDate","fcstTime"] if c in df_vil.columns]
                if key_subset:
                    df_vil = (
                        df_vil.sort_values(["fcstDate","fcstTime"])
                            .drop_duplicates(subset=key_subset, keep="last")
                    )

                # 병합 키
                merge_keys = [k for k in ["nx","ny","fcstDate","fcstTime"] if k in df.columns and k in df_vil.columns]
                if merge_keys:
                    df = df.merge(df_vil, on=merge_keys, how="left", suffixes=("", "_vil"))

                    # 단기예보 값을 우선(coalesce): 기존 값이 NaN이면 *_vil로 채움
                    for col in ("PCP","POP","WSD"):
                        vil_col = f"{col}_vil"
                        if vil_col in df.columns:
                            if col in df.columns:
                                df[col] = df[col].where(df[col].notna(), df[vil_col])
                            else:
                                df[col] = df[vil_col]
                            df.drop(columns=[vil_col], inplace=True)

                    # 타입 정리: 숫자로 강제 (PCP는 mm, POP는 %)
                    if "PCP" in df.columns:
                        df["PCP"] = pd.to_numeric(df["PCP"], errors="coerce")
                    if "POP" in df.columns:
                        df["POP"] = pd.to_numeric(df["POP"], errors="coerce").clip(0, 100)
                    if "WSD" in df.columns:
                        df["WSD"] = pd.to_numeric(df["WSD"], errors="coerce")
                else:
                    print(f"INFO: merge skipped — no common keys in df & short_fcst: {merge_keys}", flush=True)
        else:
            print("INFO: short_fcst.parquet not found; skip short-forecast merge", flush=True)
    except Exception as e:
        import traceback
        print("WARN: short-forecast merge failed, skipping:", e, flush=True)
        traceback.print_exc()

    # ---------------- 태풍 데이터 병합 ----------------
    if TYPHOON.exists():
        df_ty = pd.read_parquet(TYPHOON)
        df_ty = _ensure_time_str(df_ty)

        # nx, ny → Int64 변환
        for col in ("nx", "ny"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            if col in df_ty.columns:
                df_ty[col] = pd.to_numeric(df_ty[col], errors="coerce").astype("Int64")

        # 중복 보호 (같은 시각 키 중 마지막만 사용)
        df_ty = df_ty.sort_values(["fcstDate", "fcstTime"]).drop_duplicates(
            subset=[c for c in ["nx", "ny", "fcstDate", "fcstTime"] if c in df_ty.columns],
            keep="last"
        )

        keys = [k for k in ["nx", "ny", "fcstDate", "fcstTime"] if k in df.columns and k in df_ty.columns]
        if keys:
            df = df.merge(df_ty, on=keys, how="left")
        else:
            print("WARN: typhoon merge skipped (time keys missing)", flush=True)

        # 병합 성공률 확인
        if "TY_DISTANCE_KM" in df.columns:
            print("TY_DISTANCE_KM time-merge non-null rate:",
                  float(df["TY_DISTANCE_KM"].notna().mean()), flush=True)

    # ---------------- 자외선(UV) 데이터 병합 ----------------
    if UVPATH.exists():
        df_uv = pd.read_parquet(UVPATH)
        df_uv = _ensure_time_str(df_uv)
        merge_keys = [k for k in ["nx", "ny", "fcstDate", "fcstTime"] if k in df.columns and k in df_uv.columns]
        if merge_keys:
            df = df.merge(df_uv, on=merge_keys, how="left")

    # ---------------- 위험도 산출 ----------------
    df["R_rain"]    = compute_rain_risk(df)
    df["R_heat"]    = compute_heat_risk(df)
    df["R_wind"]    = compute_wind_risk(df)
    df["R_uv"]      = compute_uv_risk(df)
    df["R_typhoon"] = compute_typhoon_risk(df)

    # 가중 평균 + 최고값 기반 종합 점수
    w = {"rain":0.28, "heat":0.18, "wind":0.22, "uv":0.12, "typhoon":0.20}
    weighted = (
        w["rain"]*df["R_rain"] + w["heat"]*df["R_heat"] +
        w["wind"]*df["R_wind"] + w["uv"]*df["R_uv"] +
        w["typhoon"]*df["R_typhoon"]
    ).clip(0, 1)
    peak = pd.concat([df["R_rain"], df["R_heat"], df["R_wind"], df["R_uv"], df["R_typhoon"]], axis=1).max(axis=1)
    df["R_total"] = (0.7*peak + 0.3*weighted).clip(0, 1)

    # ---------------- 행정구역 정보 병합 ----------------
    adm_map = load_admin_map(ADMIN_MAP)
    if not adm_map.empty:
        if "nx" in df.columns: df["nx"] = pd.to_numeric(df["nx"], errors="coerce").astype("Int64")
        if "ny" in df.columns: df["ny"] = pd.to_numeric(df["ny"], errors="coerce").astype("Int64")

        adm_list = (
            adm_map
            .groupby(["nx", "ny"])
            .agg({
                "admin_name": lambda s: sorted(set(map(str, s.dropna().tolist())))
            })
            .reset_index()
            .rename(columns={"admin_name": "admin_names"})
        )
        adm_list["admin_names"] = adm_list["admin_names"].apply(_list_to_str)

        df = df.merge(adm_list, on=["nx", "ny"], how="left")
    else:
        df["admin_names"] = None

    # ---------------- 결과 저장 ----------------
    out_parq = DATA / "risk_latest.parquet"
    out_csv  = DATA / "risk_latest.csv"
    keep_cols = [c for c in [
        "nx","ny","admin_names",
        "baseDate","baseTime","fcstDate","fcstTime",
        "RN1","PCP","POP","WSD","WGS","GUST","UUU","VVV",
        "T1H","TMP","REH","PTY","SKY",
        "UVI","UV_INDEX",
        "TY_DISTANCE_KM","TY_MAX_WIND","TY_WARNING",
        "R_rain","R_heat","R_wind","R_uv","R_typhoon","R_total"
    ] if c in df.columns]

    df[keep_cols].to_parquet(out_parq, index=False)
    df[keep_cols].to_csv(out_csv, index=False, encoding="utf-8-sig")
    print("saved:", out_parq, out_csv)

    # ---------------- 디버그 출력 ----------------
    try:
        rn1_parsed = _pick_mm(df, ["RN1"])
        print("DEBUG rain: rn1_parsed.describe():", rn1_parsed.describe().to_dict())
        print("DEBUG rain: sample row:",
              df[[c for c in ["RN1","PCP","POP","WSD","R_rain"] if c in df.columns]]
              .head(2).to_dict(orient="records"))
    except Exception as e:
        print("DEBUG rain print failed:", e)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("FATAL in compute_risk.py:", repr(e), flush=True)
        traceback.print_exc()
        raise
