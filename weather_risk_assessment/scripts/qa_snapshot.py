# scripts/qa_snapshot.py
# 데이터 적재/품질 점검용 스냅샷 스크립트
from __future__ import annotations
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SINK_DIR = DATA / "live"

def to_str(df: pd.DataFrame, n=10, cols=None):
    """DataFrame → 문자열 (상위 n행, 선택 컬럼만)"""
    if cols:
        cols = [c for c in cols if c in df.columns]  # 존재하는 컬럼만 유지
        df = df[cols]
    return "<empty>" if df.empty else df.head(n).to_string(index=False)

def main():
    # --- 입력 로드 ---
    nc = pd.read_parquet(SINK_DIR / "ultra_nowcast.parquet")
    fc = pd.read_parquet(SINK_DIR / "ultra_shortfcst.parquet")
    risk_path = (DATA / "risk_latest.parquet") if (DATA / "risk_latest.parquet").exists() else (DATA / "risk_scores.parquet")
    rk = pd.read_parquet(risk_path)
    admins = pd.read_csv(DATA / "admin_centroids.csv", encoding="utf-8-sig")

    # --- 호출 커버리지 (선택적) ---
    call_path = DATA / "admin_list.csv"
    requested = None
    if call_path.exists():
        call = pd.read_csv(call_path, encoding="utf-8-sig")
        requested = call.drop_duplicates(["nx","ny"]).shape[0]

    uniq_nc = nc[["nx","ny"]].drop_duplicates().shape[0]
    uniq_fc = fc[["nx","ny"]].drop_duplicates().shape[0]
    uniq_rk = rk[["nx","ny"]].drop_duplicates().shape[0]

    print(f"nowcast: {nc.shape}  shortfcst: {fc.shape}  risk: {rk.shape}")
    if requested is not None:
        print(f"coverage (unique cells): requested={requested}, nowcast={uniq_nc}, shortfcst={uniq_fc}, risk={uniq_rk}")

    # --- 최신 시각/예보 범위 확인 ---
    if {"baseDate","baseTime"} <= set(nc.columns):
        latest_nc = nc[["baseDate","baseTime"]].dropna().tail(1).to_dict("records")
        print("nowcast latest base:", latest_nc)
    if {"fcstDate"} <= set(fc.columns):
        print("shortfcst horizon:", fc["fcstDate"].min(), "→", fc["fcstDate"].max())

    # --- 컬럼명 보정 ---
    rk = rk.rename(columns={"R_tatal":"R_total", "_R_heat":"R_heat"})

    # --- 위험도 분포 확인 ---
    if "R_total" in rk.columns:
        na = rk[["nx","ny","R_total"]].isna().sum().to_dict()
        print("NA counts in risk:", na)
        q = rk["R_total"].quantile([0, .25, .5, .75, .9, .95, 1]).round(3)
        print("R_total quantiles:", q.to_dict())
    else:
        print("R_total 컬럼이 없습니다. weights/스코어러 설정 확인 필요.")

    # --- 상위 핫스팟 (세부 위험요인 포함) ---
    extra_hazards = [c for c in ["R_rain","R_heat","R_uv","R_typhoon","R_wind"] if c in rk.columns]
    cols_for_top = (
        ["admin_name","nx","ny","R_total"] + extra_hazards
        if "R_total" in rk.columns else
        ["admin_name","nx","ny"] + extra_hazards
    )

    top = rk.sort_values("R_total", ascending=False) if "R_total" in rk.columns else rk.copy()
    top = top.merge(admins[["admin_code","admin_name","nx","ny"]], on=["nx","ny"], how="left")
    print("\nTop hotspots:")
    print(to_str(top, n=10, cols=cols_for_top))

if __name__ == "__main__":
    main()
