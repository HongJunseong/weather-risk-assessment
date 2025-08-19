# scripts/make_admin_list.py

from __future__ import annotations
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
SRC  = DATA / "admin_centroids.csv"           # 입력: 행정구역 중심점
DST  = DATA / "unique_admin_centroids.csv"    # 출력: 호출용 고유 (nx,ny) 목록
GRID = DATA / "grid_latlon.parquet"           # 추가 출력: 태풍 계산용 격자(lat/lon 포함)

def main():
    # --- 입력 ---
    df = pd.read_csv(SRC, encoding="utf-8-sig")

    # --- 호출 리스트 (nx,ny 고유) ---
    call = (
        df.drop_duplicates(["nx","ny"])
          .reset_index(drop=True)
    )
    call.to_csv(DST, index=False, encoding="utf-8-sig")

    # --- 격자 (nx,ny,lat,lon 고유) ---
    need = {"nx","ny","lat","lon"}
    if not need.issubset(df.columns):
        raise ValueError(f"{SRC}에 필요한 컬럼이 없습니다: {sorted(need)}")

    grid = (
        df[["nx","ny","lat","lon"]]
        .dropna(subset=["lat","lon"])
        .astype({"nx":"Int64","ny":"Int64"})
        .drop_duplicates(["nx","ny"])
        .reset_index(drop=True)
    )
    GRID.parent.mkdir(parents=True, exist_ok=True)
    grid.to_parquet(GRID, index=False)

    print(f"saved: {DST} rows={len(call)} (from {len(df)})")
    print(f"saved: {GRID} rows={len(grid)}")

if __name__ == "__main__":
    main()
