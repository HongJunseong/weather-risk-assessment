# scripts/build_admin_centroids_from_shp.py
from __future__ import annotations
from pathlib import Path
import pandas as pd
import geopandas as gpd

ROOT = Path(__file__).resolve().parents[1]
IN_SHP = ROOT / "data" / "border" / "N3A_G0100000.shp"   # 행정구역 경계 SHP 파일
OUT    = ROOT / "data" / "admin_centroids.csv"           # 출력 CSV 경로

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utils.latlon_to_grid import latlon_to_grid


def main():
    # --- SHP 파일 읽기 ---
    try:
        gdf = gpd.read_file(IN_SHP)
    except Exception:
        gdf = gpd.read_file(IN_SHP, engine="pyogrio")  # fallback: pyogrio 엔진 사용

    # --- 행정구역 코드/이름 컬럼 지정 ---
    code_col = "BJCD" if "BJCD" in gdf.columns else ("UFID" if "UFID" in gdf.columns else None)
    name_col = "NAME" if "NAME" in gdf.columns else None
    if code_col is None or name_col is None:
        raise ValueError(f"필수 컬럼을 찾지 못했습니다. columns={list(gdf.columns)}")

    # --- 좌표계 변환 및 대표 점 추출 ---
    # WGS84(EPSG:4326)로 변환 후, centroid 대신 representative_point() 사용
    gdf = gdf.to_crs(epsg=4326)
    reps = gdf.representative_point()  # multipolygon도 내부 점 반환
    gdf["lat"] = reps.y
    gdf["lon"] = reps.x

    # --- DFS 격자(nx, ny) 변환 ---
    gdf["nxny"] = gdf.apply(lambda r: latlon_to_grid(float(r["lat"]), float(r["lon"])), axis=1)
    gdf["nx"] = gdf["nxny"].apply(lambda t: t[0])
    gdf["ny"] = gdf["nxny"].apply(lambda t: t[1])

    # --- 결과 테이블 구성 ---
    out = pd.DataFrame({
        "admin_code": gdf[code_col].astype(str),
        "admin_name": gdf[name_col].astype(str),
        "lat": gdf["lat"],
        "lon": gdf["lon"],
        "nx": gdf["nx"],
        "ny": gdf["ny"],
    })

    # --- 법정동 코드 앞 5자리(SIG_CD) 추가 ---
    if (out["admin_code"].str.len() >= 5).any():
        out["SIG_CD"] = out["admin_code"].str[:5]

    # --- 중복 제거 ---
    out = out.drop_duplicates(subset=["admin_code"]).reset_index(drop=True)

    # --- 저장 ---
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False, encoding="utf-8-sig")
    print(f"saved: {OUT}  rows={len(out)}")


if __name__ == "__main__":
    main()
