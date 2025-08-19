# scripts/make_kepler_geojson.py
from __future__ import annotations
from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

def main():
    # 최신 위험도와 행정구역 중심점 로드
    risk = pd.read_parquet(DATA / "risk_latest.parquet")
    cent = pd.read_csv(DATA / "admin_centroids.csv", encoding="utf-8-sig")

    # (nx,ny) 기준으로 행정명 매칭
    cols = ["admin_code","admin_name","lat","lon","nx","ny"]
    cent = cent[[c for c in cols if c in cent.columns]]
    df = risk.merge(cent, on=["nx","ny"], how="left")

    # GeoJSON Feature 생성
    feats = []
    for _, r in df.iterrows():
        geom = {"type": "Point", "coordinates": [float(r["lon"]), float(r["lat"])]}
        props = {k: r.get(k) for k in [
            "admin_code","admin_names","R_total","R_rain","R_heat","R_uv","R_typhoon",
            "RN1","PCP","POP","T1H","TMP","REH","WSD","PTY","SKY",
            "baseDate","baseTime","fcstDate","fcstTime"
        ] if k in df.columns}
        feats.append({"type": "Feature", "geometry": geom, "properties": props})

    # GeoJSON 저장
    gj = {"type": "FeatureCollection", "features": feats}
    out = DATA / "risk_latest_points.geojson"
    out.write_text(json.dumps(gj, ensure_ascii=False), encoding="utf-8")
    print("saved:", out)

if __name__ == "__main__":
    main()
