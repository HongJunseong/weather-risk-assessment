# scripts/build_grid_latlon.py
from __future__ import annotations
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

def main():
    # admin_centroids.csv -> (nx,ny,lat,lon) unique grid
    cent = pd.read_csv(DATA / "admin_centroids.csv", encoding="utf-8-sig")
    grid = (
        cent[["nx","ny","lat","lon"]]
        .dropna(subset=["lat","lon"])
        .astype({"nx":"Int64","ny":"Int64"})
        .drop_duplicates(["nx","ny"])
    )
    out = DATA / "grid_latlon.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    grid.to_parquet(out, index=False)
    print("saved:", out, grid.shape)

if __name__ == "__main__":
    main()