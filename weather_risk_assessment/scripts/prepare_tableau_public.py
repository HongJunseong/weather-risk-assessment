from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from weather_risk_assessment.paths import DATA_ROOT


REQUIRED_RISK_COLUMNS = {
    "admin_names",
    "fcst_ts",
    "nx",
    "ny",
    "R_total",
    "risk_level",
}
RISK_COLUMNS = {
    "R_total": "risk_score",
    "R_rain": "rain_risk",
    "R_heat": "heat_risk",
    "R_wind": "wind_risk",
    "R_uv": "uv_risk",
    "R_typhoon": "typhoon_risk",
}


def prepare_tableau_csv(
    risk_path: str | Path,
    centroids_path: str | Path,
    output_path: str | Path,
) -> Path:
    risk = pd.read_parquet(risk_path)
    missing = REQUIRED_RISK_COLUMNS - set(risk.columns)
    if missing:
        raise ValueError(f"Missing columns in risk data: {sorted(missing)}")

    centroids = pd.read_csv(centroids_path, encoding="utf-8-sig")
    required_centroids = {"admin_name", "nx", "ny", "lat", "lon"}
    missing = required_centroids - set(centroids.columns)
    if missing:
        raise ValueError(f"Missing columns in centroid data: {sorted(missing)}")

    tableau = risk.assign(admin_name=risk["admin_names"].str.split("|")).explode(
        "admin_name"
    )
    tableau = tableau.merge(
        centroids[list(required_centroids)].drop_duplicates(["admin_name", "nx", "ny"]),
        on=["admin_name", "nx", "ny"],
        how="left",
        validate="many_to_one",
    )
    if tableau[["lat", "lon"]].isna().any(axis=None):
        raise ValueError("Centroid coordinates are missing for one or more regions.")

    columns = ["admin_name", "fcst_ts", *RISK_COLUMNS, "risk_level", "lat", "lon"]
    columns = [column for column in columns if column in tableau.columns]
    tableau = tableau[columns].rename(
        columns={
            "fcst_ts": "forecast_time",
            "lat": "latitude",
            "lon": "longitude",
            **RISK_COLUMNS,
        }
    )
    tableau = tableau.sort_values(
        ["risk_score", "admin_name"], ascending=[False, True]
    )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    tableau.to_csv(output, index=False, encoding="utf-8-sig")
    return output


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare a public CSV from the local Gold latest Parquet export."
    )
    parser.add_argument("risk_path", help="Local Parquet file or directory")
    parser.add_argument(
        "--centroids",
        default=DATA_ROOT / "admin_centroids.csv",
        help="Administrative centroid CSV",
    )
    parser.add_argument(
        "--output",
        default=DATA_ROOT / "tableau_public.csv",
        help="Output CSV path",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    output = prepare_tableau_csv(args.risk_path, args.centroids, args.output)
    print(f"saved: {output}")


if __name__ == "__main__":
    main()
