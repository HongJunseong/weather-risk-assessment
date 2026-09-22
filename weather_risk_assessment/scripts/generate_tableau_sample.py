from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from weather_risk_assessment.paths import DATA_ROOT
from weather_risk_assessment.risk.config import compute_r_total


def generate_sample(
    centroids_path: str | Path,
    output_path: str | Path,
    periods: int = 8,
    seed: int = 20250810,
) -> Path:
    if periods < 1:
        raise ValueError("periods must be at least 1")

    centroids = pd.read_csv(centroids_path, encoding="utf-8-sig")
    required = {"admin_name", "lat", "lon"}
    missing = required - set(centroids.columns)
    if missing:
        raise ValueError(f"Missing columns in centroid data: {sorted(missing)}")

    regions = centroids[["admin_name", "lat", "lon"]].drop_duplicates()
    times = pd.DataFrame(
        {
            "forecast_time": pd.date_range(
                "2025-08-10 00:00:00", periods=periods, freq="3h"
            )
        }
    )
    sample = regions.merge(times, how="cross")

    rng = np.random.default_rng(seed)
    risk_columns = ["R_rain", "R_heat", "R_wind", "R_uv", "R_typhoon"]
    sample[risk_columns] = rng.beta(1.5, 3.0, size=(len(sample), len(risk_columns)))
    sample["R_total"] = compute_r_total(sample)
    sample["risk_level"] = np.select(
        [sample["R_total"] >= 0.8, sample["R_total"] >= 0.6, sample["R_total"] >= 0.4],
        ["VERY_HIGH", "HIGH", "MED"],
        default="LOW",
    )

    sample = sample.rename(
        columns={
            "lat": "latitude",
            "lon": "longitude",
            "R_total": "risk_score",
            "R_rain": "rain_risk",
            "R_heat": "heat_risk",
            "R_wind": "wind_risk",
            "R_uv": "uv_risk",
            "R_typhoon": "typhoon_risk",
        }
    )
    sample["forecast_time"] = sample["forecast_time"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    score_columns = [
        "risk_score",
        "rain_risk",
        "heat_risk",
        "wind_risk",
        "uv_risk",
        "typhoon_risk",
    ]
    sample[score_columns] = sample[score_columns].round(4)
    sample["data_status"] = "SYNTHETIC_DEMO"
    sample = sample.sort_values(["forecast_time", "admin_name"])

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    sample.to_csv(output, index=False, encoding="utf-8-sig")
    return output


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deterministic Tableau demo data from public centroids."
    )
    parser.add_argument(
        "--centroids", default=DATA_ROOT / "admin_centroids.csv"
    )
    parser.add_argument("--output", default=DATA_ROOT / "tableau_demo.csv")
    parser.add_argument("--periods", type=int, default=8)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    output = generate_sample(args.centroids, args.output, args.periods)
    print(f"saved: {output}")


if __name__ == "__main__":
    main()
