"""Forecast station charging volume with a 24-hour persistence baseline."""

import argparse
import json
from datetime import datetime, timezone

from .config import configured_store
from .features import LOOKBACK, station_features
from .store import Store

MODEL_VERSION = "station-persistence-24h-v1"


def forecast(store: Store, as_of: datetime) -> dict:
    features = station_features(store, as_of)
    cutoff = datetime.fromisoformat(features["as_of_utc"])
    return {
        "model_version": MODEL_VERSION,
        "feature_version": features["feature_version"],
        "as_of_utc": features["as_of_utc"],
        "target_start_utc": features["as_of_utc"],
        "target_end_utc": (cutoff + LOOKBACK).isoformat(timespec="microseconds"),
        "predictions": [
            {"station_id": row["station_id"], "predicted_energy_wh": row["completed_energy_wh_24h"]}
            for row in features["rows"]
        ],
    }


def backtest(store: Store, as_of: datetime) -> dict:
    prediction = forecast(store, as_of)
    target_end = datetime.fromisoformat(prediction["target_end_utc"])
    if target_end > datetime.now(timezone.utc):
        raise ValueError("backtest target period has not ended")
    actual = {
        row["station_id"]: row["completed_energy_wh_24h"]
        for row in station_features(store, target_end)["rows"]
    }
    predicted = {
        row["station_id"]: row["predicted_energy_wh"]
        for row in prediction["predictions"]
    }
    paired = sorted(actual.keys() & predicted.keys())
    paired_rows = []
    for station in paired:
        paired_rows.append({
            "station_id": station,
            "predicted_energy_wh": predicted[station],
            "actual_energy_wh": actual[station],
            "absolute_error_wh": abs(predicted[station] - actual[station]),
        })
    error = sum(row["absolute_error_wh"] for row in paired_rows)
    total_actual = sum(actual[station] for station in paired)
    return {
        **prediction,
        "actuals_as_of_utc": prediction["target_end_utc"],
        "metrics": {
            "evaluated_stations": len(paired),
            "forecast_station_count": len(predicted),
            "actual_station_count": len(actual),
            "mae_wh": error / len(paired) if paired else None,
            "wape": error / total_actual if total_actual else None,
        },
        "missing_actuals": sorted(predicted.keys() - actual.keys()),
        "missing_predictions": sorted(actual.keys() - predicted.keys()),
        "evaluated": paired_rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True, type=datetime.fromisoformat)
    parser.add_argument("--backtest", action="store_true", help="Evaluate the historical target period")
    args = parser.parse_args()
    try:
        run = backtest if args.backtest else forecast
        report = run(configured_store(), args.as_of)
    except ValueError as error:
        parser.exit(1, f"{error}\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
