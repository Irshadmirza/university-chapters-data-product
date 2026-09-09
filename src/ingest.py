import argparse
import json
import os
from datetime import datetime, timezone

import requests

API_URL = (
    "https://services2.arcgis.com/5I7u4SJE1vUr79JC/"
    "arcgis/rest/services/UniversityChapters_Public/FeatureServer/0/query"
)


def get_data():
    params = {
        "where": "State IN ('CA','OR','WA')",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "json",
        "resultRecordCount": 1000,
    }

    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "error" in data:
        raise RuntimeError(f"ArcGIS API error: {data['error']}")

    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", help="Use local JSON instead of the API")
    parser.add_argument(
        "--out", default="data/bronze/university_chapters"
    )
    args = parser.parse_args()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if args.fixture:
        with open(args.fixture, "r", encoding="utf-8") as file:
            data = json.load(file)
    else:
        data = get_data()

    features = data.get("features", [])
    if not features:
        raise RuntimeError("Source returned no rows. Bronze was not created.")

    run_path = os.path.join(args.out, run_id)
    os.makedirs(run_path, exist_ok=True)

    bronze = {
        "ingest_run_id": run_id,
        "source_url": API_URL,
        "features": features,
    }

    output_file = os.path.join(run_path, "payload.json")
    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(bronze, file)

    print(f"ingest_run_id: {run_id}")
    print(f"rows_in: {len(features)}")
    print(f"bronze_file: {output_file}")


if __name__ == "__main__":
    main()
