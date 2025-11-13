# transform.py
"""
Robust transform script for CoinGecko ETL.
- Accepts a JSON file path as argument
- If no path is given, picks the latest raw_coingecko_*.json file automatically
- Produces data/preview_coingecko_snapshot.csv
"""

import sys
import os
import json
from glob import glob
import pandas as pd
from datetime import timezone


# -------------------------
# Transform logic
# -------------------------
def transform_market_response(json_list):
    df = pd.json_normalize(json_list)

    keep = {
        "id": "symbol",
        "name": "name",
        "current_price": "price_usd",
        "market_cap": "market_cap_usd",
        "total_volume": "total_volume",
        "circulating_supply": "circulating_supply",
        "last_updated": "snapshot_time",
        "market_cap_rank": "market_cap_rank",
        "price_change_24h": "price_change_24h",
        "price_change_percentage_24h": "price_change_percentage_24h",
    }

    df = df[list(keep.keys())].rename(columns=keep)

    numeric_cols = [
        "price_usd", "market_cap_usd", "total_volume",
        "circulating_supply", "market_cap_rank",
        "price_change_24h", "price_change_percentage_24h"
    ]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], utc=True)
    df["fetched_at"] = pd.Timestamp.now(tz=timezone.utc)
    df["raw_json"] = [json.dumps(rec) for rec in json_list]

    df = df.drop_duplicates(subset=["symbol", "snapshot_time"])

    cols = [
        "symbol", "name", "snapshot_time", "price_usd",
        "price_change_24h", "price_change_percentage_24h",
        "market_cap_usd", "market_cap_rank",
        "total_volume", "circulating_supply",
        "fetched_at", "raw_json"
    ]

    return df[cols]


# -------------------------
# CLI execution
# -------------------------
def main():
    # 1) Determine which file to load
    if len(sys.argv) > 1:
        raw_path = sys.argv[1]
    else:
        files = sorted(glob("data/raw_coingecko_*.json"))
        if not files:
            print("❌ No raw_coingecko_*.json files found in data/. Run extractor first.")
            sys.exit(1)
        raw_path = files[-1]  # latest file

    if not os.path.exists(raw_path):
        print(f"❌ File does not exist: {raw_path}")
        sys.exit(1)

    # 2) Load JSON
    with open(raw_path, "r", encoding="utf-8") as f:
        try:
            json_list = json.load(f)
        except Exception as e:
            print(f"❌ Failed to parse JSON from {raw_path}: {e}")
            sys.exit(1)

    if not isinstance(json_list, list):
        print("❌ Expected JSON list (array). Got:", type(json_list))
        sys.exit(1)

    # 3) Transform
    df = transform_market_response(json_list)

    # 4) Output
    os.makedirs("data", exist_ok=True)
    out_path = "data/preview_coingecko_snapshot.csv"
    df.to_csv(out_path, index=False)

    print("\n✅ Transform completed.")
    print("Input JSON :", raw_path)
    print("Output CSV :", out_path)
    print(f"Rows       : {len(df)}")
    print("\nPreview:")
    print(df.head().to_string(index=False))


if __name__ == "__main__":
    main()
