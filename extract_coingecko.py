#!/usr/bin/env python3
"""
extract_coingecko.py
Fetch market data from CoinGecko public API for a list of coins.
Resilient: session with retries, timeout, and basic rate-limit handling.
Prints a compact preview (first 2 records) and writes full JSON to `data/raw_<timestamp>.json`.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from typing import List, Any, Dict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# CONFIG
BASE = os.getenv("COINGECKO_API_BASE", "https://api.coingecko.com/api/v3")
DEFAULT_IDS = os.getenv("COINGECKO_IDS", "bitcoin,ethereum").split(",")
VS_CURRENCY = os.getenv("COINGECKO_VS_CURRENCY", "usd")
TIMEOUT = 15  # seconds for the HTTP request
OUTPUT_DIR = "data"

# logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

def requests_session_with_retries(
    total_retries: int = 5,
    backoff_factor: float = 0.5,
    status_forcelist = (429, 500, 502, 503, 504)
) -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def fetch_prices(ids: List[str], vs_currency: str = "usd", per_page: int = 250) -> List[Dict[str, Any]]:
    """
    Call CoinGecko /coins/markets endpoint.
    ids: list of coin ids (e.g., ['bitcoin','ethereum'])
    Returns: list of coin dicts (raw JSON)
    """
    session = requests_session_with_retries()
    endpoint = f"{BASE}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "ids": ",".join(ids),
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h"
    }

    logging.info("Requesting CoinGecko: ids=%s vs_currency=%s", ids, vs_currency)
    resp = session.get(endpoint, params=params, timeout=TIMEOUT)
    # Respect HTTP 429 (rate-limited) more politely
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "5"))
        logging.warning("Rate limited (429). Sleeping %s seconds then retrying once.", retry_after)
        time.sleep(retry_after)
        resp = session.get(endpoint, params=params, timeout=TIMEOUT)

    resp.raise_for_status()
    return resp.json()

def save_raw(data: Any, folder: str = OUTPUT_DIR) -> str:
    os.makedirs(folder, exist_ok=True)
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(folder, f"raw_coingecko_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path

def compact_preview(data: List[Dict[str, Any]], n: int = 2) -> None:
    print("\n--- compact preview (first {} records) ---".format(n))
    for i, rec in enumerate(data[:n]):
        minimal = {
            "id": rec.get("id"),
            "symbol": rec.get("symbol"),
            "name": rec.get("name"),
            "current_price": rec.get("current_price"),
            "market_cap": rec.get("market_cap"),
            "last_updated": rec.get("last_updated")
        }
        print(f"[{i}] {minimal}")
    print("--- end preview ---\n")

def main():
    ids = [x.strip() for x in DEFAULT_IDS if x.strip()]
    try:
        data = fetch_prices(ids=ids, vs_currency=VS_CURRENCY)
        if not isinstance(data, list):
            logging.error("Unexpected response shape: expected list, got %s", type(data))
            # try printing the raw payload for inspection
            print(json.dumps(data, indent=2)[:2000])
            sys.exit(2)

        # Save raw JSON
        saved_path = save_raw(data)
        logging.info("Saved raw payload to %s (records=%d)", saved_path, len(data))

        # Print compact preview
        compact_preview(data, n=3)

    except requests.HTTPError as he:
        logging.exception("HTTP error when fetching data: %s", he)
        sys.exit(3)
    except Exception as e:
        logging.exception("Unexpected error: %s", e)
        sys.exit(4)

if __name__ == "__main__":
    main()
#!/usr/bin/env python3