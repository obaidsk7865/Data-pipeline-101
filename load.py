# load.py
"""
Load module: upsert pandas DataFrame into Postgres table crypto_price_snapshots.
Uses SQLAlchemy to create engine and psycopg2.extras.execute_values for fast bulk inserts.
"""

import os
import json
from typing import Optional
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_values, Json
import psycopg2

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL must be set in environment or .env")

TABLE_NAME = os.getenv("CRYPTO_TABLE", "crypto_price_snapshots")

def get_engine():
    # create_engine accepts SQLAlchemy DB URL (e.g. postgresql+psycopg2://user:pass@host:5432/db)
    return create_engine(DATABASE_URL, pool_pre_ping=True)

def upsert_df(df: pd.DataFrame, table_name: str = TABLE_NAME, batch_size: int = 1000):
    """
    Upsert a DataFrame into Postgres table_name.
    Expects DataFrame with columns:
      ['symbol','name','snapshot_time','price_usd','price_change_24h','price_change_percentage_24h',
       'market_cap_usd','market_cap_rank','total_volume','circulating_supply','fetched_at','raw_json']
    """
    if df is None or df.shape[0] == 0:
        print("No rows to upsert.")
        return

    engine = get_engine()
    # Ensure we use a raw psycopg2 connection for execute_values speed
    conn = engine.raw_connection()
    try:
        with conn.cursor() as cur:
            # Build rows list. For JSONB use psycopg2.extras.Json to adapt Python -> JSONB
            rows = []
            for _, r in df.iterrows():
                # r may be a Series; convert types carefully
                symbol = r.get("symbol")
                name = r.get("name")
                snapshot_time = r.get("snapshot_time").to_pydatetime() if hasattr(r.get("snapshot_time"), "to_pydatetime") else r.get("snapshot_time")
                price_usd = float(r["price_usd"]) if pd.notna(r.get("price_usd")) else None
                price_change_24h = float(r["price_change_24h"]) if pd.notna(r.get("price_change_24h")) else None
                price_change_percentage_24h = float(r["price_change_percentage_24h"]) if pd.notna(r.get("price_change_percentage_24h")) else None
                market_cap_usd = float(r["market_cap_usd"]) if pd.notna(r.get("market_cap_usd")) else None
                market_cap_rank = int(r["market_cap_rank"]) if pd.notna(r.get("market_cap_rank")) else None
                total_volume = float(r["total_volume"]) if pd.notna(r.get("total_volume")) else None
                circulating_supply = float(r["circulating_supply"]) if pd.notna(r.get("circulating_supply")) else None
                fetched_at = r.get("fetched_at").to_pydatetime() if hasattr(r.get("fetched_at"), "to_pydatetime") else r.get("fetched_at")
                # raw_json may already be a JSON string or Python dict; normalize to Python dict
                raw_json_obj = None
                raw = r.get("raw_json")
                if isinstance(raw, str):
                    try:
                        raw_json_obj = json.loads(raw)
                    except Exception:
                        # keep string as-is
                        raw_json_obj = raw
                else:
                    raw_json_obj = raw

                rows.append((
                    symbol, name, snapshot_time, price_usd, price_change_24h, price_change_percentage_24h,
                    market_cap_usd, market_cap_rank, total_volume, circulating_supply, fetched_at, Json(raw_json_obj)
                ))

            # Insert statement uses execute_values with ON CONFLICT to upsert
            insert_sql = f"""
            INSERT INTO {table_name}
                (symbol, name, snapshot_time, price_usd, price_change_24h, price_change_percentage_24h,
                 market_cap_usd, market_cap_rank, total_volume, circulating_supply, fetched_at, raw_json)
            VALUES %s
            ON CONFLICT (symbol, snapshot_time) DO UPDATE SET
                name = EXCLUDED.name,
                price_usd = EXCLUDED.price_usd,
                price_change_24h = EXCLUDED.price_change_24h,
                price_change_percentage_24h = EXCLUDED.price_change_percentage_24h,
                market_cap_usd = EXCLUDED.market_cap_usd,
                market_cap_rank = EXCLUDED.market_cap_rank,
                total_volume = EXCLUDED.total_volume,
                circulating_supply = EXCLUDED.circulating_supply,
                fetched_at = EXCLUDED.fetched_at,
                raw_json = EXCLUDED.raw_json,
                updated_at = NOW()
            ;
            """

            # execute in batches for big DataFrames
            execute_values(cur, insert_sql, rows, page_size=batch_size)
        conn.commit()
        print(f"Upserted {len(rows)} rows into {table_name}.")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    # quick standalone test: read a CSV exported from transform stage
    import sys
    if len(sys.argv) < 2:
        print("Usage: python load.py path/to/preview_coingecko_snapshot.csv")
        sys.exit(1)

    path = sys.argv[1]
    df = pd.read_csv(path, parse_dates=["snapshot_time", "fetched_at"])
    # If raw_json column is string-encoded JSON, ensure it's loaded as native Python object
    if "raw_json" in df.columns:
        def maybe_parse(x):
            try:
                return json.loads(x)
            except Exception:
                return x
        df["raw_json"] = df["raw_json"].apply(maybe_parse)
    upsert_df(df)
