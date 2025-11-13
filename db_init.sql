-- db_init.sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS crypto_price_snapshots (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT,
    snapshot_time TIMESTAMP WITH TIME ZONE NOT NULL,
    price_usd NUMERIC,
    price_change_24h NUMERIC,
    price_change_percentage_24h NUMERIC,
    market_cap_usd NUMERIC,
    market_cap_rank INTEGER,
    total_volume NUMERIC,
    circulating_supply NUMERIC,
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    raw_json JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_symbol_snapshot UNIQUE (symbol, snapshot_time)
);

-- trigger to update updated_at on update (optional, helpful)
CREATE OR REPLACE FUNCTION set_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trig_set_updated_at ON crypto_price_snapshots;
CREATE TRIGGER trig_set_updated_at
BEFORE UPDATE ON crypto_price_snapshots
FOR EACH ROW
EXECUTE PROCEDURE set_updated_at_column();
