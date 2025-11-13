-- create_etl_runs.sql
-- requires uuid-ossp created earlier (db_init.sql already created it). If not, create it:
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_name TEXT NOT NULL,                          -- e.g., 'coingecko_daily_snapshot'
    run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) NOT NULL,                     -- running | success | failed
    duration_seconds NUMERIC,                        -- elapsed time
    rows_loaded INTEGER,                             -- number of upserted rows
    error_text TEXT,                                 -- short error message / stack tail
    log_path TEXT,                                   -- path to logfile (optional)
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etl_runs_run_at ON etl_runs (run_at DESC);
