# # etl.py
# """
# Orchestrator: extract -> transform -> load
# Run from project root. Exits non-zero on failure.
# """

# import logging, sys, os
# from datetime import datetime, timezone
# from extract_coingecko import fetch_prices, save_raw, compact_preview
# from transform import transform_market_response
# from load import upsert_df
# import json

# # logging (file + stdout)
# LOGDIR = "logs"
# os.makedirs(LOGDIR, exist_ok=True)
# ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
# LOGFILE = os.path.join(LOGDIR, f"etl_{ts}.log")
# logging.basicConfig(level=logging.INFO,
#                     format="%(asctime)s %(levelname)s %(message)s",
#                     handlers=[logging.FileHandler(LOGFILE), logging.StreamHandler(sys.stdout)])

# def run(ids=None):
#     try:
#         logging.info("ETL starting")
#         ids = ids or ["bitcoin", "ethereum"]
#         # 1. Extract
#         raw = fetch_prices(ids=ids)
#         saved = save_raw(raw)
#         logging.info("Saved raw JSON to %s (records=%d)", saved, len(raw))

#         # 2. Transform
#         df = transform_market_response(raw)
#         logging.info("Transformed rows=%d", len(df))

#         # 3. Load
#         upsert_df(df)
#         logging.info("Load complete")

#         logging.info("ETL finished successfully")
#         return 0
#     except Exception as e:
#         logging.exception("ETL failed: %s", e)
#         return 2

# if __name__ == "__main__":
#     sys.exit(run())



# etl.py (updated)
import logging, sys, os
from datetime import datetime, timezone
from extract_coingecko import fetch_prices, save_raw
from transform import transform_market_response
from load import upsert_df
from monitor import record_run_start, record_run_end   # <-- import the functions
import json

LOGDIR = "logs"
os.makedirs(LOGDIR, exist_ok=True)
ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
LOGFILE = os.path.join(LOGDIR, f"etl_{ts}.log")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s",
                    handlers=[logging.FileHandler(LOGFILE), logging.StreamHandler(sys.stdout)])

JOB_NAME = "coingecko_daily_snapshot"

def run(ids=None):
    # start monitoring
    run_id, start_ts = record_run_start(job_name=JOB_NAME, log_path=LOGFILE)
    try:
        logging.info("ETL starting (run_id=%s)", run_id)
        ids = ids or ["bitcoin", "ethereum"]

        # 1) Extract
        raw = fetch_prices(ids=ids)
        saved = save_raw(raw)
        logging.info("Saved raw JSON to %s (records=%d)", saved, len(raw))

        # 2) Transform
        df = transform_market_response(raw)
        logging.info("Transformed rows=%d", len(df))

        # 3) Load
        upsert_df(df)
        logging.info("Load complete")

        # success: update monitoring
        record_run_end(run_id=run_id, start_ts=start_ts, status="success", rows_loaded=len(df))
        logging.info("ETL finished successfully (run_id=%s)", run_id)
        return 0
    except Exception as e:
        logging.exception("ETL failed: %s", e)
        # update monitoring row as failed (capture rows_loaded if available)
        try:
            # try to infer rows loaded if df exists
            rows = locals().get("df")
            rows_loaded = len(rows) if rows is not None else None
        except Exception:
            rows_loaded = None
        record_run_end(run_id=run_id, start_ts=start_ts, status="failed", rows_loaded=rows_loaded, error=e)
        return 2

if __name__ == "__main__":
    sys.exit(run())
