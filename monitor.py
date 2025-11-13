# monitor.py  (create this file in project root), or paste into etl.py
import time
import uuid
import traceback
from sqlalchemy import text
from load import get_engine   # re-use get_engine from your load.py
import os
import requests
from notifiers import slack_notify 

def record_run_start(job_name: str, log_path: str | None = None):
    """
    Inserts a 'running' row and returns (run_id, start_ts)
    start_ts: monotonic timestamp (float) for duration measurement
    run_id: uuid (str) for later update
    """
    run_id = str(uuid.uuid4())
    start_ts = time.time()
    engine = get_engine()
    sql = text("""
        INSERT INTO etl_runs (run_id, job_name, run_at, status, log_path)
        VALUES (:run_id, :job_name, now(), 'running', :log_path)
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"run_id": run_id, "job_name": job_name, "log_path": log_path})
    return run_id, start_ts

  # new import

def record_run_end(run_id: str, start_ts: float, status: str = "success",
                   rows_loaded: int | None = None, error: Exception | None = None):
    """
    Updates the etl_runs row with finished_at, status, duration and optional error text/rows.
    Also sends Slack notifications on success or failure (non-blocking).
    """
    duration = round(time.time() - start_ts, 3)
    error_text = None
    if error is not None:
        # keep a short stack tail
        tb = "".join(traceback.format_exception_only(type(error), error))
        error_text = tb.strip()

    engine = get_engine()
    sql = text("""
        UPDATE etl_runs
        SET finished_at = now(),
            status = :status,
            duration_seconds = :duration,
            rows_loaded = :rows_loaded,
            error_text = :error_text
        WHERE run_id = :run_id
    """)
    with engine.begin() as conn:
        conn.execute(sql, {
            "status": status,
            "duration": duration,
            "rows_loaded": rows_loaded,
            "error_text": error_text,
            "run_id": run_id
        })

    # Compose Slack message
    if status == "failed":
        msg = (
            f":x: *ETL FAILED*\n"
            f"> *Job:* coingecko_daily_snapshot\n"
            f"> *Run ID:* `{run_id}`\n"
            f"> *Duration:* {duration}s\n"
            f"> *Rows loaded:* {rows_loaded if rows_loaded is not None else 'N/A'}\n"
            f"> *Error:* {error_text}\n"
            f"> *Log:* `{os.path.basename(os.getenv('LOGFILE','unknown'))}`"
        )
        # non-blocking best-effort
        try:
            slack_notify(msg)
        except Exception:
            pass
    else:
        msg = (
            f":white_check_mark: *ETL SUCCESS*\n"
            f"> *Job:* coingecko_daily_snapshot\n"
            f"> *Run ID:* `{run_id}`\n"
            f"> *Duration:* {duration}s\n"
            f"> *Rows loaded:* {rows_loaded if rows_loaded is not None else 0}\n"
            f"> *Log:* `{os.path.basename(os.getenv('LOGFILE','unknown'))}`"
        )
        try:
            slack_notify(msg)
        except Exception:
            pass
