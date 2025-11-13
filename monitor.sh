#!/usr/bin/env bash
# monitor.sh — produces ETL run summary using psql
# Usage: ./monitor.sh [output_log]
# If output_log is provided, append report to the logfile as well.

set -euo pipefail

# Config — edit if your DB URL differs or you want a different output path
DBURL="${DATABASE_URL:-postgresql://dbuser:dbpassword@localhost:5432/dbname}"
OUTLOG="${1:-/mnt/d/DataPipeline/logs/etl_monitor_$(date +%F).log}"
TMP="/tmp/etl_monitor_$$.txt"

# SQL queries (same as discussed)
SQL_LAST20=$(
cat <<'SQL'
SELECT run_id, job_name, run_at, finished_at, status, duration_seconds, rows_loaded
FROM etl_runs
ORDER BY run_at DESC
LIMIT 20;
SQL
)

SQL_FAIL7=$(
cat <<'SQL'
SELECT status, count(*) as cnt FROM etl_runs
WHERE run_at >= now() - interval '7 days'
GROUP BY status;
SQL
)

SQL_AVG30=$(
cat <<'SQL'
SELECT round(avg(duration_seconds)::numeric,3) as avg_seconds FROM (
  SELECT duration_seconds FROM etl_runs WHERE duration_seconds IS NOT NULL ORDER BY run_at DESC LIMIT 30
) t;
SQL
)

# Helpers
run_query() {
  local sql="$1"
  psql "$DBURL" -t -A -F $'\t' -c "$sql"
}

timestamp() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }

# Build report
{
  echo "ETL MONITOR REPORT — $(timestamp)"
  echo "DATABASE: $DBURL"
  echo
  echo "1) Last 20 runs (most recent first)"
  echo "run_id | job_name | run_at | finished_at | status | duration_seconds | rows_loaded"
  echo "--------------------------------------------------------------------------------"
  psql "$DBURL" -c "$SQL_LAST20"

  echo
  echo "2) Status counts (last 7 days)"
  echo "status | count"
  echo "-------------"
  run_query "$SQL_FAIL7" | sed 's/\t/ | /g' || true

  echo
  echo "3) Avg duration (last 30 runs)"
  run_query "$SQL_AVG30" | awk -F$'\t' '{print "avg_seconds | "$1}' || true

  echo
  echo "Generated: $(timestamp)"
} | tee -a "$TMP" | tee -a "$OUTLOG"

# cleanup temp
rm -f "$TMP"
exit 0

