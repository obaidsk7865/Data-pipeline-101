# Data-pipeline-101
A production-ready Python ETL pipeline that extracts live crypto data from the CoinGecko API, transforms it with pandas, loads it into PostgreSQL, and runs daily via cron in WSL. Includes monitoring, logging, Slack alerts, and automated tests.
# DataPipeline101 — Mini ETL


1.**What**
  A compact end-to-end ETL pipeline that:
  - Extracts live crypto prices from CoinGecko.
  - Cleans and normalizes data with pandas.
  - Loads snapshots into PostgreSQL with idempotent upserts.
  - Automates daily execution via cron (WSL).
  - Observability: `etl_runs` table + Slack notifications + logs.


**Repo layout**
2.## Quickstart (WSL Ubuntu)


  1. Open WSL (Ubuntu) and `cd /mnt/d/DataPipeline`


  2. Install system prerequisites (if not already):


```bash
sudo apt update
sudo apt install -y python3-venv python3-pip libpq-dev build-essential

3.create and activate venv
  python3 -m venv venv_wsl
  source venv_wsl/bin/activate
  pip install --upgrade pip
  pip install -r requirements.txt

4.configure environment variables
  cp .env.example .env
5.initialize the database
  sudo -u postgres psql -d dbname -f db_init.sql
  sudo -u postgres psql -d dbname -f create_etl_runs.sql
  sudo -u postgres psql -d dbname -c "GRANT INSERT, UPDATE, SELECT ON TABLE etl_runs TO dbuser;"
6.run the pipeline
  python3 etl.py
7.verify inserted rows
  psql postgresql://dbuser:dbpassword@localhost:5432/dbname -c "SELECT symbol, snapshot_time, price_usd FROM crypto_price_snapshots ORDER BY snapshot_time DESC LIMIT 10;"
  psql postgresql://dbuser:dbpassword@localhost:5432/dbname -c "SELECT run_id, job_name, run_at, finished_at, status FROM etl_runs ORDER BY run_at DESC LIMIT 5;"
8.crontab -e
# add this line (runs at 02:00 daily)
0 2 * * * /mnt/d/DataPipeline/run_etl.sh >> /mnt/d/DataPipeline/logs/cron_etl.log 2>&1
9.run tests
pytest -q
