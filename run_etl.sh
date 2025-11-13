#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# activate WSL venv
source venv_wsl/bin/activate

# rotate logs (keep last 30)
LOGDIR=logs
mkdir -p "$LOGDIR"
python etl.py 2>&1 | tee "$LOGDIR/etl_last.log"

