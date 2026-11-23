#!/usr/bin/env bash
# Local-only infrastructure; never connects to customer/cloud Kafka or databases.
set -euo pipefail
repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
lab_dir="$repo_dir/work/delivery-lab"
mkdir -p "$lab_dir/certs" "$lab_dir/results"
command -v docker >/dev/null
command -v openssl >/dev/null
mode="${1:-all}"
export LAB_STATE_DIR="$lab_dir"
export LAB_KAFKA_PORT="${LAB_KAFKA_PORT:-19092}"
export LAB_POSTGRES_PORT="${LAB_POSTGRES_PORT:-55432}"
if [[ "$mode" == "down" ]]; then
  docker compose -p kafkaui-delivery-lab --env-file "$lab_dir/lab.env" -f "$repo_dir/tests/delivery_lab/compose.yaml" down
  exit
fi
if [[ ! -f "$lab_dir/lab.env" ]]; then
  python3 - "$lab_dir/lab.env" <<'PY'
import os,secrets,sys
fd=os.open(sys.argv[1],os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'w') as f:f.write('LAB_DB_PASSWORD='+secrets.token_urlsafe(24)+'\n')
PY
fi
if [[ ! -f "$lab_dir/certs/server.crt" ]]; then
  openssl req -x509 -nodes -newkey rsa:2048 -days 30 -keyout "$lab_dir/certs/server.key" -out "$lab_dir/certs/server.crt" -subj '/CN=localhost' -addext 'subjectAltName=DNS:localhost,DNS:postgres,IP:127.0.0.1' 2>"$lab_dir/cert-generation.log"
  chmod 600 "$lab_dir/certs/server.key"
  chmod 644 "$lab_dir/certs/server.crt"
fi
if [[ ! -x "$lab_dir/venv/bin/python" ]]; then
  python3 -m venv "$lab_dir/venv"
fi
"$lab_dir/venv/bin/python" -m pip install --cache-dir "$lab_dir/pip-cache" -r "$repo_dir/tests/delivery_lab/requirements.txt" >"$lab_dir/install.log" 2>&1
docker compose -p kafkaui-delivery-lab --env-file "$lab_dir/lab.env" -f "$repo_dir/tests/delivery_lab/compose.yaml" up -d kafka postgres
cd "$repo_dir"
if [[ "$mode" == "all" || "$mode" == "verify" ]]; then
  "$lab_dir/venv/bin/python" -m unittest worker.test_delivery worker.test_credentials worker.test_callbacks worker.test_destinations
fi
"$lab_dir/venv/bin/python" tests/delivery_lab/run.py "$mode"
