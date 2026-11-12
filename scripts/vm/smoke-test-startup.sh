#!/usr/bin/env bash
# VM startup script for an unattended cloud smoke test. It fetches the latest uploaded code,
# runs the unit tests and small IA + NCTB ingests into gs://<ops>/smoke-test/data (never the
# dataset bucket), interrupts and resumes one run, verifies the results, uploads the log and
# powers the VM off. Delete the VM afterwards.
#
# Usage (from your machine):
#   scripts/gcp/upload-code.sh
#   scripts/gcp/create-vm.sh bocr-smoke e2-standard-2 30 scripts/vm/smoke-test-startup.sh
set -uo pipefail
OPS_BUCKET="bangla-ocr-dataset-ops"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOG=/var/log/bocr-smoke.log
exec > >(tee -a "$LOG") 2>&1

run() {
  set -euo pipefail
  export HOME=/root PATH="/root/.local/bin:$PATH"
  cd /root
  rm -rf bocr-scrape bocr-work
  gcloud storage cp "gs://$OPS_BUCKET/deploy/bocr-scrape-latest.tar.gz" code.tar.gz
  tar xzf code.tar.gz
  cd bocr-scrape
  ./scripts/vm/bootstrap.sh
  uv run --group dev pytest -q

  export DATA_URI="gs://$OPS_BUCKET/smoke-test/data-$STAMP" WORK_DIR=/root/bocr-work
  export PROGRESS_INTERVAL_SECONDS=30 CATALOG_FLUSH_DOCS=2
  query='language:Bengali AND mediatype:texts AND imagecount:[8 TO 40]'

  # Interrupt the first IA run after 60 s (graceful SIGINT), then resume it to completion.
  timeout -s INT 60 uv run bocr-scrape ingest ia --max-items 6 --query "$query" || true
  uv run bocr-scrape status ia
  uv run bocr-scrape ingest ia --max-items 2 --query "$query"
  uv run bocr-scrape ingest nctb --classes class-7 --max-items 1

  uv run bocr-scrape verify --source ia --check-objects -1
  uv run bocr-scrape verify --source nctb --check-objects -1
}

( run )
status=$?
echo "SMOKE_TEST_EXIT=$status"
gcloud storage cp "$LOG" "gs://$OPS_BUCKET/smoke-test/logs/smoke-$STAMP.log"
shutdown -h now
