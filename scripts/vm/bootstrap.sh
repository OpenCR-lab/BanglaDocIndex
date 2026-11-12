#!/usr/bin/env bash
# Prepare an Ubuntu VM to run bocr-scrape. Run from the unpacked repository directory.
# Safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/../.."

if ! command -v curl >/dev/null 2>&1 || ! command -v tmux >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq --no-install-recommends ca-certificates curl tmux >/dev/null
fi

if ! command -v uv >/dev/null 2>&1; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH="$HOME/.local/bin:$PATH"

uv sync --frozen --no-dev --python 3.12

if [[ ! -f .env ]]; then
  cp .env.example .env
  # VM defaults: 100 GB boot disk, so keep the work directory well under it.
  sed -i 's/^DISK_HIGH_WATER_GB=.*/DISK_HIGH_WATER_GB=60/; s/^MIN_FREE_DISK_GB=.*/MIN_FREE_DISK_GB=15/' .env
  echo "Created .env from .env.example (DATA_URI=$(grep ^DATA_URI .env | cut -d= -f2))"
fi
echo "Ready. Start a run inside tmux, e.g.:"
echo "  tmux new -s ingest"
echo "  uv run bocr-scrape ingest ia"
