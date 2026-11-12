#!/usr/bin/env bash
# Startup script for the scraper VM (runs as root on every boot; safe to re-run).
#
# First boot: installs the latest uploaded code to /opt/bocr-scrape, creates the `bocr`
# service user and one systemd service per source. Every boot: (re)starts the services,
# which resume from the local state in /var/lib/bocr/bocr-work. After a Spot preemption the
# watchdog starts the VM again and this script brings the services back.
#
# Instance metadata (all optional):
#   bocr-sources     sources to run, space separated (default: "ia")
#   bocr-data-uri    dataset root (default: gs://bangla-ocr-dataset-v2)
#   bocr-env-<src>   extra environment lines for that source's service, e.g.
#                    "DOWNLOAD_WORKERS=16\nRENDER_WORKERS=4"
#   bocr-args-<src>  extra command-line arguments, e.g. "--max-items 50"
set -euo pipefail
OPS_BUCKET="bangla-ocr-dataset-ops"
APP=/opt/bocr-scrape
HOME_DIR=/var/lib/bocr
META=http://metadata.google.internal/computeMetadata/v1/instance/attributes

meta() { curl -sf -H 'Metadata-Flavor: Google' "$META/$1" || true; }

sources="$(meta bocr-sources)"; sources="${sources:-ia}"
data_uri="$(meta bocr-data-uri)"; data_uri="${data_uri:-gs://bangla-ocr-dataset-v2}"

if ! id bocr >/dev/null 2>&1; then
  useradd --system --create-home --home-dir "$HOME_DIR" --shell /bin/bash bocr
fi
if [[ ! -d "$APP" ]]; then
  apt-get update -qq
  apt-get install -y -qq --no-install-recommends ca-certificates curl tmux >/dev/null
  tmp="$(mktemp -d)"
  gcloud storage cp "gs://$OPS_BUCKET/deploy/bocr-scrape-latest.tar.gz" "$tmp/code.tar.gz"
  tar xzf "$tmp/code.tar.gz" -C /opt
  chown -R bocr:bocr "$APP"
  sudo -u bocr -H bash -c "cd $APP && ./scripts/vm/bootstrap.sh"
fi

mkdir -p /etc/bocr
for source in $sources; do
  {
    echo "DATA_URI=$data_uri"
    echo "WORK_DIR=$HOME_DIR/bocr-work"
    echo "DISK_HIGH_WATER_GB=60"
    echo "MIN_FREE_DISK_GB=15"
    printf '%b\n' "$(meta "bocr-env-$source")"
    echo "BOCR_ARGS=$(meta "bocr-args-$source")"
  } > "/etc/bocr/$source.env"
done

cat > /etc/systemd/system/bocr-ingest@.service <<EOF
[Unit]
Description=bocr-scrape ingestion (%i)
After=network-online.target
Wants=network-online.target

[Service]
User=bocr
WorkingDirectory=$APP
EnvironmentFile=/etc/bocr/%i.env
Environment=PATH=$HOME_DIR/.local/bin:/usr/local/bin:/usr/bin:/bin
ExecStart=/bin/bash -c 'exec $HOME_DIR/.local/bin/uv run --frozen --no-dev bocr-scrape --env none ingest %i \$\$BOCR_ARGS'
KillSignal=SIGINT
TimeoutStopSec=60
Restart=on-failure
RestartSec=120

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload

for source in $sources; do
  if [[ -f "$HOME_DIR/bocr-work/$source/COMPLETED" ]]; then
    echo "bocr-ingest@$source already completed; not starting"
    continue
  fi
  systemctl enable "bocr-ingest@$source"
  systemctl start "bocr-ingest@$source"  # no-op if already running
  echo "started bocr-ingest@$source"
done
