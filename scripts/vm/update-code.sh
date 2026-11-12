#!/usr/bin/env bash
# Update the code on a production VM without losing progress (run as root on the VM):
# gracefully stop the running ingest services, install the latest uploaded code, restart them.
#   sudo gcloud storage cp gs://bangla-ocr-dataset-ops/deploy/update-code.sh /tmp/update-code.sh
#   sudo bash /tmp/update-code.sh
set -euo pipefail
OPS_BUCKET="bangla-ocr-dataset-ops"
APP=/opt/bocr-scrape

units="$(systemctl list-units --type=service --state=active --plain --no-legend 'bocr-ingest@*' \
  | awk '{print $1}')"
for unit in $units; do
  echo "stopping $unit (finishes the current batch first)"
  systemctl stop "$unit"
done

tmp="$(mktemp -d)"
gcloud storage cp "gs://$OPS_BUCKET/deploy/bocr-scrape-latest.tar.gz" "$tmp/code.tar.gz"
mkdir "$tmp/new"
tar xzf "$tmp/code.tar.gz" -C "$tmp/new" --strip-components=1
cp "$APP/.env" "$tmp/new/.env"
mv "$APP" "$APP.previous-$(date -u +%Y%m%dT%H%M%SZ)"
mv "$tmp/new" "$APP"
chown -R bocr:bocr "$APP"
sudo -u bocr -H bash -c "cd $APP && ./scripts/vm/bootstrap.sh" >/dev/null

for unit in $units; do
  systemctl start "$unit"
  echo "restarted $unit"
done
