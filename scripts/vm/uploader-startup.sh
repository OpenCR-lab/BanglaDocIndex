#!/usr/bin/env bash
# Startup script for the uploader VM (runs as root on every boot; safe to re-run).
#
# Moves sealed shards from GCS to Google Drive at a steady rate below Drive's 750 GB/day
# upload limit, and mirrors the small core/ tables. `rclone move` checks every file's MD5
# on Drive against GCS before deleting the GCS copy, so a shard leaves GCS only once it is
# verified on Drive. Transfer from a Google Cloud VM to Google Drive is not billed.
# Derived tables that Colab writes to Drive (core/derived, e.g. embeddings) are copied back
# into GCS, so core/ in GCS stays the complete source of truth.
#
# Memory: an e2-small has 2 GB and rclone holds one chunk per transfer in memory. 2 transfers
# x 64 MiB chunks fits; 4 x 256 MiB was OOM-killed in the pilot. Two transfers are enough
# because the bandwidth limit, not concurrency, sets the rate.
#
# Instance metadata (optional):
#   bocr-data-uri     dataset root in GCS (default gs://bangla-ocr-dataset-v2)
#   bocr-drive-root   folder on Drive (default bangla-ocr-dataset)
#   bocr-bwlimit      rclone bandwidth limit (default 8M = 8 MiB/s, about 725 GB/day)
#   bocr-drive-reserve-gb  free space to leave on Drive (default 50). Each pass moves at most
#                     (Drive free - reserve); shards that do not fit stay in GCS until space
#                     is freed. Drive rejects uploads over quota (storageQuotaExceeded), but a
#                     transfer already in flight may finish, so the cap keeps us well clear.
set -euo pipefail
META=http://metadata.google.internal/computeMetadata/v1/instance/attributes
meta() { curl -sf -H 'Metadata-Flavor: Google' "$META/$1" || true; }

data_uri="$(meta bocr-data-uri)"; data_uri="${data_uri:-gs://bangla-ocr-dataset-v2}"
drive_root="$(meta bocr-drive-root)"; drive_root="${drive_root:-bangla-ocr-dataset}"
bwlimit="$(meta bocr-bwlimit)"; bwlimit="${bwlimit:-8M}"
reserve_gb="$(meta bocr-drive-reserve-gb)"; reserve_gb="${reserve_gb:-50}"
gcs_path="gcs:${data_uri#gs://}"

if ! command -v rclone >/dev/null 2>&1; then
  apt-get update -qq
  apt-get install -y -qq --no-install-recommends ca-certificates curl unzip >/dev/null
  curl -sSL https://rclone.org/install.sh | bash >/dev/null
fi
install -d -m 700 /etc/rclone
gcloud secrets versions access latest --secret=rclone-conf > /etc/rclone/rclone.conf
chmod 600 /etc/rclone/rclone.conf

cat > /usr/local/bin/bocr-uploader <<EOF
#!/usr/bin/env bash
# One pass: move sealed shards (verified, then deleted from GCS) as far as Drive's free space
# minus the reserve allows, bring Colab's derived tables into GCS, mirror core tables to Drive.
set -uo pipefail
CONF=/etc/rclone/rclone.conf
free=\$(rclone --config \$CONF about drive: --json 2>/dev/null \\
  | python3 -c 'import json,sys; print(int(json.load(sys.stdin).get("free", -1)))' 2>/dev/null)
free=\${free:--1}
budget_mib=\$(( (free - $reserve_gb * 1024 * 1024 * 1024) / 1048576 ))
status=0
if [ "\$free" -lt 0 ]; then
  echo "Drive free space unknown; not moving shards this pass"
elif [ "\$budget_mib" -lt 2048 ]; then
  echo "Drive reserve reached (free \$(( free / 1073741824 )) GiB, reserve $reserve_gb GiB); shards stay in GCS"
else
  echo "Drive free \$(( free / 1073741824 )) GiB; moving at most \$budget_mib MiB this pass"
  # --cutoff-mode soft: once the budget is used, no new transfer starts (in-flight ones finish).
  rclone --config \$CONF move "$gcs_path/blobs" "drive:$drive_root/blobs" \\
    --min-age 2m --bwlimit $bwlimit --transfers 2 --checkers 8 \\
    --drive-chunk-size 64M --drive-stop-on-upload-limit \\
    --max-transfer \${budget_mib}M --cutoff-mode soft \\
    --stats 10m --stats-one-line --log-level INFO
  status=\$?
  [ \$status -eq 8 ] && status=0  # 8 = --max-transfer reached: expected, not a failure
fi
rclone --config \$CONF copy "drive:$drive_root/core/derived" "$gcs_path/core/derived" \\
  --transfers 4 --log-level INFO
rclone --config \$CONF copy "$gcs_path/core" "drive:$drive_root/core" \\
  --transfers 8 --log-level INFO
exit \$status
EOF
chmod +x /usr/local/bin/bocr-uploader

cat > /etc/systemd/system/bocr-uploader.service <<'EOF'
[Unit]
Description=Move bocr shards from GCS to Google Drive
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/bin/bash -c 'while true; do /usr/local/bin/bocr-uploader || sleep 3600; sleep 300; done'
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable bocr-uploader
systemctl restart bocr-uploader
echo "bocr-uploader started: $gcs_path -> drive:$drive_root (bwlimit $bwlimit, Drive reserve ${reserve_gb} GiB)"
