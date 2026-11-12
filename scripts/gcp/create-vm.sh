#!/usr/bin/env bash
# Create a VM (Ubuntu 24.04) that runs as the ingest-worker service account.
#
# Usage: scripts/gcp/create-vm.sh NAME [MACHINE_TYPE] [DISK_GB] [STARTUP_SCRIPT]
#   SPOT=1      Spot provisioning; on preemption the VM is STOPPED (disk kept), and the
#               watchdog (scripts/gcp/watchdog.sh) starts it again
#   METADATA    extra instance metadata, e.g. METADATA="^;^bocr-sources=ia finepdfs"
#               (the ^;^ prefix makes ';' the separator so values may contain commas)
#   ZONE        zone (default from config.sh)
# Delete it when finished:  gcloud compute instances delete NAME --zone <zone>
set -euo pipefail
source "$(dirname "$0")/config.sh"

name="${1:?usage: create-vm.sh NAME [MACHINE_TYPE] [DISK_GB] [STARTUP_SCRIPT]}"
machine="${2:-e2-standard-4}"
disk_gb="${3:-100}"
startup="${4:-}"

extra=()
if [[ -n "$startup" ]]; then
  extra+=(--metadata-from-file="startup-script=$startup")
fi
if [[ -n "${METADATA:-}" ]]; then
  extra+=(--metadata="$METADATA")
fi
if [[ "${SPOT:-0}" == "1" ]]; then
  extra+=(--provisioning-model=SPOT --instance-termination-action=STOP)
fi

$GCLOUD compute instances create "$name" \
  --project="$PROJECT" --zone="$ZONE" --machine-type="$machine" \
  --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
  --boot-disk-size="${disk_gb}GB" --boot-disk-type=pd-balanced \
  --service-account="$SERVICE_ACCOUNT" --scopes=cloud-platform \
  --labels=purpose=ingestion "${extra[@]}"
echo "Created $name ($machine, ${disk_gb}GB, spot=${SPOT:-0}) in $ZONE"
