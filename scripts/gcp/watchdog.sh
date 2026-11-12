#!/usr/bin/env bash
# Create (or delete) a Cloud Scheduler job that starts a VM every 5 minutes.
#
# Spot VMs are created with --instance-termination-action=STOP, so a preemption leaves the
# VM stopped with its disk intact. Calling the Compute Engine `start` API on a running VM
# does nothing; on a stopped VM it boots it, and the startup script resumes the work.
# This runs entirely in Google Cloud, so it works while every local machine is off.
#
# Usage: scripts/gcp/watchdog.sh create NAME [ZONE]
#        scripts/gcp/watchdog.sh delete NAME
set -euo pipefail
source "$(dirname "$0")/config.sh"
action="${1:?create|delete}"; name="${2:?vm name}"; zone="${3:-$ZONE}"
job="watchdog-$name"
WATCHDOG_SA="vm-watchdog@${PROJECT}.iam.gserviceaccount.com"

if [[ "$action" == "delete" ]]; then
  $GCLOUD scheduler jobs delete "$job" --location="$REGION" --quiet
  exit 0
fi

if ! $GCLOUD iam service-accounts describe "$WATCHDOG_SA" >/dev/null 2>&1; then
  $GCLOUD iam service-accounts create vm-watchdog --display-name="Restarts preempted VMs"
  $GCLOUD projects add-iam-policy-binding "$PROJECT" --member="serviceAccount:$WATCHDOG_SA" \
    --role=roles/compute.instanceAdmin.v1 --condition=None >/dev/null
fi
$GCLOUD scheduler jobs create http "$job" --location="$REGION" \
  --schedule="*/5 * * * *" --http-method=POST \
  --uri="https://compute.googleapis.com/compute/v1/projects/$PROJECT/zones/$zone/instances/$name/start" \
  --oauth-service-account-email="$WATCHDOG_SA" \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
  --attempt-deadline=60s
echo "Watchdog $job: starts $name in $zone every 5 minutes"
