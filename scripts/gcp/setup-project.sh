#!/usr/bin/env bash
# One-time GCP setup (already done for bangla-ocr-dataset on 2026-10-22; kept as a record
# and for recreating the environment). Safe to re-run: existing resources are left alone.
#
# Usage: BILLING_ACCOUNT=XXXXXX-XXXXXX-XXXXXX scripts/gcp/setup-project.sh
set -euo pipefail
source "$(dirname "$0")/config.sh"
: "${BILLING_ACCOUNT:?set BILLING_ACCOUNT}"

$GCLOUD projects describe "$PROJECT" >/dev/null 2>&1 \
  || $GCLOUD projects create "$PROJECT" --name="Bangla OCR Dataset"
$GCLOUD config set project "$PROJECT"
$GCLOUD billing projects link "$PROJECT" --billing-account="$BILLING_ACCOUNT"
$GCLOUD services enable compute.googleapis.com storage.googleapis.com iam.googleapis.com \
  billingbudgets.googleapis.com

for bucket in "$DATA_BUCKET" "$OPS_BUCKET"; do
  $GCLOUD storage buckets describe "gs://$bucket" >/dev/null 2>&1 \
    || $GCLOUD storage buckets create "gs://$bucket" --location="$REGION" \
         --default-storage-class=STANDARD --uniform-bucket-level-access \
         --public-access-prevention
done

$GCLOUD iam service-accounts describe "$SERVICE_ACCOUNT" >/dev/null 2>&1 \
  || $GCLOUD iam service-accounts create "${SERVICE_ACCOUNT%%@*}" \
       --display-name="Ingestion worker VMs"
# The worker can read and write objects in these two buckets and nothing else.
for bucket in "$DATA_BUCKET" "$OPS_BUCKET"; do
  $GCLOUD storage buckets add-iam-policy-binding "gs://$bucket" \
    --member="serviceAccount:$SERVICE_ACCOUNT" --role=roles/storage.objectAdmin >/dev/null
done

$GCLOUD billing budgets list --billing-account="$BILLING_ACCOUNT" \
  --format="value(displayName)" | grep -qx "bangla-ocr-dataset budget" \
  || $GCLOUD billing budgets create --billing-account="$BILLING_ACCOUNT" \
       --display-name="bangla-ocr-dataset budget" --budget-amount=250USD \
       --threshold-rule=percent=0.25 --threshold-rule=percent=0.5 \
       --threshold-rule=percent=0.75 --threshold-rule=percent=0.9 \
       --threshold-rule=percent=1.0
echo "Project $PROJECT ready: gs://$DATA_BUCKET (dataset), gs://$OPS_BUCKET (code, tests)."
