#!/usr/bin/env bash
# Package the committed code (git HEAD) and upload it to the ops bucket, so VMs can fetch it
# without GitHub credentials. Uncommitted changes are NOT included.
#
# Usage: scripts/gcp/upload-code.sh
set -euo pipefail
source "$(dirname "$0")/config.sh"
cd "$(git rev-parse --show-toplevel)"

commit="$(git rev-parse --short=12 HEAD)"
if ! git diff --quiet HEAD; then
  echo "warning: uncommitted changes are not included in the upload" >&2
fi
archive="$(mktemp -d)/bocr-scrape-${commit}.tar.gz"
git archive --format=tar.gz --prefix=bocr-scrape/ -o "$archive" HEAD
$GCLOUD storage cp "$archive" "gs://$OPS_BUCKET/deploy/bocr-scrape-${commit}.tar.gz"
$GCLOUD storage cp "$archive" "gs://$OPS_BUCKET/deploy/bocr-scrape-latest.tar.gz"
echo "$commit" | $GCLOUD storage cp - "gs://$OPS_BUCKET/deploy/LATEST_COMMIT"
$GCLOUD storage cp "$(pwd)/scripts/vm/update-code.sh" "gs://$OPS_BUCKET/deploy/update-code.sh"
echo "Uploaded commit $commit"
