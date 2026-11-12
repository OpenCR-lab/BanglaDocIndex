# Shared settings for the scripts in scripts/gcp. Override any of them in the environment.
PROJECT="${PROJECT:-bangla-ocr-dataset}"
REGION="${REGION:-us-west1}"
ZONE="${ZONE:-us-west1-b}"
DATA_BUCKET="${DATA_BUCKET:-bangla-ocr-dataset-v2}"
OPS_BUCKET="${OPS_BUCKET:-bangla-ocr-dataset-ops}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-ingest-worker@${PROJECT}.iam.gserviceaccount.com}"
GCLOUD="${GCLOUD:-gcloud}"
