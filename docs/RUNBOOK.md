# Runbook: the v2 ingestion pipeline

How to run the full scrape, archive it to Google Drive and embed it on Colab. Read
[DESIGN.md](DESIGN.md) first for the reasoning behind each choice.

```
 scraper VM (Spot)            GCS (bangla-ocr-dataset-v2)          uploader VM (Spot)        Google Drive (Pro, 5 TiB)        Colab (L4)
 ia, finepdfs, nctb  ──────▶  core/      catalog, runs, recipes  ──copy──────────────────▶  core/                 ◀── reads blobs/pages
 render 150 DPI, shard        blobs/pages  ~1 GB tars           ──move (verify, delete)──▶  blobs/pages             writes core/derived
                              blobs/pdfs   kept PDFs            ──move (verify, delete)──▶  blobs/pdfs
                              core/derived ◀────────────────────── copy back ───────────────  core/derived/embeddings
 Cloud Scheduler watchdog: POST instances.start every 5 min for each Spot VM
```

## What already exists

| Resource | Value |
|---|---|
| GCP project | `bangla-ocr-dataset` (free-trial account) |
| Region / zone | `us-west1` / `us-west1-c` (`us-west1-b` ran out of capacity in the pilot) |
| Dataset bucket | `gs://bangla-ocr-dataset-v2` (Standard, soft delete off, public access prevented) |
| Ops bucket | `gs://bangla-ocr-dataset-ops` (code tarballs under `deploy/`, `secrets/`) |
| VM service account | `ingest-worker@…`: object admin on both buckets, reads the `rclone-conf` secret |
| Watchdog service account | `vm-watchdog@…`: `compute.instanceAdmin.v1`, used only by Cloud Scheduler |
| Secret | `rclone-conf` (Secret Manager): rclone config with `gcs` (env auth) and `drive` remotes |
| Drive OAuth client | Desktop client `rclone-uploader` in the Pro account's project `bocr-uploader`, scope `drive.file` |
| Drive | Google One account: 5 TiB, folder `bangla-ocr-dataset/` |

Quota: 12 vCPUs in all regions, no GPUs. The uploader (e2-small, 2 vCPUs) and the scraper
(10 vCPUs) together use the whole quota.

### The Drive token expires every 7 days

The OAuth app is in **Testing** mode (publishing needs a verified homepage and privacy policy),
so Google expires its refresh token after 7 days. Before the main run, and then weekly:

```bash
rclone config reconnect drive: --config ~/tools/rclone.conf      # sign in as the Pro account
gcloud secrets versions add rclone-conf --data-file ~/tools/rclone.conf
# on the uploader VM: re-run the startup script so it pulls the new secret
gcloud compute ssh bocr-uploader --zone us-west1-c --command "sudo google_metadata_script_runner startup"
```

Also refresh `gs://bangla-ocr-dataset-ops/secrets/rclone-colab.conf` (the `drive` section only)
for Colab. A run that outlives the token simply stops moving shards (they wait safely in GCS)
until the token is renewed.

## 1. Upload the code

```bash
scripts/gcp/upload-code.sh
```

Puts `git HEAD` at `gs://bangla-ocr-dataset-ops/deploy/bocr-scrape-latest.tar.gz` (VMs never
need GitHub credentials) and the commit is recorded in every run record.

## 2. Scraper VM (Spot) and its watchdog

```bash
SPOT=1 ZONE=us-west1-c \
METADATA="bocr-sources=nctb ia finepdfs,bocr-env-ia=DOWNLOAD_WORKERS=6\nRENDER_WORKERS=4\nDISCOVERY_WORKERS=8,bocr-env-finepdfs=DOWNLOAD_WORKERS=24\nRENDER_WORKERS=6" \
  scripts/gcp/create-vm.sh bocr-ingest n2-custom-10-20480 200 scripts/vm/ingest-startup.sh
scripts/gcp/watchdog.sh create bocr-ingest us-west1-c
```

On Windows, separate metadata entries with commas (`^;^` breaks there). The startup script
installs the code and runs one systemd service per source (`bocr-ingest@<source>`); they
restart after crashes and reboots. Leave out `bocr-data-uri` for the real dataset root; the
pilot used `gs://bangla-ocr-dataset-v2/pilot`.

**How a preemption is handled.** Spot VMs stop (disk kept) when preempted. Within 5 minutes
the watchdog's `start` call boots the VM, the startup script starts the services, and each
service resumes from its local SQLite state: documents staged in a shard that never finished
uploading are redone, everything committed stays committed. Tested in the pilot: stopped
mid-run at 16:44, running again at 16:45, 188 staged documents redone, no duplicates.

**Updating code mid-run:** commit, `scripts/gcp/upload-code.sh`, then on the VM
`sudo gcloud storage cp gs://bangla-ocr-dataset-ops/deploy/update-code.sh /tmp/u.sh && sudo bash /tmp/u.sh`.

## 3. Uploader VM (GCS → Drive)

```bash
SPOT=1 ZONE=us-west1-c METADATA="bocr-drive-root=bangla-ocr-dataset" \
  scripts/gcp/create-vm.sh bocr-uploader e2-small 20 scripts/vm/uploader-startup.sh
scripts/gcp/watchdog.sh create bocr-uploader us-west1-c
```

Every 5 minutes it runs one pass of `/usr/local/bin/bocr-uploader`:

1. `rclone move` of `blobs/` to Drive at `--bwlimit 8M` (about 725 GB/day, under Drive's
   750 GB/day upload cap); each shard is deleted from GCS only after its MD5 matches on Drive.
2. `rclone copy` of Drive `core/derived/` back into GCS (embeddings written by Colab).
3. `rclone copy` of GCS `core/` to Drive.

If Drive's daily cap is hit, `--drive-stop-on-upload-limit` ends the pass and the loop waits an
hour. Logs: `sudo journalctl -fu bocr-uploader`.

## 4. Embeddings on Colab

Notebook: [notebooks/embed_pages.ipynb](../notebooks/embed_pages.ipynb) (source:
`notebooks/embed_pages.py`). Open it in Colab as the Pro account and choose an **L4** runtime.

The notebook needs the Colab rclone config (`gs://bangla-ocr-dataset-ops/secrets/rclone-colab.conf`).
Default: leave `CONF_URL = ""` and store the config once as a **Colab secret** named
`RCLONE_COLAB_CONF` (key icon in the left bar), whose value is the file base64-encoded on one
line (`base64 -w0 rclone-colab.conf`), with "Notebook access" switched on. Secrets have no
expiry; update the secret whenever the Drive token is renewed. Fallback: set `CONF_URL` to a
signed URL (12 h is the maximum with the impersonated service account):

```bash
gcloud storage sign-url gs://bangla-ocr-dataset-ops/secrets/rclone-colab.conf \
  --duration=12h --region=us-west1 --impersonate-service-account=ingest-worker@bangla-ocr-dataset.iam.gserviceaccount.com
```

Then Run all. It needs no Drive mount or sign-in popup: it uses the Drive API with the same
`drive.file` token. It skips shards that already have an output, so re-run it daily as new
shards arrive; it stops starting new shards after `MAX_HOURS`. **Disconnect and delete the
runtime when it finishes** (Runtime menu), or units keep draining.

Measured on an L4 (pilot, 640 FinePDFs pages, bf16): 70 tokens 39.8 pages/s, 140 tokens 21.6,
280 tokens 10.8, 560 tokens 4.65. The model is >90% of the time; Drive download and JPEG decode
are negligible. See DESIGN.md for what this means for the token budget.

## 5. Monitor

```bash
gcloud compute ssh bocr-ingest --zone us-west1-c --command "sudo tail -n 3 /var/lib/bocr/bocr-work/ia/logs/ingest.log"
gcloud storage du -s gs://bangla-ocr-dataset-v2/blobs          # backlog waiting for Drive
rclone size drive:bangla-ocr-dataset/blobs --config ~/tools/rclone.conf
```

`core/runs/<run_id>.json` (GCS and Drive) holds each run's counters, settings and commit. A
hard stop can leave NUL bytes at the end of `ingest.log`; use `grep -a`.

## 6. Finish

```bash
uv run bocr-scrape verify --source ia --run <run_id> --check-pages 2000
uv run bocr-scrape rehydrate ia --pages 200 --out /tmp/rehydrate   # re-creates pages from the catalog
```

Then delete the VMs **and their watchdogs** (a watchdog would otherwise keep trying to start a
deleted VM):

```bash
scripts/gcp/watchdog.sh delete bocr-ingest
scripts/gcp/watchdog.sh delete bocr-uploader
gcloud compute instances delete bocr-ingest bocr-uploader --zone us-west1-c
```

## Inspecting the catalog

```sql
SELECT source, count(*) docs, sum(page_count) pages
FROM read_parquet('core/catalog/documents/**/*.parquet', hive_partitioning = true)
GROUP BY source;
```

## Measured sizes and rates (pilot, 150 DPI)

| Source | KB/page | Pages/s on the pilot VM | Notes |
|---|---|---|---|
| IA colour scans | ~313 | ~48 (4 render workers) | DLI grayscale scans ~104 |
| FinePDFs, Common Crawl copy | ~243 | 20–31 (6 render workers, both kinds) | |
| FinePDFs, live website | ~329 | | PDF kept in `blobs/pdfs` (+202 KB/page); ~30% of live fetches succeed |
| NCTB | ~191 | | PDF kept (+265 KB/page) |

Main-run estimate: ~13.8M pages, ~2.9 TB (2.6–3.5) on Drive. See DESIGN.md §16.
