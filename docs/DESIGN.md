# Bangla OCR dataset pipeline — design and handoff (v2)

> **Design record.** Written during the work and kept as it was, so that the reasoning behind each
> decision stays visible. Estimates and plans in it may differ from the final release; the released
> numbers are in the [README](../README.md) and the
> [dataset card](https://huggingface.co/datasets/OpenCR-lab/BanglaDocIndex).

**Status:** stage 1 (ingestion of IA, FinePDFs and NCTB), the GCS → Drive archive and the Colab
embedding job are implemented and piloted; the main run has not started · **Last updated:** 2026-10-22

## How to use this document

The single reference for the Bangla document-page dataset we are building for OCR research.

- **Co-researchers:** sections 1–4 for the what and why, then the stage you work on.
- **A coding agent or new machine:** read all of it. Section 3 lists rules that must not be
  broken. Section 17 lists open decisions: ask, don't decide silently. Operating steps are in
  [RUNBOOK.md](RUNBOOK.md).

Every decision is written the way it would be defended in a paper: what was chosen, and the
evidence or argument for it. Numbers marked *measured* come from the pilot (section 16);
everything else is an estimate.

---

## 1. Goal

A large, well-documented, reproducible dataset of Bangla document page images for training and
evaluating OCR models, suitable for publication.

1. **Collect** every page we can find of PDFs tagged as Bengali: Internet Archive (all Bengali
   language tags), all of FinePDFs `ben_Beng` (PDFs found by Common Crawl), and NCTB textbooks.
   About **16.4M pages** (11.7M IA, 4.4M FinePDFs, <0.1M NCTB).
2. **Describe** every page: script composition at ingest, image embeddings (EmbeddingGemma 2).
   Layout detection (PP-DocLayoutV3) and OCR (Indra) are deferred.
3. **Deduplicate, analyse, cluster**, then **select** subsets for annotation and training.
4. **Annotate** the selected pages; **build training datasets** from selections.

Stages run separately, on different machines, at different times. New sources and new
processing steps must be addable **without modifying anything already stored**.

---

## 2. History

**v0 (deleted 2026-10-20).** Two scripts (`bangla_ia_pipeline.py`, `nctb_scraper.py`) produced
1.85M loose JPEGs at 180 DPI in one bucket, with an 80% Bengali-character filter on IA OCR text.
Problems: source-specific names in shared paths, no central index, no home for later stages,
implicit decisions (splits, dedup, rights, resolution).

**v1 (2026-10-21, to be deleted after the v2 pilot).** This repository's first version: a
catalog, deterministic IDs, one source-agnostic engine. IA (`language:Bengali` only, 80% filter)
and NCTB ran: 1.8M pages at 200 DPI, 634 GB as loose objects in `gs://bangla-ocr-dataset`.
Lessons that shaped v2:

1. **The 80% filter was the wrong place to decide.** It silently dropped mixed-script and
   English-heavy Bengali items, and `language:Bengali` missed items tagged `ben`, `bn`,
   `Bangla` or `বাংলা`. v2 keeps everything tagged Bengali and **stores the measured script
   composition** instead; any threshold becomes a query, not a re-scrape.
2. **200 DPI with an upsampling floor wasted space.** Low-resolution scans were upscaled (e.g. a
   72-DPI phone photo rendered to 4,584 px wide). v2 renders at 150 DPI and never upsamples.
3. **Millions of loose objects** are slow and expensive to move (Drive cannot take them at all).
   v2 writes ~1 GB tar shards at ingest.
4. **GCS-only storage does not fit the budget** at 16M pages. v2 archives to the project
   owner's 5 TiB Google Drive, which costs nothing to transfer into from a GCP VM.

---

## 3. Design principles (non-negotiable)

1. **Stored bytes are never modified.** Shards, kept PDFs and catalog parts are immutable.
2. **The catalog is the single source of truth.** Jobs find pages by reading the catalog, never
   by listing storage or reading meaning from paths.
3. **Every processing step adds a new table keyed by `page_id`**, in its own versioned folder.
4. **Common fields are columns; source-specific fields are verbatim JSON.** A new source needs
   no schema change.
5. **Every run is recorded** (`core/runs/<run_id>.json`): commit, parameters, counters.
6. **Store measurements, not decisions** (script shares, not pass/fail; probabilities, not labels).
7. **Splits are per document and fixed at ingest.**
8. **Keys in the catalog are relative** (`blobs/pages/...`), so the same catalog works in GCS,
   on Drive or on a local disk.
9. **Everything heavy can be re-created from `core/` alone** (section 6.2), so losing a tier of
   cache is an inconvenience, not data loss.

---

## 4. Pipeline and where each stage runs

| # | Stage | Runs on | Reads | Writes |
|---|---|---|---|---|
| 1 | Ingest: discover, fetch, render, shard | GCP Spot VM (10 vCPU), watchdog-restarted | IA, Common Crawl, websites, NCTB | `core/catalog`, `blobs/pages`, `blobs/pdfs` |
| 1b | Archive | GCP Spot e2-small + rclone | GCS `blobs/`, `core/` | Drive (shards moved, core mirrored) |
| 2 | Embed | Colab (L4), daily batches | Drive `blobs/pages` | Drive `core/derived/embeddings` → copied to GCS |
| 3 | Deduplicate, splits check, rights | small VM or local PC, DuckDB | `core/` | `core/analysis/` |
| 4–5 | Cluster, select | local PC | `core/` (a few GB) | `core/analysis/clusters`, `core/selections` |
| 6 | Annotate | small VM → LLM API | selection + page bytes | `core/annotations` |
| 7 | Build training datasets | VM or Colab | selection, annotations, shards | `datasets/<version>/` |

Stages 1, 1b and 2 run **concurrently** during the main run: shards flow scraper → GCS →
Drive → Colab, and GCS holds only a rolling backlog.

---

## 5. Accounts and infrastructure

| Resource | Account | Use |
|---|---|---|
| GCP project `bangla-ocr-dataset`, $300 trial credit, 90 days | free-trial account | stages 1, 1b; later 3, 6, 7 |
| Google Drive 5 TiB, Colab Pro | Google One (5 TiB) account | archive of all bytes; stage 2 |

**Trial limits (checked):** 12 vCPUs across all regions, **no GPUs**. Hence the GPU work runs
on Colab, and the scraper is sized to 10 vCPUs so the uploader fits beside it.

**GCP → Drive transfer cost.** Google Cloud's network pricing treats egress from a VM to Google
services (such as the Drive API) differently from internet egress, and we expect it to be free.
This is the assumption the whole archive plan rests on, so it is **verified on the pilot's
bill** (the SKU for the pilot's ~10 GB transfer) before the main run. Drive accepts at most **750 GB of uploads per account per day**, so the uploader
runs at 8 MiB/s (~725 GB/day) and pauses for an hour if the cap is reached.

**Drive access** uses a private OAuth client with the `drive.file` scope: the pipeline sees only
files it created, never the rest of the owner's Drive. The shared rclone client is being retired
by Google, so it is not used. The app is in Testing mode, which expires tokens after 7 days
(renewal in RUNBOOK).

---

## 6. Storage layout

The same tree exists in GCS (`gs://bangla-ocr-dataset-v2/`) and on Drive (`bangla-ocr-dataset/`):

```
core/                                  small, precious: the source of truth (kept in GCS AND Drive)
  catalog/<table>/source=<src>/run=<run_id>/part-NNNNN.parquet     immutable parts
  sources/finepdfs/manifest-<bytes>.parquet                        text-free FinePDFs index
  recipes/<render_recipe>.json                                     exact render settings
  runs/<run_id>.json, runs/<run_id>/state.sqlite.gz                run records, checkpoints
  derived/embeddings/model=<m>/tokens=<n>/v=<k>/<shard>.parquet    one file per page shard
  analysis/ selections/ annotations/                               later stages
blobs/                                 large; moved from GCS to Drive as soon as sealed
  pages/source=<src>/run=<run_id>/shard-NNNNNN-<8hex>.tar          page JPEGs + per-page JSON
  pdfs/source=<src>/run=<run_id>/shard-NNNNNN-<8hex>.tar           kept PDFs (unstable sources)
datasets/<version>/                    built training sets (later)
```

### 6.1 Tiers

| Tier | Contents | Re-creatable? | Kept in |
|---|---|---|---|
| `core/` | catalog, manifests, recipes, run records, derived tables | no | GCS + Drive |
| `blobs/pdfs` | PDFs from sources that may change or vanish (live websites, NCTB) | no | Drive |
| `blobs/pages` | rendered page JPEGs | **yes**, from `core/` + sources | Drive |
| `datasets/` | training shards | yes, from selections | where training happens |

### 6.2 Shards and rehydration

Shards are plain POSIX tar files of about 1 GB, sealed only at document boundaries, so a
document's pages are never split across shards. Each page is two members, `<page_id>.jpg` and
`<page_id>.json`. The catalog records each JPEG's shard key, byte **offset and length**, so a
single page is one HTTP range read, without unpacking the shard.

**Rehydration** (`bocr-scrape rehydrate`): for any page, the catalog's fetch spec retrieves the
identical PDF again (an IA URL plus MD5; a Common Crawl WARC path, offset and length; or the kept
PDF), checks its SHA-256, and re-renders it with the stored recipe. Pilot: 600 of 600 sampled
pages reproduced byte-for-byte. This is why page shards are a cache: they could be deleted and
rebuilt, and a different resolution can be produced later as a new recipe.

---

## 7. Identifiers

| ID | Definition |
|---|---|
| `doc_id` | first 32 hex of `sha256(source \n source_item_id \n source_filename)` |
| `page_id` | `<doc_id>-p<page:06d>` |
| `run_id` | `<stage>-<source>-<UTC timestamp>` |
| `image_sha256`, `phash` | exact and near-duplicate keys (not identifiers) |

`source_item_id`: the IA identifier; the FinePDFs `id`; for NCTB
`<year>/<level>/<class>/<category>/<sha256(url)[:16]>`. All deterministic: the same input
gives the same ID on every run and machine. `SCHEMA_VERSION = 2`.

---

## 8. Catalog schema

Columns may be **added** (nullable, version bump), never renamed or removed. All tables have
`ingest_run_id` and `schema_version`.

**Script composition** (on `source_items` and `documents`): `script_bengali`, `script_latin`,
`script_arabic`, `script_devanagari`, `script_other` (shares of alphabetic characters),
`script_letters` (how many were counted) and `script_method` (how the text was obtained).

| Table | One row per | Key columns |
|---|---|---|
| `source_items` | examined source item, accepted or not | `status` (accepted / no_pdf / skipped / error), script fields, `raw_metadata` (verbatim JSON) |
| `documents` | PDF | identity, title/creator/year, script fields, **fetch spec** (`fetch_method` http / cc_warc / live, `warc_path`, `warc_offset`, `warc_length`, `source_md5`), `pdf_bytes/md5/sha256`, kept-PDF location (`kept_pdf_shard/offset/length`), `page_count`, `duplicate_of`, `license_url`, `rights_raw`, `source_metadata`, `split` |
| `pages` | page | `page_id`, `doc_id`, `page_number`, **location** (`shard_key`, `member`, `offset`, `length`), `width`, `height`, `dpi`, `native_dpi`, `render_recipe`, `image_sha256`, `phash`, `split` |
| `shards` | sealed shard | `shard_key`, `kind` (pages / pdfs), `bytes`, `sha256`, `md5`, `members`, `sealed_at` |

---

## 9. Stage 1: ingestion

### 9.1 Contract and crash safety

1. Discovery records every examined item in `source_items`, with its script composition.
2. Each document is fetched, its exact duplicates are skipped (section 9.5), and every page is
   rendered and appended to the open page shard; kept PDFs go to the open PDF shard.
3. A document is **staged** until its shard is sealed, uploaded and verified; only then are its
   catalog rows committed. Catalog parts are written once and never edited.
4. On restart (crash, Spot preemption, code update), documents staged in a shard that never
   uploaded are **redone**; everything committed stays. Tested with a hard stop mid-run.
5. SQLite is operational state only, checkpointed to `core/runs/<run_id>/state.sqlite.gz`.

### 9.2 Render recipe (decided)

`r2-150dpi-noupsample-8mp-q88-pdfium6462`: pypdfium2, **150 DPI**, never above the page's
native image resolution, at most 8 megapixels per page, JPEG quality 88 (optimised), RGB.

- **150 DPI** is a storage-driven choice, and its limitation should be reported as such. At
  150 DPI a 10-pt glyph is about 21 px tall (x-height about 10 px); Tesseract's documentation
  recommends at least 10 pt at 300 DPI, so small print (footnotes, dense newspapers) is
  under-resolved for classical OCR. The argument for it: storage scales with DPI², so 150 DPI is
  about 56% of 200 DPI and is what fits 16M pages in the Drive archive; the intended consumers
  are neural models that downscale their input (EmbeddingGemma sees at most 1,120 tokens); and
  any subset that needs more resolution is re-rendered from the PDFs as a new recipe (section
  6.2), never by overwriting.
- **Never upsample:** upscaling adds bytes, not information.
- **8 MP cap:** guards against posters and maps rendered to hundreds of megapixels.

### 9.3 Sources

**Internet Archive** (`fetch_method = http`, PDF not kept: IA is itself an archive and the
MD5 identifies the exact file).
- Query `language:(Bengali OR ben OR bn OR Bangla OR বাংলা)`: every spelling of the tag used
  on IA. No script threshold.
- Script composition from the item's `_djvu.txt`: three 256 KB windows (start, middle, end), so
  a Bengali book with an English preface is measured fairly. `script_method = ia_djvu_txt_3x256k`.
- Dark (withdrawn) items are recorded as `skipped`.

**FinePDFs `ben_Beng`** (539,857 documents, 4.44M pages). FinePDFs publishes text and pointers,
not PDFs, so it is used as an index:
- The parquet files are downloaded once (resumable), and a text-free **manifest** is written with
  every metadata column plus the script composition of the full extracted text
  (`script_method = finepdfs_text_full`). Rows are processed in a fixed hash order so requests to
  the same host are spread out.
- **Not truncated in Common Crawl** → one byte-range read of the WARC record (`cc_warc`).
  Common Crawl is permanent, so the PDF is not kept. Transport: Common Crawl's S3 bucket
  (`s3://commoncrawl`, which Common Crawl recommends for bulk access; the bucket owner pays
  for requests and transfer), read with temporary AWS credentials minted from the VM's
  Google identity, so no AWS key exists. Its HTTPS endpoint (`data.commoncrawl.org`)
  throttled the main run to ~0.75 records/s with constant 503s; S3 served 22 records/s with
  no errors, and 20 of 20 test records were byte-identical over both. HTTPS remains the
  fallback when no AWS role is configured.
- **Truncated** (19% of documents but 3.34M of the 4.44M pages: Common Crawl stores only the
  first ~1 MB) → fetched from the **original website** (`live`), and the PDF is kept, because
  websites change. Fail fast: 10 s connect / 60 s read timeouts, 2 attempts, at most 2
  connections per host and 1 request per second per host, and a host is abandoned after 5
  consecutive connection failures. **No Wayback Machine fallback** (decided: broken links cost
  time and yield little).
- 1,456 documents hosted on archive.org are skipped: the IA source covers them.

**NCTB** (`http`, PDF kept): 2026 secondary textbooks (classes 6–10, Madhhomik and Dakhil),
discovered via the third-party aggregator `eproshnobank.com`, downloaded from
`drive.egovcloud.gov.bd`.

### 9.4 Splits (decided)

`split` hashes `doc_id` into [0, 1): test below 0.05, val below 0.10, otherwise train (90/5/5).
All pages of a document share one split.

### 9.5 Duplicates at ingest

Only **exact** duplicate PDFs are skipped, and they are still recorded (`duplicate_of`): before
download by the source's MD5 (IA), after download by SHA-256. Everything subtler (re-scans,
other editions, near-identical pages) is left to stage 3, using the stored `image_sha256`,
`phash` and embeddings: a deduplication rule is a research decision that should be reported and
changeable, not baked into collection.

---

## 10. Stage 1b: archive to Drive

Described operationally in RUNBOOK §3. `rclone move` deletes a shard from GCS only after its MD5
matches on Drive; `core/` is copied both ways. The design goal is that GCS never holds more than
a few days of backlog, which keeps storage cost negligible.

---

## 11. Stage 2: embeddings (Colab)

- **Model:** `google/embeddinggemma-2` (Apache 2.0), image input, bf16 (it must not run in
  fp16), L2-normalised 768-dim vectors stored as float16 with `page_id`. Matryoshka: shorter
  vectors are prefixes, so only the full vector is stored.
- **Where:** Colab, because the GCP trial has no GPUs. The notebook reads page shards directly
  from Drive through the Drive API (no mount popup), writes one Parquet file per shard to
  `core/derived/embeddings/model=embeddinggemma-2/tokens=<n>/v=1/`, skips shards already done,
  and is re-run daily. The uploader copies results back into GCS.
- **Measured throughput on an L4** (640 pages, bf16): 70 tokens 39.8 pages/s, 140 tokens 21.6,
  280 tokens 10.8, 560 tokens 4.65. Time is >90% model; download and decode are negligible.
  This makes the **vision token budget** the main cost lever.
- **Decided: 70 tokens.** The embeddings are for feature extraction (clustering, selection), not
  reading text. Layout-level features are enough for that (as in OmniDocBench's ResNet-feature
  clustering), so the smallest budget is used: ~96 L4-hours (~165 Colab units) for ~13.8M
  pages. Pre-flight check: k-means (k=16) on 1,570 pilot pages from all sources gave visually
  clean page-type clusters (comics, covers/photos, tables, official letters, stamped documents,
  verse, near-blank scans, textbook pages, dense multi-column). Some clusters are dominated by a
  single book, so selection caps pages per document.

---

## 12–15. Later stages

- **Deduplication and analysis (3):** exact groups by `image_sha256` / `pdf_sha256`, near
  duplicates by `phash` and embedding similarity; flags only, nothing deleted. A duplicate group
  spanning train and test is a leak to resolve at selection time.
- **Clustering and selection (4–5):** k-means on truncated embeddings; selections are Parquet
  lists of `page_id` with reasons, caps per document and per cluster, recorded seeds.
- **Annotation (6):** per selection, resumable, every prompt or model version in a new run
  folder, a hand-labelled gold set from val/test for evaluation.
- **Training datasets (7):** shards built from a selection at training resolution, with a
  manifest and dataset card.
- **Rights (15):** raw rights metadata is kept at ingest (`license_url`, `rights_raw`,
  `raw_metadata`); status is a derived, versioned table (public domain / CC / government free
  distribution / in copyright / unknown). Release images only for clearly reusable subsets; for
  the rest, release IDs, URLs and fetch specs so others can rebuild the pages.

---

## 16. Pilot results and estimates

Pilot on 2026-10-22, one n2-custom-10 Spot VM, data under `gs://bangla-ocr-dataset-v2/pilot`:
IA 70 documents / 12,512 pages; FinePDFs 6,000 manifest rows → 5,184 documents / 32,012 pages;
NCTB 4 books / 401 pages. Verification: no problems; every row unique after a hard stop
mid-run; 2,296 pages re-read from Drive shards matched their catalog SHA-256 at the recorded
offsets.

| Measure (150 DPI) | IA colour scans | IA DLI scans (local pilot) | FinePDFs, CC copy | FinePDFs, live | NCTB |
|---|---|---|---|---|---|
| KB per page | 313 (median 322) | ~104 | 243 | 329 | 191 |
| Kept PDF, KB per page | — | — | — | 202 | 265 |

- **IA page size depends on the collection.** The `booksbylanguage_bengali` and similar
  collections are colour scans (median 1.2 MP, ~300 KB); Digital Library of India scans are
  grayscale and about a third of that. DLI is ~67% of the 55k Bengali-tagged IA text items.
- **Live fetching succeeds for about 30% of truncated documents** (340 of ~1,130). Failures are
  dominated by TLS errors, refused connections and timeouts on old or retired sites, each
  abandoned in seconds; one slow host held the run's tail for ~8 minutes. All failures are
  recorded, so the yield is reportable per host and year.
- Throughput: IA ~48 pages/s with 4 render workers; FinePDFs 20–31 pages/s with 6.

**Main-run estimate** (assumes 11.7M IA pages, 60% of them DLI; the live success rate also
applies to pages):

| Part | Pages | Size |
|---|---|---|
| IA | 11.7M | ~2.1 TB (1.8–2.6 depending on the DLI share) |
| FinePDFs, CC copy | ~1.1M | ~0.27 TB |
| FinePDFs, live (~30% of 3.34M) | ~1.0M | ~0.33 TB pages + ~0.2 TB kept PDFs |
| NCTB | <0.1M | <0.03 TB |
| **Total** | **~13.8M** | **~2.9 TB (2.6–3.5)**, against 4.1 TiB (4.5 TB) free on Drive |

At ~725 GB/day, the Drive upload cap is about 4 days of transfer, so it is not the bottleneck.

---

## 17. Open decisions (ask the project owner)

1. ~~Embedding token budget~~ — decided: 70 tokens (section 11).
2. Train/val/test ratios (90/5/5 implemented) and whether test should hold out whole sources.
3. Annotation schema and models; training resolution; page caps and cluster quotas.

---

## Glossary

- **Catalog:** the Parquet tables in `core/catalog/`, the source of truth.
- **Shard:** an immutable ~1 GB tar of page JPEGs (or kept PDFs) with byte offsets in the catalog.
- **Recipe:** the exact render settings, stored in `core/recipes/`.
- **Fetch spec:** catalog columns that retrieve the identical PDF again.
- **Rehydration:** rebuilding page images from the catalog, fetch specs and recipe.
- **Derived table:** output of a processing step, keyed by `page_id`, versioned, never edited.
- **Matryoshka embedding:** an embedding whose first N dimensions are a usable smaller embedding.
