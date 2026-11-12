# BanglaDocIndex

**An index of 19.9 million Bangla document pages: catalog, source links and page embeddings.**

[![Dataset on Hugging Face](https://img.shields.io/badge/%F0%9F%A4%97%20Dataset-OpenCR--lab%2FBanglaDocIndex-yellow)](https://huggingface.co/datasets/OpenCR-lab/BanglaDocIndex)
[![Code licence: Apache-2.0](https://img.shields.io/badge/code-Apache--2.0-blue)](LICENSE)
[![Data licence: ODC-By 1.0](https://img.shields.io/badge/data-ODC--By%201.0-green)](https://opendatacommons.org/licenses/by/1-0/)

This repository holds the code that built **BanglaDocIndex**: the pipeline that found Bangla
PDFs on the Internet Archive, in FinePDFs (Common Crawl) and on the NCTB textbook site, rendered
every page, recorded where each page comes from, and embedded every page image with
EmbeddingGemma 2. The index itself is on the Hugging Face Hub:
**[OpenCR-lab/BanglaDocIndex](https://huggingface.co/datasets/OpenCR-lab/BanglaDocIndex)**.

The index publishes **no images**. Every page points back to its source document, and
[`examples/fetch_pages.py`](examples/fetch_pages.py) rebuilds the exact page image from that
source. BanglaDocIndex is the pool from which the
[BanglaDocBench](https://github.com/OpenCR-lab/BanglaDocBench) benchmark pages are selected.

## At a glance

| | |
|---|---|
| Pages | **19,873,418** |
| Documents | **525,618** (plus 14,811 exact-duplicate PDFs, recorded but not rendered) |
| Sources | Internet Archive 16.70M pages · FinePDFs 3.16M (Common Crawl copy 1.14M, live website 2.02M) · NCTB textbooks 16.7k |
| Embeddings | `google/embeddinggemma-2`, 768-d, unit-normalised, float16, 70 image tokens per page |
| Rendering | 150 DPI, never above a scan's own resolution, at most 8 MP, JPEG quality 88 |
| Splits | train / val / test = 90 / 5 / 5 %, assigned per document from a hash |
| Size on the Hub | about 23 GB (catalog 1.5 GB, embeddings 21.3 GB) |

## Using the index

```python
from datasets import load_dataset

docs = load_dataset("OpenCR-lab/BanglaDocIndex", "documents", split="train")   # 187 MB
pages = load_dataset("OpenCR-lab/BanglaDocIndex", "pages", split="train", streaming=True)
emb = load_dataset("OpenCR-lab/BanglaDocIndex", "embeddings", split="train", streaming=True)
```

SQL over the Hub without downloading everything (DuckDB reads only the columns it needs):

```python
import duckdb
duckdb.sql("""
    SELECT source, count(*) AS pages
    FROM 'hf://datasets/OpenCR-lab/BanglaDocIndex/core/catalog/pages/**/*.parquet'
    GROUP BY source
""").show()
```

The [dataset card](https://huggingface.co/datasets/OpenCR-lab/BanglaDocIndex) documents every table
and column, the licence of each source, and how to download part of the index.

## Rebuilding page images

```bash
git clone https://github.com/OpenCR-lab/BanglaDocIndex && cd BanglaDocIndex
uv sync
uv run python examples/fetch_pages.py f0747b20d08a09027bbf42ea8f0dcc35-p000001 --out pages/
```

For each page the script downloads the source PDF (Internet Archive, the Common Crawl WARC
record, or the original website), checks it against the recorded SHA-256, and renders the page
with the recorded recipe. With a local copy of the catalog (`--catalog`) it also compares every
rebuilt image with the catalog: byte-identical (`image_sha256`) or visually identical (`phash`
distance <= 2; JPEG bytes can differ between Pillow / libjpeg-turbo builds). Pages of `live`
documents disappear when websites change; such documents are skipped.

## How the index was built

```
discover (per source)  ->  download PDF  ->  render pages (pypdfium2)  ->  ~1 GB tar shards
      -> catalog rows (Parquet, written only after a shard is safely stored)
      -> page embeddings on a Colab L4 GPU (EmbeddingGemma 2, bfloat16)
```

1. **Discovery.** Internet Archive: every item tagged Bengali, kept when at least 80% of its
   letters are Bengali script. FinePDFs: every `ben_Beng` document, fetched from its Common
   Crawl WARC record when the crawl stored the whole PDF, otherwise from the live website.
   NCTB: the 2026 secondary textbooks (classes 6 to 10).
2. **Rendering.** One JPEG per page at 150 DPI, never upsampled above a scan's native
   resolution, capped at 8 MP. The exact recipe (pdfium build, Pillow and libjpeg-turbo versions)
   is stored in `core/recipes/`.
3. **Catalog.** Four Parquet tables: `source_items` (everything discovered, with the source's
   own metadata), `documents`, `pages` and `shards`. IDs are deterministic: `doc_id` is a hash
   of source, item and file name; `page_id` is `<doc_id>-pNNNNNN`.
4. **Embeddings.** Every page image, embedded with `google/embeddinggemma-2` at a budget of 70
   image tokens (bfloat16 on an NVIDIA L4), normalised and stored as float16.

Ingestion ran on Google Cloud Spot VMs (2026-10-23 to 2026-10-28), resuming from a local SQLite
state after every preemption; embedding finished on 2026-11-12.
[docs/DESIGN.md](docs/DESIGN.md) is the design record written during the work, with the reason
for each decision; [docs/RUNBOOK.md](docs/RUNBOOK.md) has the operating steps.

## Repository layout

```
src/bocr_scrape/          the pipeline (package name kept from development)
  sources/                source plug-ins: internet_archive.py, finepdfs.py, nctb.py
  engine.py               download, render, shard, commit, resume
  fetch.py                Internet Archive / Common Crawl / live-website downloads
  render.py               PDF -> JPEG rendering, native-DPI detection, perceptual hash
  catalog.py              Parquet schemas of the catalog tables
  rehydrate.py            rebuild a sample of pages and compare them with the catalog
notebooks/embed_pages.*   the Colab embedding job (.py source and .ipynb)
examples/fetch_pages.py   rebuild page images from their sources
scripts/gcp, scripts/vm   cloud set-up and VM start-up scripts
docs/                     DESIGN.md (design record), RUNBOOK.md (operations)
tests/                    unit and end-to-end tests
```

## Development

Requires [uv](https://docs.astral.sh/uv/) and Python 3.11 to 3.13.

```bash
uv sync
uv run pytest -m "not network"
```

A small local ingest, into a folder instead of a cloud bucket:

```bash
export DATA_URI=./local-data WORK_DIR=./local-work
uv run bocr-scrape ingest ia --max-items 10
uv run bocr-scrape verify --source ia --check-objects -1
```

## Licence

Code: [Apache-2.0](LICENSE). Index data on the Hub:
[ODC-By 1.0](https://opendatacommons.org/licenses/by/1-0/). The source documents keep their own
licences; see the dataset card.

## Citation

```bibtex
@misc{ashraf2026bangladocindex,
  title        = {BanglaDocIndex: An Index of 19.9 Million Bangla Document Pages},
  author       = {Ashraf, Nafis},
  year         = {2026},
  howpublished = {\url{https://huggingface.co/datasets/OpenCR-lab/BanglaDocIndex}}
}
```
