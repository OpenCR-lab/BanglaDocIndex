"""Rebuild page images of BanglaDocIndex from their original sources.

BanglaDocIndex publishes no images. Each page can be rebuilt from its source document: this
script looks up the document in the catalog, downloads the identical PDF (Internet Archive,
a Common Crawl WARC record, or the original website), checks the PDF's SHA-256, and renders the
requested pages with the same recipe that produced the embeddings (150 DPI, never above a
scan's own resolution, at most 8 MP, JPEG quality 88).

    # catalog read straight from the Hugging Face Hub (no full download; slower)
    uv run python examples/fetch_pages.py f0747b20d08a09027bbf42ea8f0dcc35-p000001 --out pages/

    # whole documents, using a local copy of the catalog (faster, and verifies every image)
    hf download OpenCR-lab/BanglaDocIndex --repo-type dataset --local-dir index \
        --include "core/catalog/documents/*" "core/catalog/pages/*"
    uv run python examples/fetch_pages.py --doc f0747b20d08a09027bbf42ea8f0dcc35 \
        --catalog index/core/catalog --out pages/

A page ID is `<doc_id>-pNNNNNN` (1-based page number). With a local catalog the script also
compares each image with the catalog's `image_sha256` (byte-identical) and `phash`.
Websites change: a `live` document whose PDF no longer matches its recorded SHA-256 is skipped.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import os
import shutil
import sys
import tempfile
import threading
from pathlib import Path

import duckdb
import requests

os.environ.setdefault("DATA_URI", "./bangla-doc-index-work")  # required by Settings; unused here

from bocr_scrape.config import Settings  # noqa: E402
from bocr_scrape.fetch import Fetcher, FetchError  # noqa: E402
from bocr_scrape.render import RenderOptions, render_pages  # noqa: E402

HUB_CATALOG = "hf://datasets/OpenCR-lab/BanglaDocIndex/core/catalog"
RECIPE = RenderOptions(target_dpi=150, jpeg_quality=88, max_pixels=8_000_000)


def parse_page_id(page_id: str) -> tuple[str, int]:
    doc_id, _, number = page_id.rpartition("-p")
    if not doc_id or not number.isdigit():
        raise SystemExit(f"not a page ID (expected <doc_id>-pNNNNNN): {page_id}")
    return doc_id, int(number)


def table(catalog: str, name: str) -> str:
    return f"read_parquet('{catalog}/{name}/**/*.parquet', union_by_name=true)"


def quoted(values) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("page_ids", nargs="*", help="page IDs such as <doc_id>-p000001")
    parser.add_argument("--doc", action="append", default=[], help="all pages of this doc_id")
    parser.add_argument("--catalog", default=HUB_CATALOG,
                        help="folder holding the catalog tables (default: read from the Hub)")
    parser.add_argument("--out", type=Path, default=Path("pages"), help="output folder")
    args = parser.parse_args()
    if not args.page_ids and not args.doc:
        parser.error("give page IDs or --doc")

    local = not args.catalog.startswith("hf://")
    wanted: dict[str, set[int]] = collections.defaultdict(set)
    for page_id in args.page_ids:
        doc_id, number = parse_page_id(page_id)
        wanted[doc_id].add(number)
    all_pages = set(args.doc)

    db = duckdb.connect()
    docs = {r[0]: dict(zip([c[0] for c in db.description], r)) for r in db.execute(
        f"SELECT * FROM {table(args.catalog, 'documents')} "
        f"WHERE doc_id IN ({quoted(set(wanted) | all_pages)})").fetchall()}
    for doc_id in set(wanted) | all_pages:
        if doc_id not in docs:
            print(f"{doc_id}: not in the catalog", file=sys.stderr)
        elif docs[doc_id]["duplicate_of"]:
            print(f"{doc_id}: duplicate of {docs[doc_id]['duplicate_of']}; use that doc_id",
                  file=sys.stderr)
    for doc_id in all_pages & docs.keys():
        wanted[doc_id] |= set(range(1, (docs[doc_id]["page_count"] or 0) + 1))

    expected = {}
    if local:  # catalog rows of the requested pages, to verify the rebuilt images
        ids = [f"{d}-p{n:06d}" for d, numbers in wanted.items() for n in numbers]
        for page_id, sha, phash in db.execute(
                f"SELECT page_id, image_sha256, phash FROM {table(args.catalog, 'pages')} "
                f"WHERE doc_id IN ({quoted(wanted)}) AND page_id IN ({quoted(ids)})").fetchall():
            expected[page_id] = (sha, phash)

    fetcher = Fetcher(Settings.from_env(), threading.Event())
    http = requests.Session()
    http.headers["User-Agent"] = "BanglaDocIndex-fetch-pages/1.0 (research)"
    args.out.mkdir(parents=True, exist_ok=True)
    counts = collections.Counter()
    with tempfile.TemporaryDirectory(prefix="bdi-") as temp:
        for doc_id, numbers in wanted.items():
            doc = docs.get(doc_id)
            if not doc or doc["duplicate_of"]:
                continue
            pdf = Path(temp) / f"{doc_id}.pdf"
            candidate = {"source_url": doc["source_url"], "md5": doc["source_md5"],
                         "fetch_method": doc["fetch_method"], "warc_path": doc["warc_path"],
                         "warc_offset": doc["warc_offset"], "warc_length": doc["warc_length"]}
            try:
                fetcher.fetch(candidate, pdf, http)
            except (FetchError, OSError) as exc:
                print(f"{doc_id}: download failed ({doc['fetch_method']}): {exc}", file=sys.stderr)
                counts["download_failed"] += len(numbers)
                continue
            if hashlib.sha256(pdf.read_bytes()).hexdigest() != doc["pdf_sha256"]:
                print(f"{doc_id}: the source now serves a different PDF; skipped", file=sys.stderr)
                counts["pdf_changed"] += len(numbers)
                continue
            rendered = render_pages(str(pdf), sorted(numbers), str(Path(temp) / doc_id), RECIPE)
            for page in rendered:
                page_id = f"{doc_id}-p{page['page_number']:06d}"
                if "error" in page:
                    counts["render_failed"] += 1
                    continue
                shutil.move(page["path"], args.out / f"{page_id}.jpg")
                if page_id in expected:
                    sha, phash = expected[page_id]
                    distance = bin(int(phash, 16) ^ int(page["phash"], 16)).count("1")
                    counts["identical" if page["image_sha256"] == sha else
                           "visually_identical" if distance <= 2 else "different"] += 1
                else:
                    counts["written"] += 1
            pdf.unlink(missing_ok=True)
    print(dict(counts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
