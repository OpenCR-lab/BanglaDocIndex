"""Rebuild page images from the catalog alone (DESIGN.md section 6, "cache blobs").

For each selected document: fetch the identical PDF via its fetch spec (Internet Archive
download, Common Crawl byte range, or the kept copy for sources that may disappear), check
its SHA-256, render the selected pages with the recorded recipe, and compare every image
with the catalog: exact (same SHA-256), perceptually identical (pHash distance <= 2), or
different. The report is the reproducibility evidence for the paper.
"""

from __future__ import annotations

import collections
import hashlib
import json
import logging
import random
import tempfile
import threading
from pathlib import Path

import requests

from . import catalog
from .fetch import Fetcher, FetchError
from .render import RenderOptions, render_pages
from .storage import Storage

LOG = logging.getLogger(__name__)


def _phash_distance(a: str, b: str) -> int:
    return bin(int(a, 16) ^ int(b, 16)).count("1")


def recipe_options(storage: Storage, recipe: str) -> RenderOptions:
    data = storage.get_bytes(f"core/recipes/{recipe}.json")
    if data is None:
        raise SystemExit(f"recipe {recipe} not found under core/recipes/")
    spec = json.loads(data)
    return RenderOptions(target_dpi=spec["target_dpi"],
                         jpeg_quality=spec["jpeg_quality"], max_pixels=spec["max_pixels"])


def rehydrate_sample(settings, storage: Storage, source: str, pages_wanted: int,
                     out_dir: Path | None = None, seed: int = 0) -> dict:
    docs = {d["doc_id"]: d for d in catalog.read_table(storage, "documents", source).to_pylist()
            if not d["duplicate_of"]}
    pages = [p for p in catalog.read_table(storage, "pages", source).to_pylist()
             if p["doc_id"] in docs]
    rng = random.Random(seed)
    sample = rng.sample(pages, min(pages_wanted, len(pages)))
    by_doc: dict[str, list[dict]] = collections.defaultdict(list)
    for page in sample:
        by_doc[page["doc_id"]].append(page)

    fetcher = Fetcher(settings, threading.Event())
    http = requests.Session()
    http.headers["User-Agent"] = settings.user_agent
    options_cache: dict[str, RenderOptions] = {}
    result = collections.Counter()
    by_method = collections.Counter()
    with tempfile.TemporaryDirectory(prefix="bocr-rehydrate-") as temp:
        work = Path(temp)
        for doc_id, doc_pages in by_doc.items():
            doc = docs[doc_id]
            pdf = work / f"{doc_id}.pdf"
            method = "kept_pdf" if doc["kept_pdf_shard"] else doc["fetch_method"]
            try:
                if doc["kept_pdf_shard"]:
                    pdf.write_bytes(storage.get_range(doc["kept_pdf_shard"],
                                                      doc["kept_pdf_offset"],
                                                      doc["kept_pdf_length"]))
                else:
                    candidate = {"source_url": doc["source_url"], "md5": doc["source_md5"],
                                 "fetch_method": doc["fetch_method"],
                                 "warc_path": doc["warc_path"],
                                 "warc_offset": doc["warc_offset"]}
                    fetcher.fetch(candidate, pdf, http)
            except (FetchError, OSError) as exc:
                LOG.warning("Could not fetch %s (%s): %s", doc_id, method, exc)
                result["fetch_failed"] += len(doc_pages)
                by_method[f"{method}:failed"] += 1
                continue
            if hashlib.sha256(pdf.read_bytes()).hexdigest() != doc["pdf_sha256"]:
                result["pdf_changed"] += len(doc_pages)
                by_method[f"{method}:pdf_changed"] += 1
                continue
            by_method[f"{method}:ok"] += 1
            recipe = doc_pages[0]["render_recipe"]
            if recipe not in options_cache:
                options_cache[recipe] = recipe_options(storage, recipe)
            rendered = render_pages(str(pdf), [p["page_number"] for p in doc_pages],
                                    str(out_dir or work / "out"), options_cache[recipe])
            by_number = {r["page_number"]: r for r in rendered}
            for page in doc_pages:
                new = by_number[page["page_number"]]
                if new["image_sha256"] == page["image_sha256"]:
                    result["exact"] += 1
                elif _phash_distance(new["phash"], page["phash"]) <= 2:
                    result["perceptual"] += 1
                else:
                    result["different"] += 1
            pdf.unlink(missing_ok=True)
    return {"source": source, "pages_sampled": len(sample), "documents": len(by_doc),
            "results": dict(result), "fetch": dict(by_method)}
