"""Consistency checks over what a run (or a whole source) wrote to the dataset."""

from __future__ import annotations

import collections
import hashlib
import logging
import random

from . import catalog
from .ids import page_id, split_for
from .storage import Storage

LOG = logging.getLogger(__name__)


def verify(storage: Storage, source: str | None = None, run_id: str | None = None,
           check_pages: int = 200, split_val: float = 0.05, split_test: float = 0.05,
           seed: int = 0) -> dict:
    """Return a report; report['problems'] is empty when everything is consistent.

    check_pages: how many pages to read back from their shards and hash-check (byte-range
    reads; -1 = all). Shards already moved off this storage are skipped and counted.
    """
    problems: list[str] = []
    docs = catalog.read_table(storage, "documents", source, run_id).to_pylist()
    pages = catalog.read_table(storage, "pages", source, run_id).to_pylist()
    items = catalog.read_table(storage, "source_items", source, run_id).to_pylist()
    shards = catalog.read_table(storage, "shards", source, run_id).to_pylist()

    doc_ids = collections.Counter(d["doc_id"] for d in docs)
    page_ids = collections.Counter(p["page_id"] for p in pages)
    if any(n > 1 for n in doc_ids.values()):
        problems.append("duplicate doc rows (consolidation will dedupe)")
    if any(n > 1 for n in page_ids.values()):
        problems.append("duplicate page rows (consolidation will dedupe)")

    shard_index = {s["shard_key"]: s for s in shards}
    by_doc: dict[str, list[dict]] = collections.defaultdict(list)
    for page in pages:
        by_doc[page["doc_id"]].append(page)
        if page["shard_key"] not in shard_index:
            problems.append(f"{page['page_id']}: shard {page['shard_key']} not in shards table")
            break
    rendered = [d for d in {d["doc_id"]: d for d in docs}.values() if not d["duplicate_of"]]
    for doc in rendered:
        numbers = {p["page_number"] for p in by_doc.get(doc["doc_id"], [])}
        if numbers != set(range(1, doc["page_count"] + 1)):
            problems.append(f"{doc['doc_id']}: pages {len(numbers)}/{doc['page_count']}")
        if doc["split"] != split_for(doc["doc_id"], split_val, split_test):
            problems.append(f"{doc['doc_id']}: split does not match its hash")
        if doc["kept_pdf_shard"] and doc["kept_pdf_shard"] not in shard_index:
            problems.append(f"{doc['doc_id']}: kept PDF shard missing from shards table")
    for page in pages:
        if page["page_id"] != page_id(page["doc_id"], page["page_number"]):
            problems.append(f"{page['page_id']}: id does not match doc/page number")
            break

    rng = random.Random(seed)
    sample = pages if check_pages < 0 else rng.sample(pages, min(check_pages, len(pages)))
    present = {key: storage.exists(key) for key in {p["shard_key"] for p in sample}}
    checked = moved = bad = 0
    for page in sample:
        if not present[page["shard_key"]]:
            moved += 1
            continue
        data = storage.get_range(page["shard_key"], page["offset"], page["length"])
        checked += 1
        if hashlib.sha256(data).hexdigest() != page["image_sha256"]:
            bad += 1
    if bad:
        problems.append(f"{bad}/{checked} sampled pages do not match their checksum")

    kept_checked = kept_bad = 0
    for doc in [d for d in rendered if d["kept_pdf_shard"]][:50]:
        if not storage.exists(doc["kept_pdf_shard"]):
            continue
        data = storage.get_range(doc["kept_pdf_shard"], doc["kept_pdf_offset"],
                                 doc["kept_pdf_length"])
        kept_checked += 1
        if hashlib.sha256(data).hexdigest() != doc["pdf_sha256"]:
            kept_bad += 1
    if kept_bad:
        problems.append(f"{kept_bad}/{kept_checked} kept PDFs do not match their checksum")

    return {
        "source": source,
        "run_id": run_id,
        "source_items": dict(collections.Counter(i["status"] for i in items)),
        "documents": len(doc_ids),
        "documents_duplicate": sum(1 for d in docs if d["duplicate_of"]),
        "fetch_methods": dict(collections.Counter(d["fetch_method"] for d in docs)),
        "pages": len(page_ids),
        "page_bytes": sum(p["length"] for p in pages),
        "shards": dict(collections.Counter(s["kind"] for s in shards)),
        "splits": dict(collections.Counter(d["split"] for d in rendered)),
        "pages_hash_checked": checked,
        "pages_skipped_moved": moved,
        "kept_pdfs_hash_checked": kept_checked,
        "problems": problems,
    }
