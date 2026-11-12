"""Deterministic identifiers and split assignment (DESIGN.md sections 7 and 9.4).

These functions define the dataset's identity. Changing them changes every ID, so treat
them as frozen; bump SCHEMA_VERSION and write a migration note if they ever must change.
"""

from __future__ import annotations

import datetime as dt
import hashlib

SCHEMA_VERSION = 2


def doc_id(source: str, source_item_id: str, source_filename: str) -> str:
    key = f"{source}\n{source_item_id}\n{source_filename}".encode()
    return hashlib.sha256(key).hexdigest()[:32]


def page_id(doc: str, page_number: int) -> str:
    if page_number < 1:
        raise ValueError("page numbers are 1-based")
    return f"{doc}-p{page_number:06d}"


def split_for(doc: str, val_fraction: float, test_fraction: float) -> str:
    """Assign train/val/test from a hash of the document ID, so all pages share one split."""
    if val_fraction < 0 or test_fraction < 0 or val_fraction + test_fraction >= 1:
        raise ValueError("invalid split fractions")
    digest = hashlib.sha256(f"split\n{doc}".encode()).digest()
    position = int.from_bytes(digest[:8], "big") / 2**64
    if position < test_fraction:
        return "test"
    if position < test_fraction + val_fraction:
        return "val"
    return "train"


def new_run_id(stage: str, source: str, now: dt.datetime | None = None) -> str:
    now = now or dt.datetime.now(dt.UTC)
    return f"{stage}-{source}-{now.strftime('%Y%m%dT%H%M%SZ')}"
