"""The contract every source plug-in implements (DESIGN.md section 9.1).

A source only discovers and describes documents. Downloading, rendering, uploading,
catalog writing and resume are shared by all sources in bocr_scrape.engine.
"""

from __future__ import annotations

import dataclasses
import threading
from abc import ABC
from collections.abc import Callable, Iterator
from typing import Any

import requests

from ..config import Settings


@dataclasses.dataclass
class DocCandidate:
    """One source PDF that should become a document in the catalog."""

    source_item_id: str
    source_filename: str
    source_url: str
    title: str | None = None
    creator: str | None = None
    pub_year: int | None = None
    language: str = "bn"
    size_bytes: int | None = None
    md5: str | None = None
    # How to fetch it: http (stable host) | cc_warc (Common Crawl archive) | live (website).
    fetch_method: str = "http"
    warc_path: str | None = None
    warc_offset: int | None = None
    # Script composition measured from the source's text (never used to filter).
    script: dict[str, Any] = dataclasses.field(default_factory=dict)
    script_method: str | None = None
    license_url: str | None = None
    rights_raw: dict[str, Any] = dataclasses.field(default_factory=dict)
    source_metadata: dict[str, Any] = dataclasses.field(default_factory=dict)
    # Keep the original PDF in a PDF shard (only for sources that may disappear).
    store_pdf: bool = False


@dataclasses.dataclass
class ItemCheck:
    """The outcome of examining one source item, recorded in source_items."""

    source_item_id: str
    status: str  # accepted | no_pdf | skipped | error
    script: dict[str, Any] = dataclasses.field(default_factory=dict)
    script_method: str | None = None
    text_file: str | None = None
    error: str | None = None
    raw_metadata: dict[str, Any] = dataclasses.field(default_factory=dict)
    candidates: list[DocCandidate] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class DiscoveryContext:
    settings: Settings
    session_factory: Callable[[], requests.Session]
    stop_event: threading.Event
    already_checked: Callable[[str], bool]
    _local: threading.local = dataclasses.field(default_factory=threading.local, repr=False)

    @property
    def http(self) -> requests.Session:
        """An HTTP session private to the calling thread (checks may run concurrently)."""
        session = getattr(self._local, "session", None)
        if session is None:
            session = self._local.session = self.session_factory()
        return session


class Source(ABC):
    #: Short stable name used in IDs, paths and the catalog ("ia", "nctb", ...).
    name: str
    #: True if check() calls are independent and may run concurrently (see the engine).
    parallel_discovery: bool = False

    def iter_items(self, ctx: DiscoveryContext) -> Iterator[str]:
        """Yield IDs of source items not yet checked (honouring ctx.already_checked)."""
        raise NotImplementedError

    def check(self, ctx: DiscoveryContext, item_id: str) -> ItemCheck:
        """Examine one item. Must not raise: return an ItemCheck with status 'error' instead."""
        raise NotImplementedError

    def discover(self, ctx: DiscoveryContext) -> Iterator[ItemCheck]:
        """Sequential discovery. Sources override this or implement iter_items + check."""
        for item_id in self.iter_items(ctx):
            if ctx.stop_event.is_set():
                return
            yield self.check(ctx, item_id)
