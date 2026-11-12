"""Internet Archive source (DESIGN.md section 9.3).

Discovery: every item whose language metadata normalizes to Bengali (all spellings and codes
uploaders use), with every original PDF. Nothing is filtered by content: the script mix of
each item's OCR text is measured and stored for later analysis. IA PDFs are not stored
(IA is the archive; source_url + md5 identify the exact file).
"""

from __future__ import annotations

import logging
import re
import threading
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

import internetarchive as ia

from ..scripts import script_composition
from .base import DiscoveryContext, DocCandidate, ItemCheck, Source

LOG = logging.getLogger(__name__)

# All forms of the language field seen on IA for Bengali (English name, ISO 639-2/3 code,
# ISO 639-1 code, endonym); `bengali` matches case-insensitively.
DEFAULT_QUERY = "language:(Bengali OR ben OR bn OR Bangla OR বাংলা)"
SCRIPT_METHOD = "ia_djvu_txt_3x256k"
TEXT_WINDOWS = 3
TEXT_WINDOW_BYTES = 256 * 1024
SEARCH_ATTEMPTS = 20
SCRAPE_URL = "https://archive.org/services/search/v1/scrape"
SCRAPE_PAGE = 10000
RIGHTS_KEY_PATTERN = re.compile(r"right|licen|copyright", re.IGNORECASE)
SOURCE_METADATA_KEYS = (
    "collection", "mediatype", "publisher", "date", "year", "language", "contributor",
    "sponsor", "scanner", "ppi", "subject", "imagecount", "volume", "isbn", "identifier-ark",
)


def file_url(identifier: str, filename: str) -> str:
    return f"https://archive.org/download/{quote(identifier, safe='')}/{quote(filename, safe='/')}"


def _first(value: Any) -> Any:
    return value[0] if isinstance(value, list) and value else value


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        return "; ".join(str(v) for v in value) or None
    return str(value)


def pub_year(metadata: dict) -> int | None:
    for key in ("year", "date"):
        match = re.search(r"\b(1[5-9]\d\d|20\d\d)\b", str(_first(metadata.get(key)) or ""))
        if match:
            return int(match.group(1))
    return None


def select_text_file(files: list[dict]) -> dict | None:
    def name(f: dict) -> str:
        return str(f.get("name") or "")

    for predicate in (
        lambda f: name(f).lower().endswith("_djvu.txt"),
        lambda f: str(f.get("format") or "").lower() == "djvutxt",
        lambda f: name(f).lower().endswith(".txt"),
    ):
        matches = sorted((f for f in files if predicate(f)), key=lambda f: name(f).lower())
        if matches:
            return matches[0]
    return None


def select_pdf_files(files: list[dict]) -> list[dict]:
    candidates = [
        f for f in files
        if str(f.get("name") or "").lower().endswith(".pdf")
        or "pdf" in str(f.get("format") or "").lower()
    ]
    originals = [f for f in candidates if str(f.get("source") or "").lower() == "original"]
    return sorted(originals or candidates, key=lambda f: str(f.get("name")).lower())


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class InternetArchiveSource(Source):
    name = "ia"
    parallel_discovery = True

    def __init__(self, query: str = DEFAULT_QUERY, max_items: int = 0,
                 identifiers: list[str] | None = None):
        self.query = query
        self.max_items = max_items
        self.identifiers = identifiers
        self._local = threading.local()

    def _ia(self, ctx: DiscoveryContext) -> ia.ArchiveSession:
        """One internetarchive session per thread (checks run concurrently)."""
        session = getattr(self._local, "session", None)
        if session is None:
            session = ia.get_session(
                config={"general": {"user_agent_suffix": ctx.settings.user_agent}})
            self._local.session = session
        return session

    def _search(self, ctx: DiscoveryContext) -> Iterator[str]:
        if self.identifiers:
            yield from self.identifiers
            return
        # IA's scrape API pages with a cursor. Do not ask for a sort: with `sorts` the cursor
        # is rejected ("Bad cursor") after the first 10,000 results (seen in the main run).
        cursor = None
        while True:
            params = {"q": self.query, "fields": "identifier", "count": SCRAPE_PAGE}
            if cursor:
                params["cursor"] = cursor
            response = ctx.http.get(SCRAPE_URL, params=params, timeout=180)
            response.raise_for_status()
            page = response.json()
            if "error" in page:
                raise RuntimeError(f"IA scrape API: {page['error']}")
            for item in page.get("items", []):
                yield str(item.get("identifier") or "").strip()
            cursor = page.get("cursor")
            if not cursor:
                return

    def iter_items(self, ctx: DiscoveryContext) -> Iterator[str]:
        """Yield unchecked identifiers. If the IA search API fails mid-way (it has outages),
        wait and restart the search; items already checked are skipped quickly."""
        LOG.info("IA discovery: query=%r identifiers=%s max_items=%s", self.query,
                 len(self.identifiers) if self.identifiers else "search", self.max_items)
        for attempt in range(1, SEARCH_ATTEMPTS + 1):
            # max_items counts items checked in earlier sessions too, so a capped run
            # keeps its cap across restarts.
            examined = 0
            try:
                for identifier in self._search(ctx):
                    if ctx.stop_event.is_set():
                        return
                    if not identifier:
                        continue
                    if self.max_items and examined >= self.max_items:
                        LOG.info("IA discovery stopped at max_items=%s", self.max_items)
                        return
                    examined += 1
                    if not ctx.already_checked(identifier):
                        yield identifier
                return
            except Exception as exc:  # noqa: BLE001 - search outages are common and transient
                if attempt == SEARCH_ATTEMPTS:
                    raise
                delay = min(900, 60 * attempt)
                LOG.warning("IA search failed (attempt %d/%d); restarting in %ds: %s", attempt,
                            SEARCH_ATTEMPTS, delay, exc)
                if ctx.stop_event.wait(delay):
                    return

    def _text_sample(self, ctx: DiscoveryContext, identifier: str, text_file: dict) -> str:
        """Three evenly spaced 256 KB windows of the OCR text (start, middle, end), so long
        books are not judged by their front matter alone. Raises on fetch errors."""
        size = _int(text_file.get("size")) or 0
        url = file_url(identifier, str(text_file["name"]))
        if size <= TEXT_WINDOWS * TEXT_WINDOW_BYTES:
            starts = [0]
            length = max(size, TEXT_WINDOW_BYTES)
        else:
            step = (size - TEXT_WINDOW_BYTES) // (TEXT_WINDOWS - 1)
            starts = [i * step for i in range(TEXT_WINDOWS)]
            length = TEXT_WINDOW_BYTES
        parts = []
        for start in starts:
            response = ctx.http.get(
                url, headers={"Range": f"bytes={start}-{start + length - 1}"},
                timeout=(ctx.settings.connect_timeout, ctx.settings.read_timeout),
            )
            response.raise_for_status()
            parts.append(response.content.decode("utf-8", errors="ignore"))
        return "\n".join(parts)

    def check(self, ctx: DiscoveryContext, item_id: str) -> ItemCheck:
        try:
            return self._check(ctx, item_id)
        except Exception as exc:  # noqa: BLE001 - recorded as error, retried next run
            LOG.warning("IA item check failed for %s (will retry next run): %s", item_id, exc)
            return ItemCheck(item_id, "error", error=str(exc)[:2000])

    def _check(self, ctx: DiscoveryContext, identifier: str) -> ItemCheck:
        item = self._ia(ctx).get_item(identifier)
        record = item.item_metadata or {}
        metadata = record.get("metadata") or {}
        files = record.get("files") or []
        raw = {
            "metadata": metadata,
            "created": record.get("created"),
            "item_size": record.get("item_size"),
            "files_count": len(files),
            "is_dark": record.get("is_dark"),
        }
        if not metadata:
            return ItemCheck(identifier, "skipped", error="item has no metadata (dark or removed)",
                             raw_metadata=raw)

        text_file = select_text_file(files)
        script, method = {}, None
        if text_file is not None:
            script = script_composition(self._text_sample(ctx, identifier, text_file))
            method = SCRIPT_METHOD
        check = ItemCheck(identifier, "accepted", script=script, script_method=method,
                          text_file=None if text_file is None else str(text_file["name"]),
                          raw_metadata=raw)
        pdfs = select_pdf_files(files)
        if not pdfs:
            check.status = "no_pdf"
            return check

        rights = {k: v for k, v in metadata.items() if RIGHTS_KEY_PATTERN.search(k)}
        source_metadata = {k: metadata[k] for k in SOURCE_METADATA_KEYS if k in metadata}
        for pdf in pdfs:
            name = str(pdf["name"])
            check.candidates.append(
                DocCandidate(
                    source_item_id=identifier,
                    source_filename=name,
                    source_url=file_url(identifier, name),
                    title=_text(metadata.get("title")),
                    creator=_text(metadata.get("creator")),
                    pub_year=pub_year(metadata),
                    size_bytes=_int(pdf.get("size")),
                    md5=pdf.get("md5"),
                    script=script,
                    script_method=method,
                    license_url=_text(metadata.get("licenseurl")),
                    rights_raw=rights,
                    source_metadata={**source_metadata, "pdf_format": pdf.get("format"),
                                     "pdf_source": pdf.get("source")},
                    store_pdf=False,
                )
            )
        return check
