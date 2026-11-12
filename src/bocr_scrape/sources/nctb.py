"""NCTB textbook source (DESIGN.md section 9.3).

Discovery reads the 2026 secondary textbook listings on eproshnobank.com (a third-party
aggregator of NCTB books). Book cards live in the Next.js server-component payload; each
card's first red link is the Bangla PDF, hosted on drive.egovcloud.gov.bd. Madhhomik wins
over Dakhil when titles duplicate. PDFs are stored because these links change every year.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import unicodedata
import urllib.parse
from collections.abc import Iterator

from .base import DiscoveryContext, DocCandidate, ItemCheck, Source

LOG = logging.getLogger(__name__)

YEAR = 2026
LEVEL = "secondary"
BASE_URL = f"https://eproshnobank.com/nctb-books/{YEAR}/{LEVEL}"
CLASS_SLUGS = ("class-6", "class-7", "class-8", "class-9-10")
RIGHTS = {
    "rights_holder": "National Curriculum and Textbook Board (NCTB), Bangladesh",
    "distribution": "free public distribution by the Government of Bangladesh",
    "discovered_via": "eproshnobank.com (third-party aggregator)",
}

CARD_MARKER = re.compile(
    r'className":"font-semibold text-slate-800 truncate",'
    r'"children":"(?P<title>(?:\\.|[^"\\])*)"'
)
HREF = re.compile(r'"href":"(?P<url>https?://[^"\\]+)"')
NEXT_PAYLOAD = re.compile(r"self\.__next_f\.push\((\[.*?\])\)")


def title_key(title: str) -> str:
    normalized = unicodedata.normalize("NFKC", title).casefold()
    return "".join(c for c in normalized if unicodedata.category(c)[0] in {"L", "M", "N"})


def safe_filename(title: str) -> str:
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", title).strip(" .")
    return f"{name or 'untitled'}.pdf"


def direct_download_url(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    if parts.netloc.casefold() == "drive.egovcloud.gov.bd":
        path = parts.path.rstrip("/")
        if not path.endswith("/download"):
            path += "/download"
        return urllib.parse.urlunsplit(parts._replace(path=path))
    return url


def extract_next_data(html: str) -> str:
    parts = []
    for match in NEXT_PAYLOAD.finditer(html):
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list) and len(payload) >= 2 and payload[0] == 1 \
                and isinstance(payload[1], str):
            parts.append(payload[1])
    if not parts:
        raise RuntimeError("page has no recognizable Next.js payload; the site may have changed")
    return "".join(parts)


def parse_book_cards(html: str, page_url: str, category: str) -> tuple[list[dict], list[str]]:
    data = extract_next_data(html)
    markers = list(CARD_MARKER.finditer(data))
    if not markers:
        raise RuntimeError(f"no book cards found at {page_url}; the site may have changed")
    books, missing = [], []
    for index, marker in enumerate(markers):
        end = markers[index + 1].start() if index + 1 < len(markers) else len(data)
        card = data[marker.end():end]
        links = [
            m.group("url") for m in HREF.finditer(card)
            if "border-red-500" in card[m.end():m.end() + 500]
        ]
        title = json.loads(f'"{marker.group("title")}"').strip()
        if not links:
            missing.append(title)
            continue
        books.append({
            "title": title,
            "category": category,
            "share_url": links[0],
            "pdf_url": direct_download_url(links[0]),
            "source_page": page_url,
        })
    return books, missing


class NCTBSource(Source):
    name = "nctb"

    def __init__(self, classes: list[str] | None = None, max_items: int = 0):
        self.classes = classes or list(CLASS_SLUGS)
        unknown = set(self.classes) - set(CLASS_SLUGS)
        if unknown:
            raise ValueError(f"unknown NCTB classes: {sorted(unknown)}")
        self.max_items = max_items

    def _fetch(self, ctx: DiscoveryContext, url: str) -> str:
        response = ctx.http.get(url, timeout=(ctx.settings.connect_timeout, 120))
        response.raise_for_status()
        response.encoding = "utf-8"
        return response.text

    def discover_class(self, ctx: DiscoveryContext, class_slug: str) -> list[dict]:
        madhhomik_url = f"{BASE_URL}/{class_slug}"
        dakhil_url = f"{madhhomik_url}?cat=dakhil"
        madhhomik, missing_m = parse_book_cards(self._fetch(ctx, madhhomik_url), madhhomik_url,
                                                "madhhomik")
        dakhil, missing_d = parse_book_cards(self._fetch(ctx, dakhil_url), dakhil_url, "dakhil")
        seen = {title_key(b["title"]) for b in madhhomik}
        books = list(madhhomik)
        for book in dakhil:
            key = title_key(book["title"])
            if key not in seen:
                seen.add(key)
                books.append(book)
        LOG.info("NCTB %s: %d books (madhhomik %d, dakhil %d, no link %d)", class_slug,
                 len(books), len(madhhomik), len(dakhil), len(missing_m) + len(missing_d))
        for book in books:
            book["class"] = class_slug
        return books

    def discover(self, ctx: DiscoveryContext) -> Iterator[ItemCheck]:
        examined = 0
        for class_slug in self.classes:
            for book in self.discover_class(ctx, class_slug):
                if ctx.stop_event.is_set():
                    return
                url_hash = hashlib.sha256(book["pdf_url"].encode()).hexdigest()[:16]
                item_id = f"{YEAR}/{LEVEL}/{class_slug}/{book['category']}/{url_hash}"
                if self.max_items and examined >= self.max_items:
                    LOG.info("NCTB discovery stopped at max_items=%s", self.max_items)
                    return
                examined += 1  # counts items checked before a restart, too
                if ctx.already_checked(item_id):
                    continue
                candidate = DocCandidate(
                    source_item_id=item_id,
                    source_filename=safe_filename(book["title"]),
                    source_url=book["pdf_url"],
                    title=book["title"],
                    creator="National Curriculum and Textbook Board (NCTB)",
                    pub_year=YEAR,
                    rights_raw=RIGHTS,
                    source_metadata={
                        "year": YEAR, "level": LEVEL, "class": class_slug,
                        "category": book["category"], "share_url": book["share_url"],
                        "source_page": book["source_page"],
                    },
                    store_pdf=True,
                )
                yield ItemCheck(item_id, "accepted", raw_metadata=book, candidates=[candidate])
