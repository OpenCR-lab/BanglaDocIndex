import gzip
import json

import pytest
import requests

from bocr_scrape.fetch import FetchError, extract_warc_payload
from bocr_scrape.scripts import script_composition
from bocr_scrape.sources.internet_archive import (
    DEFAULT_QUERY,
    pub_year,
    select_pdf_files,
    select_text_file,
)
from bocr_scrape.sources.nctb import direct_download_url, parse_book_cards, safe_filename


def test_script_composition():
    bengali = script_composition("আমার সোনার বাংলা")
    assert bengali["bengali"] > 0.99 and bengali["letters"] > 0
    mixed = script_composition("বাংলা" * 10 + "abc" * 10 + "مرحبا" * 2)
    assert 0.3 < mixed["bengali"] < 0.7
    assert mixed["arabic"] > 0
    assert abs(sum(mixed[k] for k in ("bengali", "latin", "arabic", "devanagari", "other"))
               - 1) < 1e-9
    assert script_composition("1234 !!")["letters"] == 0


def test_script_composition_matches_isalpha():
    text = "Ünïcode ক্ষ ১২৩ हिंदी 中文 x"
    assert script_composition(text)["letters"] == sum(c.isalpha() for c in text)


def test_ia_query_covers_all_bengali_tags():
    for tag in ("Bengali", "ben", "bn", "Bangla", "বাংলা"):
        assert tag in DEFAULT_QUERY


def test_ia_file_selection():
    files = [
        {"name": "b.txt"},
        {"name": "book_djvu.txt", "format": "DjVuTXT"},
        {"name": "book.pdf", "format": "Text PDF", "source": "derivative"},
        {"name": "scan.pdf", "format": "Image Container PDF", "source": "original"},
    ]
    assert select_text_file(files)["name"] == "book_djvu.txt"
    assert [f["name"] for f in select_pdf_files(files)] == ["scan.pdf"]
    assert [f["name"] for f in select_pdf_files(files[:3])] == ["book.pdf"]


def test_ia_pub_year():
    assert pub_year({"date": "1952-03-01"}) == 1952
    assert pub_year({"date": "unknown"}) is None


def _warc(body: bytes, headers: bytes = b"Content-Type: application/pdf") -> bytes:
    http = b"HTTP/1.1 200 OK\r\n" + headers + b"\r\n\r\n" + body
    warc = (b"WARC/1.0\r\nWARC-Type: response\r\nContent-Length: "
            + str(len(http)).encode() + b"\r\n\r\n" + http + b"\r\n\r\n")
    return warc


def test_warc_payload_extraction():
    pdf = b"%PDF-1.4 hello %%EOF"
    assert extract_warc_payload(_warc(pdf)) == pdf
    chunked = b"5\r\n%PDF-\r\n4\r\n1.4!\r\n0\r\n\r\n"
    assert extract_warc_payload(_warc(chunked, b"Transfer-Encoding: chunked")) == b"%PDF-1.4!"
    zipped = gzip.compress(pdf)
    assert extract_warc_payload(_warc(zipped, b"Content-Encoding: gzip")) == pdf
    with pytest.raises(FetchError):
        extract_warc_payload(_warc(b"<html>not found</html>"))


def _next_payload(cards: str) -> str:
    return f"<script>self.__next_f.push({json.dumps([1, cards])})</script>"


def test_nctb_card_parsing():
    card = (
        'className":"font-semibold text-slate-800 truncate","children":"বাংলা সাহিত্য"'
        ',"x":{"href":"https://drive.egovcloud.gov.bd/s/abc","className":"border-red-500"}'
        'className":"font-semibold text-slate-800 truncate","children":"No link book"'
    )
    books, missing = parse_book_cards(_next_payload(card), "https://example/page", "madhhomik")
    assert missing == ["No link book"]
    assert books[0]["pdf_url"] == "https://drive.egovcloud.gov.bd/s/abc/download"


def test_nctb_helpers():
    assert direct_download_url("https://example.com/a.pdf") == "https://example.com/a.pdf"
    assert safe_filename('a/b:c?') == "a_b_c_.pdf"


def test_ia_item_check_builds_candidates(monkeypatch):
    """Exercise the whole IA item check with a fake item (no network)."""
    from bocr_scrape.sources.internet_archive import InternetArchiveSource

    record = {
        "metadata": {"title": "বই", "date": "1950", "language": "ben", "licenseurl": "x",
                     "collection": ["digitallibraryindia"], "possible-copyright-status": "NOT"},
        "files": [
            {"name": "book_djvu.txt", "format": "DjVuTXT", "size": "900000"},
            {"name": "book.pdf", "format": "Image Container PDF", "source": "original",
             "size": "123", "md5": "abc", "sha1": "def"},
        ],
    }

    class Item:
        item_metadata = record

    class Session:
        def get_item(self, identifier):
            return Item()

    source = InternetArchiveSource()
    monkeypatch.setattr(source, "_ia", lambda ctx: Session())
    monkeypatch.setattr(source, "_text_sample", lambda ctx, ident, f: "বাংলা text")
    check = source.check(None, "item-1")
    assert check.status == "accepted", check.error
    [candidate] = check.candidates
    assert candidate.md5 == "abc" and candidate.size_bytes == 123
    assert candidate.pub_year == 1950
    assert candidate.script["bengali"] > 0.3 and candidate.script_method
    assert "possible-copyright-status" in candidate.rights_raw


def test_finepdfs_max_items_counts_items_checked_before_restart(tmp_path):
    import threading

    import pyarrow as pa
    import pyarrow.parquet as pq

    from bocr_scrape.sources.base import DiscoveryContext
    from bocr_scrape.sources.finepdfs import META_COLUMNS, FinePDFsSource

    rows = []
    for i in range(5):
        row = {c: None for c in META_COLUMNS}
        row.update(id=f"doc{i}", url=f"https://example.org/{i}.pdf", file_path="crawl/x.warc.gz",
                   offset=0, is_truncated=False, pages=1, script_bengali=1.0, order_key=f"{i}")
        rows.append(row)
    manifest = tmp_path / "manifest.parquet"
    pq.write_table(pa.Table.from_pylist(rows), manifest)
    ctx = DiscoveryContext(settings=None, session_factory=lambda: None,
                           stop_event=threading.Event(),
                           already_checked=lambda item: item in {"doc0", "doc1"})
    checks = list(FinePDFsSource(manifest, max_items=3).discover(ctx))
    assert [c.source_item_id for c in checks] == ["doc2"]


class _FakeResponse:
    def __init__(self, status: int, body: bytes = b""):
        self.status_code = status
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def iter_content(self, size):
        for i in range(0, len(self.body), size):
            yield self.body[i:i + size]


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, url, **kwargs):
        self.calls += 1
        return self.responses.pop(0)


def test_cc_warc_retries_throttling_within_one_attempt(tmp_path, settings):
    import dataclasses
    import threading

    from bocr_scrape.fetch import Fetcher

    from .conftest import make_scanned_pdf

    pdf = make_scanned_pdf(tmp_path / "doc.pdf", pages=1).read_bytes()
    member = gzip.compress(_warc(pdf))
    quick = dataclasses.replace(settings, cc_max_backoff_seconds=0, cc_throttle_retries=5)
    fetcher = Fetcher(quick, threading.Event())
    candidate = {"warc_path": "crawl-data/x.warc.gz", "warc_offset": 0}

    session = _FakeSession([_FakeResponse(503), _FakeResponse(503), _FakeResponse(206, member)])
    info = fetcher.fetch_cc_warc(candidate, tmp_path / "out.pdf", session)
    assert session.calls == 3
    assert (tmp_path / "out.pdf").read_bytes() == pdf
    assert info["warc_length"] == len(member)

    always = _FakeSession([_FakeResponse(503)] * 5)
    with pytest.raises(FetchError) as excinfo:
        fetcher.fetch_cc_warc(candidate, tmp_path / "out2.pdf", always)
    assert excinfo.value.retryable and always.calls == 5


def test_cc_throttle_does_not_escalate_during_a_pause(settings):
    import threading

    from bocr_scrape.fetch import Fetcher

    fetcher = Fetcher(settings, threading.Event())
    for _ in range(6):  # six requests that were in flight all get 503 at once
        fetcher._cc_throttled()
    assert fetcher.cc_backoff == 5.0


def test_ia_search_pages_with_cursor_and_no_sort():
    import threading

    from bocr_scrape.sources.base import DiscoveryContext
    from bocr_scrape.sources.internet_archive import InternetArchiveSource

    class Page:
        def __init__(self, data):
            self.data = data

        def raise_for_status(self):
            pass

        def json(self):
            return self.data

    class Scrape:
        def __init__(self):
            self.params = []

        def get(self, url, params=None, timeout=None):
            self.params.append(dict(params))
            if "cursor" not in params:
                return Page({"items": [{"identifier": "a"}, {"identifier": "b"}], "cursor": "c1"})
            return Page({"items": [{"identifier": "c"}]})

    scrape = Scrape()
    ctx = DiscoveryContext(settings=None, session_factory=lambda: scrape,
                           stop_event=threading.Event(), already_checked=lambda i: i == "b")
    assert list(InternetArchiveSource().iter_items(ctx)) == ["a", "c"]
    assert scrape.params[1]["cursor"] == "c1"
    assert all("sorts" not in p and "sort" not in p for p in scrape.params)


def test_cc_s3_reader_ranges_fallback_and_errors(tmp_path):
    import threading

    from botocore.exceptions import ClientError

    from bocr_scrape.fetch import CommonCrawlS3, _Throttled

    from .conftest import make_scanned_pdf

    member = gzip.compress(_warc(make_scanned_pdf(tmp_path / "d.pdf", pages=1).read_bytes()))
    blob = b"x" * 100 + member + gzip.compress(b"next record")

    class Body:
        def __init__(self, data):
            self.data = data

        def iter_chunks(self, size):
            for i in range(0, len(self.data), size):
                yield self.data[i:i + size]

        def close(self):
            pass

    class Client:
        def __init__(self, error=None):
            self.ranges, self.error = [], error

        def get_object(self, Bucket, Key, Range):
            self.ranges.append(Range)
            if self.error:
                raise ClientError({"Error": {"Code": self.error},
                                   "ResponseMetadata": {"HTTPStatusCode": 503 if self.error == "SlowDown" else 404}},
                                  "GetObject")
            start, _, end = Range.removeprefix("bytes=").partition("-")
            stop = int(end) + 1 if end else len(blob)
            return {"Body": Body(blob[int(start):stop])}

    def reader(client, range_bytes):
        r = CommonCrawlS3.__new__(CommonCrawlS3)
        r.client, r.range_bytes = client, range_bytes
        return r

    stop = threading.Event()
    big = Client()
    record, used = reader(big, 10_000_000).read_member("k", 100, stop)
    assert used == len(member) and record == gzip.decompress(member)
    assert big.ranges == [f"bytes=100-{100 + 10_000_000 - 1}"]

    small = Client()  # the record is longer than the bounded range: open-ended retry
    record, used = reader(small, 50).read_member("k", 100, stop)
    assert used == len(member) and small.ranges[-1] == "bytes=100-"

    with pytest.raises(_Throttled):
        reader(Client("SlowDown"), 50).read_member("k", 100, stop)
    with pytest.raises(FetchError) as excinfo:
        reader(Client("NoSuchKey"), 50).read_member("k", 100, stop)
    assert not excinfo.value.retryable
