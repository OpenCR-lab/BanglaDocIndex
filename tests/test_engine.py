"""End-to-end ingestion against a local HTTP server and the local storage backend."""

import dataclasses
import shutil
from collections.abc import Iterator

from bocr_scrape import catalog
from bocr_scrape.engine import IngestEngine
from bocr_scrape.sources.base import DiscoveryContext, DocCandidate, ItemCheck, Source
from bocr_scrape.storage import open_storage
from bocr_scrape.verify import verify

from .conftest import make_scanned_pdf


class FakeSource(Source):
    name = "fake"

    def __init__(self, base_url: str):
        self.base_url = base_url

    def discover(self, ctx: DiscoveryContext) -> Iterator[ItemCheck]:
        script = {"bengali": 0.9, "latin": 0.1, "arabic": 0.0, "devanagari": 0.0,
                  "other": 0.0, "letters": 100}
        items = [
            ItemCheck("item-a", "accepted", script=script, script_method="test", candidates=[
                DocCandidate("item-a", "a.pdf", f"{self.base_url}/a.pdf", title="A",
                             script=script, script_method="test")]),
            ItemCheck("item-b", "accepted", candidates=[
                DocCandidate("item-b", "b.pdf", f"{self.base_url}/b.pdf", title="B",
                             store_pdf=True, rights_raw={"licenseurl": "x"})]),
            ItemCheck("item-c", "no_pdf"),
            ItemCheck("item-d", "accepted", candidates=[
                DocCandidate("item-d", "missing.pdf", f"{self.base_url}/missing.pdf")]),
            ItemCheck("item-e", "accepted", candidates=[
                DocCandidate("item-e", "copy.pdf", f"{self.base_url}/a_copy.pdf")]),
        ]
        for item in items:
            if not ctx.already_checked(item.source_item_id):
                yield item


def _write_pdfs(root):
    make_scanned_pdf(root / "a.pdf", pages=5, dpi=150, seed=1)
    make_scanned_pdf(root / "b.pdf", pages=3, dpi=300, seed=2)
    shutil.copy(root / "a.pdf", root / "a_copy.pdf")


def test_ingest_end_to_end_and_rerun_is_idempotent(settings, http_dir):
    root, base_url = http_dir
    _write_pdfs(root)
    storage = open_storage(settings.data_uri)

    assert IngestEngine(settings, FakeSource(base_url), storage).run() == 0

    docs = catalog.read_table(storage, "documents", "fake").to_pylist()
    pages = catalog.read_table(storage, "pages", "fake").to_pylist()
    shards = catalog.read_table(storage, "shards", "fake").to_pylist()
    by_name = {d["source_filename"]: d for d in docs}
    assert sorted(by_name) == ["a.pdf", "b.pdf", "copy.pdf"]  # missing.pdf failed (404)
    assert by_name["copy.pdf"]["duplicate_of"] == by_name["a.pdf"]["doc_id"]
    assert by_name["copy.pdf"]["page_count"] is None
    assert len(pages) == 8
    assert {p["dpi"] for p in pages if p["doc_id"] == by_name["a.pdf"]["doc_id"]} == {150}
    assert {p["dpi"] for p in pages if p["doc_id"] == by_name["b.pdf"]["doc_id"]} == {150}
    assert by_name["b.pdf"]["kept_pdf_shard"] and not by_name["a.pdf"]["kept_pdf_shard"]
    assert by_name["a.pdf"]["script_bengali"] == 0.9
    assert {s["kind"] for s in shards} == {"pages", "pdfs"}
    assert len([s for s in shards if s["kind"] == "pages"]) >= 2  # small test shard target
    for shard in shards:  # every catalogued shard exists, complete
        assert storage.exists(shard["shard_key"])
    assert storage.get_bytes("core/recipes/" + pages[0]["render_recipe"] + ".json")

    report = verify(storage, "fake", check_pages=-1)
    assert report["problems"] == [], report
    assert report["pages_hash_checked"] == 8 and report["kept_pdfs_hash_checked"] == 1

    # A second run over the same work dir finds nothing new and writes no duplicate rows.
    IngestEngine(settings, FakeSource(base_url), storage).run()
    assert len(catalog.read_table(storage, "pages", "fake")) == 8
    assert len(catalog.read_table(storage, "documents", "fake")) == 3


def test_stop_before_seal_redoes_staged_documents(settings, http_dir):
    """A Spot preemption between staging and sealing loses nothing and duplicates nothing."""
    root, base_url = http_dir
    _write_pdfs(root)
    settings = dataclasses.replace(settings, shard_target_bytes=10**12)  # never seal early
    storage = open_storage(settings.data_uri)

    engine = IngestEngine(settings, FakeSource(base_url), storage)
    original = engine.state.stage_document

    def stage_then_stop(*args, **kwargs):
        original(*args, **kwargs)
        engine.stop.set()  # "preempted" right after the first document is staged

    engine.state.stage_document = stage_then_stop
    assert engine.run() == 1
    assert len(catalog.read_table(storage, "pages", "fake")) == 0  # nothing committed
    assert len(list(storage.list("blobs/"))) == 0  # no partial shard in storage

    assert IngestEngine(settings, FakeSource(base_url), storage).run() == 0
    pages = catalog.read_table(storage, "pages", "fake").to_pylist()
    assert len(pages) == 8 and len({p["page_id"] for p in pages}) == 8
    assert verify(storage, "fake", check_pages=-1)["problems"] == []


class ParallelSource(Source):
    """Uses the concurrent discovery path (iter_items + check)."""

    name = "par"
    parallel_discovery = True

    def __init__(self, base_url: str, count: int):
        self.base_url, self.count = base_url, count

    def iter_items(self, ctx):
        for i in range(self.count):
            if not ctx.already_checked(f"item-{i}"):
                yield f"item-{i}"

    def check(self, ctx, item_id):
        assert ctx.http is ctx.http  # one session per thread
        return ItemCheck(item_id, "accepted", candidates=[
            DocCandidate(item_id, "doc.pdf", f"{self.base_url}/{item_id}.pdf")])


def test_parallel_discovery_and_downloads(settings, http_dir):
    root, base_url = http_dir
    for i in range(6):
        make_scanned_pdf(root / f"item-{i}.pdf", pages=2, dpi=100, seed=i)
    settings = dataclasses.replace(settings, discovery_workers=4, download_workers=3)
    storage = open_storage(settings.data_uri)
    assert IngestEngine(settings, ParallelSource(base_url, 6), storage).run() == 0
    assert len(catalog.read_table(storage, "documents", "par")) == 6
    assert len(catalog.read_table(storage, "pages", "par")) == 12
    assert verify(storage, "par", check_pages=-1)["problems"] == []


def test_errored_items_are_rechecked_within_the_run(settings, http_dir):
    root, base_url = http_dir
    make_scanned_pdf(root / "item-0.pdf", pages=1, dpi=100)

    class Flaky(ParallelSource):
        calls = 0

        def check(self, ctx, item_id):
            Flaky.calls += 1
            if Flaky.calls == 1:
                return ItemCheck(item_id, "error", error="IA 500")
            return super().check(ctx, item_id)

    storage = open_storage(settings.data_uri)
    assert IngestEngine(settings, Flaky(base_url, 1), storage).run() == 0
    assert Flaky.calls == 2
    assert len(catalog.read_table(storage, "documents", "par")) == 1


def test_several_documents_processed_at_once(settings, http_dir):
    root, base_url = http_dir
    for i in range(12):
        make_scanned_pdf(root / f"item-{i}.pdf", pages=1 + i % 3, dpi=100, seed=i)
    settings = dataclasses.replace(settings, download_workers=4, process_workers=4,
                                   render_workers=2)
    storage = open_storage(settings.data_uri)
    assert IngestEngine(settings, ParallelSource(base_url, 12), storage).run() == 0
    docs = catalog.read_table(storage, "documents", "par").to_pylist()
    pages = catalog.read_table(storage, "pages", "par").to_pylist()
    assert len(docs) == 12 and len(pages) == sum(1 + i % 3 for i in range(12))
    assert len({p["page_id"] for p in pages}) == len(pages)
    # all pages of a document are in one shard, in page order, at increasing offsets
    for doc in docs:
        mine = sorted((p for p in pages if p["doc_id"] == doc["doc_id"]),
                      key=lambda p: p["page_number"])
        assert len({p["shard_key"] for p in mine}) == 1
        assert [p["offset"] for p in mine] == sorted(p["offset"] for p in mine)
    assert verify(storage, "par", check_pages=-1)["problems"] == []


def _die(_):
    import os
    os._exit(1)


def _square(x):
    return x * x


def test_render_pool_replaces_itself_after_a_worker_dies():
    import concurrent.futures as cf

    import pytest

    from bocr_scrape.engine import RenderPool

    pool = RenderPool(2)
    try:
        with pytest.raises(cf.process.BrokenProcessPool):
            pool.submit(_die, 0).result(timeout=60)
        pool.replace_if_broken()
        assert pool.rebuilds == 1
        assert pool.submit(_square, 7).result(timeout=60) == 49
    finally:
        pool.shutdown()


def test_retry_reuses_the_downloaded_pdf_only_if_unchanged(settings, http_dir, tmp_path):
    import json as _json

    from bocr_scrape.engine import file_digests

    root, base_url = http_dir
    storage = open_storage(settings.data_uri)
    engine = IngestEngine(settings, ParallelSource(base_url, 1), storage)
    dest = tmp_path / "doc.pdf"
    make_scanned_pdf(dest, pages=1, dpi=100)
    digests = file_digests(dest)
    job = {"fetch_json": _json.dumps({"warc_length": 5, "digests": digests})}
    fetch, again = engine._reuse_download(job, dest)
    assert fetch == {"warc_length": 5} and again["sha256"] == digests["sha256"]
    dest.write_bytes(b"%PDF-1.4 changed")
    assert engine._reuse_download(job, dest) is None
    assert engine._reuse_download({"fetch_json": None}, dest) is None


def test_checkpoint_streams_and_restores(settings, http_dir, tmp_path):
    import dataclasses as _dc
    import sqlite3

    from bocr_scrape.engine import restore_checkpoint

    root, base_url = http_dir
    _write_pdfs(root)
    storage = open_storage(settings.data_uri)
    engine = IngestEngine(settings, FakeSource(base_url), storage)
    assert engine.run() == 0
    engine.state = type(engine.state)(settings.work_dir / "fake" / "state" / "ingest.sqlite")
    engine.checkpoint()
    engine.state.close()

    elsewhere = _dc.replace(settings, work_dir=tmp_path / "other")
    restore_checkpoint(elsewhere, storage, "fake", engine.run_id)
    restored = elsewhere.work_dir / "fake" / "state" / "ingest.sqlite"
    original = settings.work_dir / "fake" / "state" / "ingest.sqlite"
    query = "SELECT doc_id, status FROM jobs ORDER BY doc_id"
    with sqlite3.connect(original) as a, sqlite3.connect(restored) as b:
        assert a.execute(query).fetchall() == b.execute(query).fetchall()
        assert len(a.execute(query).fetchall()) >= 3
