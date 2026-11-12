"""The shared ingestion engine: discover -> fetch -> render -> shard -> commit.

Threads: discovery (a pool for concurrent item checks where the source allows it),
downloaders, a shard uploader, progress and checkpoint. The main thread renders pages in a
process pool and appends them to the open shard on local disk.

Invariants (so a Spot VM can be stopped at any instant without corrupting anything):

- A shard is uploaded only after it is sealed, as one object, so storage never holds a
  partial shard.
- A document becomes `complete` only after the shard holding its pages (and its kept PDF)
  is uploaded; only complete documents and their pages are written to the catalog.
- On restart, documents staged in a shard that never sealed are redone (bounded rework).
- Catalog parts are immutable; a crash between writing a part and marking it flushed can
  duplicate rows in a later part, which consolidation removes by ID.
"""

from __future__ import annotations

import concurrent.futures as cf
import contextlib
import dataclasses
import datetime as dt
import gzip
import hashlib
import io
import json
import logging
import multiprocessing
import os
import platform
import queue
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time
from collections import deque
from pathlib import Path

import requests

from . import __version__, catalog
from .config import Settings
from .fetch import Fetcher, FetchError
from .ids import SCHEMA_VERSION, doc_id, new_run_id, page_id, split_for
from .render import RenderOptions, page_count, render_pages
from .shards import ShardWriter, new_shard_key
from .sources.base import DiscoveryContext, ItemCheck, Source
from .state import StateStore, utc_now
from .storage import Storage

LOG = logging.getLogger(__name__)
FLUSH_INTERVAL_SECONDS = 600
ERROR_RETRY_ROUNDS = 3
ITEM_FLUSH_THRESHOLD = 2000
SCRIPT_KEYS = ("bengali", "latin", "arabic", "devanagari", "other")


def human_bytes(value: float | None) -> str:
    if value is None:
        return "?"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024 or unit == "TiB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TiB"


def git_commit() -> str | None:
    if os.getenv("GIT_COMMIT"):
        return os.environ["GIT_COMMIT"]
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parent,
            capture_output=True, text=True, timeout=10, check=True,
        )
        return result.stdout.strip() or None
    except Exception:  # noqa: BLE001 - not a git checkout
        return None


def file_digests(path: Path) -> dict[str, str | int]:
    md5, sha256 = hashlib.md5(), hashlib.sha256()  # noqa: S324 - checksums, not security
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024**2):
            md5.update(chunk)
            sha256.update(chunk)
    return {"bytes": path.stat().st_size, "md5": md5.hexdigest(), "sha256": sha256.hexdigest()}


def script_fields(script: dict | None, method: str | None) -> dict:
    script = script or {}
    out = {f"script_{k}": script.get(k) for k in SCRIPT_KEYS}
    out["script_letters"] = script.get("letters")
    out["script_method"] = method if script else None
    return out


def _ignore_sigint() -> None:
    """Render workers leave Ctrl+C / SIGINT to the main process, which stops gracefully."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class RenderPool:
    """A process pool for rendering that replaces itself when a worker dies.

    If a render worker is killed (a malformed PDF crashing pdfium, or memory), Python's
    ProcessPoolExecutor becomes permanently unusable. Without a replacement, every later
    document failed at once (main run, 2026-10-24: FinePDFs stalled for 8 hours)."""

    def __init__(self, workers: int):
        self.workers = workers
        self.context = multiprocessing.get_context("spawn")
        self.lock = threading.Lock()
        self.pool = self._new()
        self.rebuilds = 0

    def _new(self) -> cf.ProcessPoolExecutor:
        return cf.ProcessPoolExecutor(self.workers, mp_context=self.context,
                                      initializer=_ignore_sigint)

    def submit(self, fn, *args):
        with self.lock:
            pool = self.pool
        try:
            return pool.submit(fn, *args)
        except cf.process.BrokenProcessPool:
            self.replace_if_broken()
            with self.lock:
                return self.pool.submit(fn, *args)

    def replace_if_broken(self) -> None:
        with self.lock:
            if not getattr(self.pool, "_broken", False):
                return
            broken, self.pool = self.pool, self._new()
            self.rebuilds += 1
        LOG.warning("A render worker died; replaced the render pool (replacement %d)",
                    self.rebuilds)
        broken.shutdown(wait=False, cancel_futures=True)

    def shutdown(self) -> None:
        with self.lock:
            self.pool.shutdown(wait=True, cancel_futures=True)


class PermanentError(Exception):
    pass


class ShardUploadError(Exception):
    """A sealed shard could not be uploaded; the run stops and is redone on restart."""


class IngestEngine:
    def __init__(self, settings: Settings, source: Source, storage: Storage,
                 cli_args: dict | None = None):
        self.settings = settings
        self.source = source
        self.storage = storage
        self.cli_args = cli_args or {}
        self.work = settings.work_dir / source.name
        self.pdf_dir = self.work / "pdfs"
        self.render_dir = self.work / "render"
        self.shard_dir = self.work / "shards"
        for directory in (self.pdf_dir, self.work / "state"):
            directory.mkdir(parents=True, exist_ok=True)

        self.state = StateStore(self.work / "state" / "ingest.sqlite")
        self.run_id = self.state.get_meta("run_id")
        if self.run_id is None:
            self.run_id = new_run_id("ingest", source.name)
            self.state.set_meta("run_id", self.run_id)
            self.state.set_meta("source", source.name)
            self.state.set_meta("started_at", utc_now())
        elif self.state.get_meta("source") != source.name:
            raise SystemExit(f"{self.work} belongs to source {self.state.get_meta('source')}")
        redo = self.state.recover_after_restart()
        if redo:
            LOG.warning("Redoing %d documents staged in a shard that never sealed", redo)
        for directory in (self.render_dir, self.shard_dir):
            shutil.rmtree(directory, ignore_errors=True)
            directory.mkdir(parents=True, exist_ok=True)

        self.render_options = RenderOptions(
            target_dpi=settings.render_dpi,
            jpeg_quality=settings.jpeg_quality, max_pixels=settings.max_page_pixels,
        )
        self.stop = threading.Event()
        self.discovery_done = threading.Event()
        self.discovery_failed = False
        self.signalled = False
        self.fetcher = Fetcher(settings, self.stop)
        self.downloaded: queue.Queue[str] = queue.Queue(maxsize=max(2, settings.download_workers))
        self.downloaders: list[threading.Thread] = []
        self.last_flush = time.monotonic()
        self.rate_samples: deque[tuple[float, int]] = deque(maxlen=60)
        self.page_shard: ShardWriter | None = None
        self.pdf_shard: ShardWriter | None = None
        self.uploader = cf.ThreadPoolExecutor(1, thread_name_prefix="shard-upload")
        self.upload_future: cf.Future | None = None
        self.flush_lock = threading.Lock()
        self.commit_lock = threading.Lock()

    # -- helpers --------------------------------------------------------------------------

    def _session(self) -> requests.Session:
        session = requests.Session()
        session.headers["User-Agent"] = self.settings.user_agent
        adapter = requests.adapters.HTTPAdapter(pool_connections=64, pool_maxsize=64)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def install_signal_handlers(self) -> None:
        def handle(signum, _frame):
            LOG.warning("Signal %s received; stopping", signum)
            self.signalled = True
            self.stop.set()

        signal.signal(signal.SIGINT, handle)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, handle)

    # -- discovery ------------------------------------------------------------------------

    def item_row(self, check: ItemCheck) -> dict:
        return {
            "source": self.source.name,
            "source_item_id": check.source_item_id,
            "status": check.status,
            **script_fields(check.script, check.script_method),
            "text_file": check.text_file,
            "pdf_count": len(check.candidates),
            "error": check.error,
            "raw_metadata": json.dumps(check.raw_metadata, ensure_ascii=False, default=str),
            "checked_at": utc_now(),
            "ingest_run_id": self.run_id,
            "schema_version": SCHEMA_VERSION,
        }

    def _record(self, check: ItemCheck) -> None:
        candidates = [
            (doc_id(self.source.name, c.source_item_id, c.source_filename), dataclasses.asdict(c))
            for c in check.candidates
        ]
        new = self.state.record_item(self.item_row(check), candidates)
        bengali = (check.script or {}).get("bengali")
        LOG.info("Item %s: %s bengali=%s docs=%d new=%d", check.source_item_id, check.status,
                 None if bengali is None else f"{bengali:.3f}", len(candidates), new)

    def _check_all(self, ctx: DiscoveryContext, item_ids) -> None:
        workers = max(1, self.settings.discovery_workers)
        with cf.ThreadPoolExecutor(workers, thread_name_prefix="check") as pool:
            in_flight: set[cf.Future] = set()
            for item_id in item_ids:
                if self.stop.is_set():
                    break
                in_flight.add(pool.submit(self.source.check, ctx, item_id))
                if len(in_flight) >= 2 * workers:
                    done, in_flight = cf.wait(in_flight, return_when=cf.FIRST_COMPLETED)
                    for future in done:
                        self._record(future.result())
            for future in cf.as_completed(in_flight):
                self._record(future.result())

    def discovery_worker(self) -> None:
        ctx = DiscoveryContext(self.settings, self._session, self.stop, self.state.item_checked)
        try:
            if self.source.parallel_discovery:
                self._check_all(ctx, self.source.iter_items(ctx))
                for attempt in range(1, ERROR_RETRY_ROUNDS + 1):
                    errored = self.state.errored_items()
                    if not errored or self.stop.wait(self.settings.error_retry_delay_seconds):
                        break
                    LOG.info("Re-checking %d errored items (round %d/%d)", len(errored),
                             attempt, ERROR_RETRY_ROUNDS)
                    self._check_all(ctx, errored)
            else:
                for check in self.source.discover(ctx):
                    self._record(check)
        except Exception:
            LOG.exception("Discovery failed; finishing queued documents, then rerun to resume")
            self.discovery_failed = True
        finally:
            self.discovery_done.set()
            LOG.info("Discovery finished")

    # -- documents ------------------------------------------------------------------------

    def doc_row(self, doc: str, candidate: dict, *, fetch: dict, digests: dict | None,
                pages: int | None, kept: tuple[str, int, int] | None,
                duplicate_of: str | None = None) -> dict:
        split = split_for(doc, self.settings.split_val, self.settings.split_test)
        return {
            "doc_id": doc,
            "source": self.source.name,
            "source_item_id": candidate["source_item_id"],
            "source_filename": candidate["source_filename"],
            "source_url": candidate["source_url"],
            "title": candidate.get("title"),
            "creator": candidate.get("creator"),
            "pub_year": candidate.get("pub_year"),
            "language": candidate.get("language"),
            **script_fields(candidate.get("script"), candidate.get("script_method")),
            "fetch_method": candidate.get("fetch_method") or "http",
            "warc_path": candidate.get("warc_path"),
            "warc_offset": candidate.get("warc_offset"),
            "warc_length": fetch.get("warc_length"),
            "source_md5": candidate.get("md5"),
            "pdf_bytes": digests and digests["bytes"],
            "pdf_md5": digests and digests["md5"],
            "pdf_sha256": digests and digests["sha256"],
            "kept_pdf_shard": kept and kept[0],
            "kept_pdf_offset": kept and kept[1],
            "kept_pdf_length": kept and kept[2],
            "page_count": pages,
            "duplicate_of": duplicate_of,
            "license_url": candidate.get("license_url"),
            "rights_raw": json.dumps(candidate.get("rights_raw") or {}, ensure_ascii=False,
                                     default=str),
            "source_metadata": json.dumps(candidate.get("source_metadata") or {},
                                          ensure_ascii=False, default=str),
            "split": split,
            "ingest_run_id": self.run_id,
            "ingested_at": utc_now(),
            "schema_version": SCHEMA_VERSION,
        }

    def _duplicate(self, doc: str, candidate: dict, original: str, fetch: dict,
                   digests: dict | None) -> None:
        row = self.doc_row(doc, candidate, fetch=fetch, digests=digests, pages=None, kept=None,
                           duplicate_of=original)
        self.state.mark_duplicate(doc, row)
        LOG.info("Duplicate PDF: %s is identical to %s; not rendered", doc, original)

    # -- download -------------------------------------------------------------------------

    def workspace_bytes(self) -> int:
        total = 0
        for root, _, files in os.walk(self.work):
            for name in files:
                with contextlib.suppress(OSError):
                    total += (Path(root) / name).stat().st_size
        return total

    def wait_for_disk(self, incoming: int) -> None:
        warned = False
        while not self.stop.is_set():
            free = shutil.disk_usage(self.work).free
            if (self.workspace_bytes() + incoming <= self.settings.disk_high_bytes
                    and free - incoming >= self.settings.min_free_bytes):
                return
            if not warned:
                LOG.warning("Waiting for disk space (free %s)", human_bytes(free))
                warned = True
            self.stop.wait(15)
        raise InterruptedError("stopped while waiting for disk")

    def attempt_limit(self, candidate: dict) -> int:
        if candidate.get("fetch_method") == "live":
            return min(self.settings.live_max_attempts, self.settings.max_attempts)
        return self.settings.max_attempts

    def _reuse_download(self, job: dict, dest: Path) -> tuple[dict, dict] | None:
        """A retried document whose PDF is still on disk from an earlier attempt (e.g. it
        was downloaded, then failed while rendering) is not fetched again, provided the file
        still has the SHA-256 recorded at download time. This matters for live websites,
        which may since have gone away."""
        if not dest.exists() or not job.get("fetch_json"):
            return None
        fetch = json.loads(job["fetch_json"])
        recorded = fetch.pop("digests", None)
        if not recorded:
            return None
        digests = file_digests(dest)
        if digests.get("sha256") != recorded.get("sha256"):
            return None
        return fetch, digests

    def download_one(self, job: dict, http: requests.Session) -> bool:
        """Fetch a document's PDF. Returns False if it was a duplicate (nothing to render)."""
        doc, candidate = job["doc_id"], job["candidate"]
        if candidate.get("md5"):
            original = self.state.seen(f"md5:{str(candidate['md5']).lower()}")
            if original and original != doc:
                self._duplicate(doc, candidate, original, {}, None)
                return False
        dest = self.pdf_dir / f"{doc}.pdf"
        reused = self._reuse_download(job, dest)
        if reused is not None:
            fetch, digests = reused
        else:
            self.wait_for_disk(candidate.get("size_bytes") or 64 * 1024**2)
            fetch = self.fetcher.fetch(candidate, dest, http)
            digests = file_digests(dest)
        original = self.state.seen(f"sha256:{digests['sha256']}")
        if original and original != doc:
            dest.unlink(missing_ok=True)
            self._duplicate(doc, candidate, original, fetch, digests)
            return False
        self.state.remember(f"sha256:{digests['sha256']}", doc)
        self.state.remember(f"md5:{digests['md5']}", doc)
        self.state.set_job(doc, status="downloaded", local_pdf=str(dest),
                           fetch_json=json.dumps({**fetch, "digests": digests}))
        return True

    def downloader_worker(self) -> None:
        http = self._session()
        try:
            while not self.stop.is_set():
                job = self.state.claim_next_job(self.settings.max_attempts)
                if job is None:
                    if self.discovery_done.is_set() and self.state.unfinished_count() == 0:
                        return
                    self.stop.wait(min(10.0, self.state.next_retry_in() or 2.0))
                    continue
                doc, candidate = job["doc_id"], job["candidate"]
                limit = self.attempt_limit(candidate)
                try:
                    if self.download_one(job, http):
                        while not self.stop.is_set():
                            try:
                                self.downloaded.put(doc, timeout=1)
                                break
                            except queue.Full:
                                continue
                except InterruptedError:
                    self.state.set_job(doc, status="pending")
                    return
                except FetchError as exc:
                    level = logging.WARNING if exc.retryable else logging.INFO
                    LOG.log(level, "Fetch failed for %s (%s): %s", doc,
                            candidate.get("fetch_method"), exc)
                    self.state.mark_failed(doc, str(exc), exc.retryable, limit,
                                           self.settings.retry_base_seconds)
                except Exception as exc:  # noqa: BLE001
                    LOG.warning("Download failed for %s: %s", doc, exc)
                    self.state.mark_failed(doc, str(exc), True, limit,
                                           self.settings.retry_base_seconds)
        finally:
            LOG.info("Downloader %s finished", threading.current_thread().name)

    # -- render and shard -----------------------------------------------------------------

    def _open_shards(self) -> None:
        if self.page_shard is None:
            sequence = self.state.next_counter("page_shard_seq")
            key = new_shard_key("pages", self.source.name, self.run_id, sequence)
            self.page_shard = ShardWriter(self.shard_dir / Path(key).name, key)
        if self.pdf_shard is None:
            sequence = self.state.next_counter("pdf_shard_seq")
            key = new_shard_key("pdfs", self.source.name, self.run_id, sequence)
            self.pdf_shard = ShardWriter(self.shard_dir / Path(key).name, key)

    def process(self, doc: str, renderers: cf.Executor) -> None:
        job = self.state.get_job(doc)
        candidate = job["candidate"]
        pdf_path = Path(job["local_pdf"])
        fetch_info = json.loads(job["fetch_json"] or "{}")
        digests = fetch_info.pop("digests", None)
        self.state.set_job(doc, status="processing")
        try:
            pages = page_count(pdf_path)
        except Exception as exc:  # noqa: BLE001 - unreadable PDF
            raise PermanentError(f"unreadable PDF: {exc}") from exc
        if pages <= 0:
            raise PermanentError("PDF has no pages")

        numbers = list(range(1, pages + 1))
        size = self.settings.batch_pages
        out_dir = self.render_dir / doc
        futures = [renderers.submit(render_pages, str(pdf_path), numbers[i:i + size],
                                    str(out_dir), self.render_options)
                   for i in range(0, pages, size)]
        infos = []
        try:
            for future in futures:
                while True:
                    try:
                        infos.extend(future.result(timeout=1))
                        break
                    except cf.TimeoutError:
                        if self.stop.is_set():
                            raise InterruptedError("stopped during rendering") from None
        finally:
            for future in futures:
                future.cancel()
        skipped = sorted(i["page_number"] for i in infos if "error" in i)
        infos = [i for i in infos if "error" not in i]
        if not infos:
            raise PermanentError(f"no page could be rendered ({len(skipped)} pages failed)")
        if skipped:
            # Recorded implicitly too: page_count is the PDF's count, so these are the gaps
            # in page_number for this document.
            LOG.warning("Skipped %d unrenderable pages of %s: %s", len(skipped), doc,
                        skipped[:50])

        # Several consumer threads may render documents at the same time; appending to
        # the open shards, staging and sealing happen one document at a time.
        with self.commit_lock:
            self._open_shards()
            split = split_for(doc, self.settings.split_val, self.settings.split_test)
            rows = []
            for info in sorted(infos, key=lambda i: i["page_number"]):
                pid = page_id(doc, info["page_number"])
                image = Path(info["path"]).read_bytes()
                offset, length = self.page_shard.add(f"{pid}.jpg", image)
                self.page_shard.add(f"{pid}.json", json.dumps(
                    {"page_id": pid, "doc_id": doc, "source": self.source.name,
                     "page_number": info["page_number"], "split": split}).encode())
                rows.append({
                    "page_id": pid,
                    "doc_id": doc,
                    "source": self.source.name,
                    "page_number": info["page_number"],
                    "shard_key": self.page_shard.key,
                    "member": f"{pid}.jpg",
                    "offset": offset,
                    "length": length,
                    "width": info["width"],
                    "height": info["height"],
                    "dpi": info["dpi"],
                    "native_dpi": info["native_dpi"],
                    "render_recipe": self.render_options.version,
                    "image_sha256": info["image_sha256"],
                    "phash": info["phash"],
                    "split": split,
                    "ingest_run_id": self.run_id,
                    "schema_version": SCHEMA_VERSION,
                })
            kept = None
            if candidate.get("store_pdf"):
                offset, length = self.pdf_shard.add(f"{doc}.pdf", pdf_path.read_bytes())
                kept = (self.pdf_shard.key, offset, length)
            row = self.doc_row(doc, candidate, fetch=fetch_info, digests=digests, pages=pages,
                               kept=kept)
            self.state.stage_document(doc, self.page_shard.key, row, rows)
            pdf_path.unlink(missing_ok=True)
            shutil.rmtree(out_dir, ignore_errors=True)
            LOG.info("Staged %s (%d pages) %s", doc, pages, candidate.get("title") or "")
            if self.page_shard.size >= self.settings.shard_target_bytes:
                self.seal()

    def seal(self) -> None:
        """Seal the open shards and upload them in the background (one upload in flight)."""
        if self.page_shard is None or self.page_shard.members == 0:
            return
        if self.upload_future is not None:
            self.upload_future.result()  # at most one shard uploading; surfaces errors
        page_shard, pdf_shard = self.page_shard, self.pdf_shard
        self.page_shard = self.pdf_shard = None
        page_shard.seal()
        if pdf_shard is not None and pdf_shard.members:
            pdf_shard.seal()
        else:
            if pdf_shard is not None:
                pdf_shard.discard()
            pdf_shard = None
        self.upload_future = self.uploader.submit(self._upload_and_commit, page_shard, pdf_shard)

    def _upload_and_commit(self, page_shard: ShardWriter, pdf_shard: ShardWriter | None) -> None:
        rows = []
        for shard, kind in ((pdf_shard, "pdfs"), (page_shard, "pages")):
            if shard is None:
                continue
            digests = file_digests(shard.path)
            for attempt in range(1, 6):
                try:
                    self.storage.put_file(shard.path, shard.key,
                                          content_type="application/x-tar", if_absent=True)
                    break
                except Exception as exc:  # noqa: BLE001
                    if attempt == 5:
                        raise ShardUploadError(f"{shard.key}: {exc}") from exc
                    LOG.warning("Shard upload failed (attempt %d/5): %s", attempt, exc)
                    time.sleep(30 * attempt)
            rows.append({"shard_key": shard.key, "kind": kind, "source": self.source.name,
                         "bytes": digests["bytes"], "sha256": digests["sha256"],
                         "md5": digests["md5"], "members": shard.members,
                         "sealed_at": utc_now(), "ingest_run_id": self.run_id,
                         "schema_version": SCHEMA_VERSION})
        documents = self.state.commit_shards(rows, page_shard.key)
        for shard in (page_shard, pdf_shard):
            if shard is not None:
                shard.path.unlink(missing_ok=True)
        LOG.info("Committed %s (%s, %d documents)", page_shard.key,
                 human_bytes(page_shard.size), documents)
        self.flush()

    def consume(self) -> None:
        """Process downloaded documents. Rendering runs in a process pool; with
        process_workers > 1, several documents are rendered at once (small documents are
        dominated by per-document overhead, so one at a time cannot keep the pool busy)."""
        workers = max(1, self.settings.process_workers)
        renderers = RenderPool(max(1, self.settings.render_workers))
        try:
            if workers == 1:
                finished = self._consume_loop(renderers)
            else:
                with cf.ThreadPoolExecutor(workers, thread_name_prefix="process") as pool:
                    futures = [pool.submit(self._consume_loop, renderers)
                               for _ in range(workers)]
                    finished = all(f.result() for f in futures)
            if finished:
                with self.commit_lock:
                    self.seal()  # final, possibly small, shard
        finally:
            renderers.shutdown()

    def _consume_loop(self, renderers: RenderPool) -> bool:
        """Returns True when all downloads are done, False when stopped."""
        try:
            while True:
                try:
                    doc = self.downloaded.get(timeout=1)
                except queue.Empty:
                    if self.stop.is_set():
                        return False
                    if not any(t.is_alive() for t in self.downloaders) and self.downloaded.empty():
                        return True
                    self.maybe_flush()
                    continue
                job = self.state.get_job(doc)
                limit = self.attempt_limit(job["candidate"])
                try:
                    self.process(doc, renderers)
                except InterruptedError:
                    self.state.set_job(doc, status="pending")
                    return False
                except ShardUploadError:
                    raise
                except cf.process.BrokenProcessPool as exc:
                    # This document was rendering when a worker died (it may have caused it).
                    # Replace the pool and count one attempt: a PDF that keeps killing
                    # workers fails permanently after max_attempts; the others succeed.
                    renderers.replace_if_broken()
                    LOG.warning("Render pool broke while processing %s; will retry", doc)
                    self.state.mark_failed(doc, f"render worker died: {exc}", True, limit,
                                           self.settings.retry_base_seconds)
                except PermanentError as exc:
                    LOG.error("Document %s failed permanently: %s", doc, exc)
                    self.state.mark_failed(doc, str(exc), False, limit,
                                           self.settings.retry_base_seconds)
                except Exception as exc:  # noqa: BLE001
                    LOG.exception("Processing failed for %s", doc)
                    self.state.mark_failed(doc, str(exc), True, limit,
                                           self.settings.retry_base_seconds)
                self.maybe_flush()
        except BaseException:
            self.stop.set()  # one consumer failing (e.g. a shard upload) stops the others
            raise

    # -- catalog, checkpoints, run records ------------------------------------------------

    def maybe_flush(self) -> None:
        items, docs = self.state.unflushed_counts()
        due = time.monotonic() - self.last_flush >= FLUSH_INTERVAL_SECONDS
        if items >= ITEM_FLUSH_THRESHOLD or (due and (items or docs)):
            self.flush()

    def flush(self) -> None:
        with self.flush_lock:
            rows = self.state.pending_catalog_rows()
            self.last_flush = time.monotonic()
            if not any(rows.values()):
                return
            part = self.state.next_counter("catalog_part")
            for table, table_rows in rows.items():
                if table_rows:
                    key = catalog.part_key(table, self.source.name, self.run_id, part)
                    self.storage.put_bytes(catalog.to_parquet_bytes(table_rows, table), key,
                                           content_type="application/vnd.apache.parquet",
                                           if_absent=True)
            self.state.mark_flushed(rows)
            LOG.info("Catalog part %05d: %s", part,
                     ", ".join(f"{t}={len(r)}" for t, r in rows.items()))
        self.write_run_record("running")

    def checkpoint(self) -> None:
        # Streamed file to file: the IA state database reached 14.5 GB, and compressing it in
        # memory took the 20 GB VM out of memory (OOM kill, main run, 2026-10-27).
        with tempfile.TemporaryDirectory(prefix="bocr-ckpt-") as temp:
            raw = Path(temp) / "state.sqlite"
            self.state.backup_to(raw)
            packed = Path(temp) / "state.sqlite.gz"
            with raw.open("rb") as src, gzip.open(packed, "wb", compresslevel=6) as dst:
                shutil.copyfileobj(src, dst, 16 * 1024**2)
            raw.unlink()
            size = packed.stat().st_size
            self.storage.put_file(packed, f"core/runs/{self.run_id}/state.sqlite.gz",
                                  content_type="application/gzip")
        LOG.info("State checkpoint uploaded (%s)", human_bytes(size))

    def write_recipe(self) -> None:
        recipe = self.render_options.recipe()
        self.storage.put_bytes(json.dumps(recipe, indent=2).encode(),
                               f"core/recipes/{recipe['render_recipe']}.json",
                               content_type="application/json", if_absent=True)

    def write_run_record(self, status: str) -> None:
        record = {
            "run_id": self.run_id,
            "stage": "ingest",
            "source": self.source.name,
            "status": status,
            "started_at": self.state.get_meta("started_at"),
            "updated_at": utc_now(),
            "package_version": __version__,
            "git_commit": git_commit(),
            "host": socket.gethostname(),
            "platform": platform.platform(),
            "render_recipe": self.render_options.version,
            "schema_version": SCHEMA_VERSION,
            "data_uri": self.settings.data_uri,
            "settings": self.settings.public_dict(),
            "args": self.cli_args,
            "stats": self.state.stats(),
        }
        body = json.dumps(record, indent=2, ensure_ascii=False, default=str).encode()
        self.storage.put_bytes(body, f"core/runs/{self.run_id}.json",
                               content_type="application/json")

    def progress_worker(self) -> None:
        while not self.stop.wait(self.settings.progress_interval_seconds):
            stats = self.state.stats()
            now, pages = time.monotonic(), stats.get("pages_committed", 0)
            self.rate_samples.append((now, pages))
            rate = 0.0
            if len(self.rate_samples) >= 2:
                (t0, p0), (t1, p1) = self.rate_samples[0], self.rate_samples[-1]
                rate = (p1 - p0) / (t1 - t0) if t1 > t0 else 0.0
            LOG.info(
                "PROGRESS items=%d | docs complete=%d staged=%d pending=%d dup=%d failed=%d "
                "| pages=%d (%s) rate=%.1f/s shards=%d | discovery=%s",
                sum(v for k, v in stats.items() if k.startswith("items_")),
                stats.get("docs_complete", 0), stats.get("docs_staged", 0),
                stats.get("docs_pending", 0), stats.get("docs_duplicate", 0),
                stats.get("docs_permanent_failed", 0), pages,
                human_bytes(stats.get("page_bytes_committed", 0)), rate,
                stats.get("shards_committed", 0),
                "done" if self.discovery_done.is_set() else "running",
            )

    def checkpoint_worker(self) -> None:
        while not self.stop.wait(self.settings.checkpoint_interval_seconds):
            try:
                self.checkpoint()
            except Exception:  # noqa: BLE001
                LOG.exception("Periodic checkpoint failed")

    # -- entry point ----------------------------------------------------------------------

    def run(self) -> int:
        LOG.info("Run %s: source=%s data=%s recipe=%s render_workers=%d download_workers=%d",
                 self.run_id, self.source.name, self.settings.data_uri,
                 self.render_options.version, self.settings.render_workers,
                 self.settings.download_workers)
        self.install_signal_handlers()
        self.write_recipe()
        self.write_run_record("running")
        self.downloaders = [
            threading.Thread(target=self.downloader_worker, name=f"download-{i}", daemon=True)
            for i in range(max(1, self.settings.download_workers))
        ]
        threads = [
            threading.Thread(target=self.discovery_worker, name="discovery", daemon=True),
            *self.downloaders,
            threading.Thread(target=self.progress_worker, name="progress", daemon=True),
            threading.Thread(target=self.checkpoint_worker, name="checkpoint", daemon=True),
        ]
        for thread in threads:
            thread.start()
        status = "failed"  # anything escaping consume() must not look like success
        try:
            self.consume()
            if self.upload_future is not None:
                self.upload_future.result()
            status = "stopped" if self.stop.is_set() else "completed"
        except Exception:
            LOG.exception("Run failed")
        finally:
            if self.signalled:
                status = "stopped"
            self.stop.set()
            for thread in threads:
                thread.join(timeout=30)
            if self.upload_future is not None and not self.upload_future.done():
                with contextlib.suppress(Exception):
                    self.upload_future.result(timeout=120)
            self.uploader.shutdown(wait=False, cancel_futures=True)
            with contextlib.suppress(Exception):
                self.flush()
            with contextlib.suppress(Exception):
                self.checkpoint()
            stats = self.state.stats()
            if status == "completed" and self.discovery_failed:
                status = "discovery_failed"
            elif status == "completed" and stats.get("docs_permanent_failed"):
                status = "completed_with_failures"
            with contextlib.suppress(Exception):
                self.write_run_record(status)
            if status.startswith("completed"):
                (self.work / "COMPLETED").write_text(f"{self.run_id} {utc_now()}\n")
            LOG.info("Run %s %s: %s", self.run_id, status, json.dumps(stats))
            self.state.close()
        return 0 if status.startswith("completed") else 1


def restore_checkpoint(settings: Settings, storage: Storage, source: str, run_id: str) -> None:
    """Recreate local state for a run from its checkpoint (e.g. on a replacement VM)."""
    target = settings.work_dir / source / "state" / "ingest.sqlite"
    if target.exists():
        raise SystemExit(f"Local state already exists at {target}; refusing to overwrite")
    data = storage.get_bytes(f"core/runs/{run_id}/state.sqlite.gz")
    if data is None:
        raise SystemExit(f"No checkpoint found for run {run_id}")
    target.parent.mkdir(parents=True, exist_ok=True)
    # Decompress in chunks: the raw database can be several times the size of memory left.
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as src, target.open("wb") as dst:
        shutil.copyfileobj(src, dst, 16 * 1024**2)
    del data
    LOG.info("Restored state for %s at %s (%s)", run_id, target,
             dt.datetime.now(dt.UTC).isoformat(timespec="seconds"))
