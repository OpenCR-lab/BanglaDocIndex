"""Fetching source PDFs (DESIGN.md section 9).

Three methods, chosen per document by its fetch spec:

- `http`     stable hosts (Internet Archive, NCTB): resumable download with size/MD5 checks.
- `cc_warc`  Common Crawl archive: one byte-range read of the WARC record at a known offset,
             gzip member decoded, HTTP payload extracted. Permanent and reproducible.
- `live`     original websites (large FinePDFs PDFs that Common Crawl truncated). Fail fast:
             short timeouts, few attempts, at most N concurrent requests and a minimum gap per
             host, and a circuit breaker that stops trying a host after repeated connection
             failures. No archive fallbacks: unreachable documents are recorded and skipped.
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import os
import threading
import time
import urllib.parse
import zlib
from pathlib import Path

import requests

LOG = logging.getLogger(__name__)
CC_BASE = "https://data.commoncrawl.org/"


class FetchError(Exception):
    def __init__(self, message: str, retryable: bool):
        super().__init__(message)
        self.retryable = retryable


def md5_of(path: Path) -> str:
    digest = hashlib.md5()  # noqa: S324 - checksum comparison, not security
    with path.open("rb") as source:
        while chunk := source.read(8 * 1024**2):
            digest.update(chunk)
    return digest.hexdigest()


def _check_pdf(path: Path) -> None:
    with path.open("rb") as handle:
        head = handle.read(1024)
    if b"%PDF-" not in head:
        path.unlink(missing_ok=True)
        raise FetchError("response is not a PDF", retryable=False)


class _Throttled(Exception):
    """Common Crawl asked us to slow down (HTTP 503 / S3 SlowDown)."""


def _read_gzip_member(chunks, stop: threading.Event) -> tuple[bytes, int]:
    """Decode one gzip member (one WARC record) from a byte stream; return the record and
    the number of compressed bytes it occupied."""
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    record = bytearray()
    consumed = 0
    for chunk in chunks:
        if stop.is_set():
            raise InterruptedError("stopped during download")
        record += decoder.decompress(chunk)
        consumed += len(chunk)
        if decoder.eof:
            return bytes(record), consumed - len(decoder.unused_data)
    raise FetchError("WARC record ended early", retryable=True)


class CommonCrawlS3:
    """Common Crawl's S3 bucket (s3://commoncrawl, Payer: BucketOwner), read with temporary
    AWS credentials obtained from this VM's Google identity (AssumeRoleWithWebIdentity), so
    no AWS key is stored anywhere. Common Crawl recommends S3 for bulk access; its HTTPS
    endpoint throttled this run to ~0.75 records/s, S3 served 22 records/s without errors."""

    METADATA_IDENTITY = ("http://metadata.google.internal/computeMetadata/v1/instance/"
                         "service-accounts/default/identity")

    def __init__(self, role_arn: str, max_connections: int, range_bytes: int):
        import boto3
        import botocore
        import botocore.config
        from botocore.credentials import RefreshableCredentials
        from botocore.session import get_session

        self.role_arn = role_arn
        self.range_bytes = range_bytes
        self.sts = boto3.client("sts", region_name="us-east-1",
                                config=botocore.config.Config(signature_version=botocore.UNSIGNED))
        session = get_session()
        session._credentials = RefreshableCredentials.create_from_metadata(
            metadata=self._credentials(), refresh_using=self._credentials,
            method="gce-web-identity")
        self.client = boto3.Session(botocore_session=session).client(
            "s3", region_name="us-east-1",
            config=botocore.config.Config(max_pool_connections=max(10, max_connections),
                                          retries={"max_attempts": 1}))

    def _credentials(self) -> dict:
        token = requests.get(self.METADATA_IDENTITY, headers={"Metadata-Flavor": "Google"},
                             params={"audience": "sts.amazonaws.com", "format": "full"},
                             timeout=10)
        token.raise_for_status()
        creds = self.sts.assume_role_with_web_identity(
            RoleArn=self.role_arn, RoleSessionName="bocr-ingest", WebIdentityToken=token.text,
            DurationSeconds=3600)["Credentials"]
        return {"access_key": creds["AccessKeyId"], "secret_key": creds["SecretAccessKey"],
                "token": creds["SessionToken"], "expiry_time": creds["Expiration"].isoformat()}

    def read_member(self, key: str, offset: int, stop: threading.Event) -> tuple[bytes, int]:
        from botocore.exceptions import BotoCoreError, ClientError

        # A bounded range first (records of untruncated documents are well under it); an
        # open-ended read only if the record turns out to be longer.
        for end in (offset + self.range_bytes - 1, None):
            byte_range = f"bytes={offset}-{end}" if end is not None else f"bytes={offset}-"
            try:
                body = self.client.get_object(Bucket="commoncrawl", Key=key,
                                              Range=byte_range)["Body"]
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if code in ("SlowDown", "Throttling", "ServiceUnavailable") or status == 503:
                    raise _Throttled() from exc
                if code in ("NoSuchKey", "InvalidRange"):
                    raise FetchError(f"Common Crawl S3 {code}", retryable=False) from exc
                # AccessDenied and the rest: configuration or transient, never the document.
                raise FetchError(f"Common Crawl S3 {code or status}", retryable=True) from exc
            except BotoCoreError as exc:
                raise FetchError(f"Common Crawl S3: {exc}", retryable=True) from exc
            try:
                return _read_gzip_member(body.iter_chunks(256 * 1024), stop)
            except FetchError:
                if end is None:
                    raise
            except BotoCoreError as exc:
                raise FetchError(f"Common Crawl S3: {exc}", retryable=True) from exc
            finally:
                body.close()
        raise FetchError("WARC record ended early", retryable=True)


class HostGate:
    """Per-host politeness and a circuit breaker for dead hosts."""

    def __init__(self, per_host: int, min_interval: float, dead_after: int):
        self.per_host = per_host
        self.min_interval = min_interval
        self.dead_after = dead_after
        self.lock = threading.Lock()
        self.semaphores: dict[str, threading.Semaphore] = {}
        self.last_request: dict[str, float] = {}
        self.failures: dict[str, int] = {}

    def is_dead(self, host: str) -> bool:
        with self.lock:
            return self.failures.get(host, 0) >= self.dead_after

    def acquire(self, host: str) -> None:
        with self.lock:
            semaphore = self.semaphores.setdefault(host, threading.Semaphore(self.per_host))
        semaphore.acquire()
        with self.lock:
            wait = self.last_request.get(host, 0.0) + self.min_interval - time.monotonic()
            self.last_request[host] = time.monotonic() + max(0.0, wait)
        if wait > 0:
            time.sleep(wait)

    def release(self, host: str, connected: bool) -> None:
        with self.lock:
            self.failures[host] = 0 if connected else self.failures.get(host, 0) + 1
            semaphore = self.semaphores[host]
        semaphore.release()


class Fetcher:
    def __init__(self, settings, stop_event: threading.Event):
        self.settings = settings
        self.stop = stop_event
        self.gate = HostGate(settings.live_per_host, settings.live_min_interval_seconds,
                             settings.live_dead_host_failures)
        # Common Crawl: shared concurrency cap and a shared cooldown after a 503 "SlowDown",
        # so the whole process slows down together instead of every thread hammering it.
        self.cc_slots = threading.BoundedSemaphore(max(1, settings.cc_max_concurrency))
        self.cc_lock = threading.Lock()
        self.cc_resume_at = 0.0
        self.cc_backoff = 0.0
        self.cc_s3 = (CommonCrawlS3(settings.cc_s3_role_arn, settings.cc_max_concurrency,
                                    settings.cc_s3_range_bytes)
                      if settings.cc_s3_role_arn else None)

    def fetch(self, candidate: dict, dest: Path, http: requests.Session) -> dict:
        method = candidate.get("fetch_method") or "http"
        if method == "cc_warc":
            return self.fetch_cc_warc(candidate, dest, http)
        if method == "live":
            return self.fetch_live(candidate, dest, http)
        return self.fetch_http(candidate, dest, http)

    # -- stable hosts ----------------------------------------------------------------------

    def fetch_http(self, candidate: dict, dest: Path, http: requests.Session) -> dict:
        partial = dest.with_suffix(dest.suffix + ".partial")
        expected = candidate.get("size_bytes")
        if not dest.exists():
            existing = partial.stat().st_size if partial.exists() else 0
            headers = {"Range": f"bytes={existing}-"} if existing else {}
            try:
                with http.get(candidate["source_url"], headers=headers, stream=True,
                              timeout=(self.settings.connect_timeout, self.settings.read_timeout),
                              allow_redirects=True) as response:
                    if response.status_code == 416 and existing and existing == expected:
                        partial.replace(dest)
                    else:
                        if response.status_code in (400, 401, 403, 404, 410, 451):
                            raise FetchError(f"HTTP {response.status_code}", retryable=False)
                        response.raise_for_status()
                        append = existing > 0 and response.status_code == 206
                        self._stream_to(response, partial, append)
                        partial.replace(dest)
            except requests.RequestException as exc:
                raise FetchError(str(exc), retryable=True) from exc
        _check_pdf(dest)
        size = dest.stat().st_size
        if expected and size != expected:
            dest.unlink()
            raise FetchError(f"size mismatch: expected {expected}, got {size}", retryable=True)
        if candidate.get("md5") and md5_of(dest) != str(candidate["md5"]).lower():
            dest.unlink()
            raise FetchError("md5 mismatch", retryable=True)
        return {}

    def _stream_to(self, response: requests.Response, path: Path, append: bool,
                   limit: int | None = None) -> None:
        written = path.stat().st_size if append and path.exists() else 0
        with path.open("ab" if append else "wb") as target:
            for chunk in response.iter_content(self.settings.download_chunk_bytes):
                if self.stop.is_set():
                    raise InterruptedError("stopped during download")
                target.write(chunk)
                written += len(chunk)
                if limit and written > limit:
                    raise FetchError(f"larger than {limit} bytes", retryable=False)
            target.flush()
            os.fsync(target.fileno())

    # -- Common Crawl archive ---------------------------------------------------------------

    def _cc_wait(self) -> None:
        while True:
            with self.cc_lock:
                remaining = self.cc_resume_at - time.monotonic()
            if remaining <= 0:
                return
            if self.stop.wait(min(remaining, 5)):
                raise InterruptedError("stopped while Common Crawl was throttling")

    def _cc_throttled(self) -> None:
        with self.cc_lock:
            if time.monotonic() < self.cc_resume_at:
                return  # already pausing: requests that were in flight do not escalate it
            self.cc_backoff = min(max(self.cc_backoff * 2, 5.0),
                                  float(self.settings.cc_max_backoff_seconds))
            self.cc_resume_at = max(self.cc_resume_at, time.monotonic() + self.cc_backoff)
            backoff = self.cc_backoff
        LOG.info("Common Crawl throttling (503); pausing its requests for %.0f s", backoff)

    def fetch_cc_warc(self, candidate: dict, dest: Path, http: requests.Session) -> dict:
        """Read one WARC record. A 503 is Common Crawl asking clients to slow down, not a
        property of the document, so it is retried here (after the shared cooldown) instead
        of using up the document's attempts. The bytes are the same over S3 or HTTPS; only
        the transport differs."""
        key = str(candidate["warc_path"]).removeprefix("s3://commoncrawl/")
        offset = int(candidate["warc_offset"])
        for _ in range(max(1, self.settings.cc_throttle_retries)):
            self._cc_wait()
            try:
                with self.cc_slots:
                    if self.cc_s3 is not None:
                        record, consumed = self.cc_s3.read_member(key, offset, self.stop)
                    else:
                        record, consumed = self._cc_read_https(key, offset, http)
            except _Throttled:
                self._cc_throttled()
                continue
            with self.cc_lock:
                self.cc_backoff = 0.0
            payload = extract_warc_payload(record)
            dest.write_bytes(payload)
            _check_pdf(dest)
            return {"warc_length": consumed}
        raise FetchError("Common Crawl kept throttling (503)", retryable=True)

    def _cc_read_https(self, key: str, offset: int, http: requests.Session) -> tuple[bytes, int]:
        try:
            with http.get(CC_BASE + key, headers={"Range": f"bytes={offset}-"}, stream=True,
                          timeout=(self.settings.connect_timeout, self.settings.read_timeout)
                          ) as response:
                if response.status_code in (429, 503):
                    raise _Throttled()
                if response.status_code in (403, 404, 416):
                    raise FetchError(f"Common Crawl HTTP {response.status_code}", False)
                response.raise_for_status()
                return _read_gzip_member(response.iter_content(256 * 1024), self.stop)
        except requests.RequestException as exc:
            raise FetchError(str(exc), retryable=True) from exc

    # -- live websites -------------------------------------------------------------------------

    def fetch_live(self, candidate: dict, dest: Path, http: requests.Session) -> dict:
        url = candidate["source_url"]
        host = urllib.parse.urlsplit(url).hostname or ""
        if self.gate.is_dead(host):
            raise FetchError(f"host {host} unreachable (circuit open)", retryable=False)
        self.gate.acquire(host)
        connected = False
        partial = dest.with_suffix(dest.suffix + ".partial")
        try:
            with http.get(url, stream=True, allow_redirects=True,
                          timeout=(self.settings.live_connect_timeout,
                                   self.settings.live_read_timeout)) as response:
                connected = True
                if 400 <= response.status_code < 500:
                    raise FetchError(f"HTTP {response.status_code}", retryable=False)
                response.raise_for_status()
                self._stream_to(response, partial, append=False,
                                limit=self.settings.live_max_bytes)
            partial.replace(dest)
        except (requests.exceptions.ConnectionError, requests.exceptions.SSLError) as exc:
            # DNS failure, refused or reset connection, bad certificate: not worth retrying.
            raise FetchError(f"connection failed: {type(exc).__name__}", retryable=False) from exc
        except requests.exceptions.Timeout as exc:
            raise FetchError("timeout", retryable=True) from exc
        except requests.RequestException as exc:
            raise FetchError(str(exc)[:200], retryable=True) from exc
        finally:
            self.gate.release(host, connected)
            partial.unlink(missing_ok=True)
        _check_pdf(dest)
        return {}


def _dechunk(body: bytes) -> bytes:
    out, position = bytearray(), 0
    while True:
        line_end = body.find(b"\r\n", position)
        if line_end < 0:
            break
        size = int(body[position:line_end].split(b";")[0].strip() or b"0", 16)
        if size == 0:
            break
        start = line_end + 2
        out += body[start:start + size]
        position = start + size + 2
    return bytes(out)


def extract_warc_payload(record: bytes) -> bytes:
    """HTTP response body from one decompressed WARC 'response' record."""
    warc_head, sep, rest = record.partition(b"\r\n\r\n")
    if not sep or not warc_head.startswith(b"WARC/"):
        raise FetchError("not a WARC record", retryable=False)
    if b"WARC-Type: response" not in warc_head:
        raise FetchError("WARC record is not an HTTP response", retryable=False)
    for line in warc_head.split(b"\r\n"):
        if line.lower().startswith(b"content-length:"):
            rest = rest[:int(line.split(b":", 1)[1])]  # drop the record separator
            break
    http_head, sep, body = rest.partition(b"\r\n\r\n")
    if not sep:
        raise FetchError("WARC record has no HTTP body", retryable=False)
    headers = {}
    for line in http_head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        headers[name.strip().lower()] = value.strip().lower()
    if headers.get(b"transfer-encoding") == b"chunked":
        body = _dechunk(body)
    if headers.get(b"content-encoding") in (b"gzip", b"x-gzip"):
        body = gzip.decompress(body)
    start = body.find(b"%PDF-", 0, 1024)
    if start < 0:
        raise FetchError("WARC payload is not a PDF", retryable=False)
    return body[start:]
