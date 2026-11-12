"""Runtime settings, read from environment variables (optionally loaded from a .env file)."""

from __future__ import annotations

import dataclasses
import os
from pathlib import Path


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value in (None, "") else value.strip()


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value in (None, "") else int(value)


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return default if value in (None, "") else float(value)


@dataclasses.dataclass(frozen=True)
class Settings:
    # Where the dataset lives: "gs://<bucket>" or a local directory (for tests/dev).
    data_uri: str
    # Local scratch space for state, downloaded PDFs, rendered pages and logs.
    work_dir: Path

    # Rendering (see DESIGN.md section 9.2).
    render_dpi: int = 150
    max_page_pixels: int = 8_000_000
    jpeg_quality: int = 88
    batch_pages: int = 8
    render_workers: int = 2
    process_workers: int = 1  # documents rendered at the same time
    upload_workers: int = 4
    # Concurrent source-item checks (network bound) and concurrent PDF downloads.
    discovery_workers: int = 8
    download_workers: int = 4

    # Reliability and bookkeeping.
    max_attempts: int = 5
    retry_base_seconds: int = 30
    # Wait before re-checking source items whose check failed (transient errors).
    error_retry_delay_seconds: int = 900
    catalog_flush_docs: int = 100
    checkpoint_interval_seconds: int = 1800
    progress_interval_seconds: int = 30
    connect_timeout: int = 20
    read_timeout: int = 180
    download_chunk_bytes: int = 4 * 1024**2

    # Shards (DESIGN.md section 6): sealed once this size is reached at a document boundary.
    shard_target_bytes: int = 1024**3

    # Live websites (large FinePDFs PDFs): fail fast, be polite, give up on dead hosts.
    live_per_host: int = 2
    live_min_interval_seconds: float = 1.0
    live_dead_host_failures: int = 5
    live_connect_timeout: int = 10
    live_read_timeout: int = 60
    live_max_attempts: int = 2
    live_max_bytes: int = 1024**3

    # Common Crawl (FinePDFs cc_warc): data.commoncrawl.org answers 503 "SlowDown" when
    # clients send too many requests. Cap concurrency and back off together on a 503.
    # Measured in the main run: 3 and 6 concurrent requests gave the same ~0.75 documents/s,
    # but 3 drew a third as many 503s, so 3 is the default.
    cc_max_concurrency: int = 3
    cc_max_backoff_seconds: int = 300
    cc_throttle_retries: int = 12
    # Read Common Crawl from S3 instead of HTTPS when an AWS role is set (DESIGN.md 9.3).
    cc_s3_role_arn: str = ""
    cc_s3_range_bytes: int = 2_000_000

    # Disk backpressure for the local work directory.
    disk_high_bytes: int = 150 * 1024**3
    min_free_bytes: int = 20 * 1024**3

    # Document-level split fractions; the rest is train (DESIGN.md section 9.4).
    split_val: float = 0.05
    split_test: float = 0.05

    user_agent: str = "bocr-scrape/0.1 (Bangla OCR dataset research)"

    @classmethod
    def from_env(cls) -> Settings:
        data_uri = _env_str("DATA_URI", "")
        if not data_uri:
            raise SystemExit("DATA_URI is required (e.g. gs://bangla-ocr-dataset or ./local-data)")
        gib = 1024**3
        return cls(
            data_uri=data_uri,
            work_dir=Path(_env_str("WORK_DIR", str(Path.home() / "bocr-work"))).expanduser(),
            render_dpi=_env_int("RENDER_DPI", 150),
            max_page_pixels=_env_int("MAX_PAGE_PIXELS", 8_000_000),
            jpeg_quality=_env_int("JPEG_QUALITY", 88),
            batch_pages=_env_int("BATCH_PAGES", 8),
            render_workers=_env_int("RENDER_WORKERS", max(1, os.cpu_count() or 1)),
            process_workers=_env_int("PROCESS_WORKERS", 1),
            upload_workers=_env_int("UPLOAD_WORKERS", 4),
            discovery_workers=_env_int("DISCOVERY_WORKERS", 8),
            download_workers=_env_int("DOWNLOAD_WORKERS", 4),
            max_attempts=_env_int("MAX_ATTEMPTS", 5),
            retry_base_seconds=_env_int("RETRY_BASE_SECONDS", 30),
            error_retry_delay_seconds=_env_int("ERROR_RETRY_DELAY_SECONDS", 900),
            catalog_flush_docs=_env_int("CATALOG_FLUSH_DOCS", 100),
            checkpoint_interval_seconds=_env_int("CHECKPOINT_INTERVAL_SECONDS", 1800),
            progress_interval_seconds=_env_int("PROGRESS_INTERVAL_SECONDS", 30),
            connect_timeout=_env_int("REQUEST_CONNECT_TIMEOUT", 20),
            read_timeout=_env_int("REQUEST_READ_TIMEOUT", 180),
            download_chunk_bytes=_env_int("DOWNLOAD_CHUNK_MIB", 4) * 1024**2,
            shard_target_bytes=int(_env_float("SHARD_TARGET_GB", 1.0) * gib),
            live_per_host=_env_int("LIVE_PER_HOST", 2),
            live_min_interval_seconds=_env_float("LIVE_MIN_INTERVAL_SECONDS", 1.0),
            live_dead_host_failures=_env_int("LIVE_DEAD_HOST_FAILURES", 5),
            live_connect_timeout=_env_int("LIVE_CONNECT_TIMEOUT", 10),
            live_read_timeout=_env_int("LIVE_READ_TIMEOUT", 60),
            live_max_attempts=_env_int("LIVE_MAX_ATTEMPTS", 2),
            live_max_bytes=int(_env_float("LIVE_MAX_GB", 1.0) * gib),
            cc_max_concurrency=_env_int("CC_MAX_CONCURRENCY", 3),
            cc_max_backoff_seconds=_env_int("CC_MAX_BACKOFF_SECONDS", 300),
            cc_throttle_retries=_env_int("CC_THROTTLE_RETRIES", 12),
            cc_s3_role_arn=_env_str("CC_S3_ROLE_ARN", ""),
            cc_s3_range_bytes=_env_int("CC_S3_RANGE_BYTES", 2_000_000),
            disk_high_bytes=int(_env_float("DISK_HIGH_WATER_GB", 150) * gib),
            min_free_bytes=int(_env_float("MIN_FREE_DISK_GB", 20) * gib),
            split_val=_env_float("SPLIT_VAL_FRACTION", 0.05),
            split_test=_env_float("SPLIT_TEST_FRACTION", 0.05),
            user_agent=_env_str("USER_AGENT", "bocr-scrape/0.1 (Bangla OCR dataset research)"),
        )

    def public_dict(self) -> dict:
        """Settings recorded in run records (nothing secret lives here)."""
        out = dataclasses.asdict(self)
        out["work_dir"] = str(self.work_dir)
        return out
