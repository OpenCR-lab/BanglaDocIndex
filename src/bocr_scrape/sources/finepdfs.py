"""FinePDFs (Hugging Face) Bengali subset as a document index (DESIGN.md section 9.3).

FinePDFs publishes extracted text and pointers, not PDFs. We use it only as an index:

1. Build a manifest once from the `ben_Beng` parquet files (train and test): every column
   except the text, plus the script composition of each document's text, computed in
   passing and then discarded. The manifest is kept under core/sources/finepdfs/.
2. Fetch each PDF from where its complete bytes exist:
   - not truncated in Common Crawl -> one byte-range read of the WARC record (`cc_warc`);
     Common Crawl is permanent, so these PDFs are not stored;
   - truncated (Common Crawl kept only ~1 MB) -> the original website (`live`), fail fast,
     no archive fallbacks; these PDFs are kept, because websites change.

Documents hosted on archive.org are skipped: they come from the Internet Archive source.
Rows are visited in a fixed pseudo-random order (hash of the id) so requests to the same
government host are spread out instead of arriving in bursts.
"""

from __future__ import annotations

import hashlib
import logging
import urllib.parse
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ..scripts import script_composition
from .base import DiscoveryContext, DocCandidate, ItemCheck, Source

LOG = logging.getLogger(__name__)

HF_REPO = "HuggingFaceFW/finepdfs"
HF_SUBSET = "data/ben_Beng"
SCRIPT_METHOD = "finepdfs_text_full"
META_COLUMNS = (
    "id", "dump", "url", "date", "file_path", "offset", "token_count", "language",
    "page_average_lid", "page_average_lid_score", "full_doc_lid", "full_doc_lid_score",
    "per_page_languages", "is_truncated", "extractor", "ocr_quality_scores",
    "minhash_cluster_size", "duplicate_count",
)
RIGHTS = {
    "index_dataset": "HuggingFaceFW/finepdfs (ben_Beng)",
    "index_license": "ODC-By-1.0",
    "content_terms": "Common Crawl Terms of Use; original copyright of the PDF applies",
}


def order_key(doc: str) -> str:
    return hashlib.sha256(f"order\n{doc}".encode()).hexdigest()[:16]


def download_subset(dest_dir: Path, ca_bundle: str | None = None) -> list[Path]:
    """Download the subset's parquet files (about 4 GB) with resume and retries.

    Streaming them over HTTP inside DuckDB works until one ranged request fails mid-way;
    local files make the manifest build restartable and deterministic.
    """
    import time

    import requests

    session = requests.Session()
    if ca_bundle:
        session.verify = ca_bundle
    api = f"https://huggingface.co/api/datasets/{HF_REPO}/tree/main/{HF_SUBSET}"
    files: list[str] = []
    for split in ("train", "test"):
        listing = session.get(f"{api}/{split}", timeout=60)
        listing.raise_for_status()
        files += [f["path"] for f in listing.json() if f["path"].endswith(".parquet")]
    paths = []
    for name in sorted(files):
        target = dest_dir / name
        target.parent.mkdir(parents=True, exist_ok=True)
        url = f"https://huggingface.co/datasets/{HF_REPO}/resolve/main/{name}"
        for attempt in range(1, 11):
            try:
                head = session.head(url, allow_redirects=True, timeout=60)
                head.raise_for_status()
                size = int(head.headers.get("content-length", 0))
                if target.exists() and size and target.stat().st_size == size:
                    break
                have = target.stat().st_size if target.exists() else 0
                headers = {"Range": f"bytes={have}-"} if have else {}
                with session.get(url, headers=headers, stream=True, timeout=(20, 300)) as r:
                    r.raise_for_status()
                    with target.open("ab" if have and r.status_code == 206 else "wb") as out:
                        for chunk in r.iter_content(8 * 1024**2):
                            out.write(chunk)
                if not size or target.stat().st_size == size:
                    break
            except requests.RequestException as exc:
                LOG.warning("Download of %s failed (attempt %d): %s", name, attempt, exc)
                time.sleep(min(300, 10 * attempt))
        else:
            raise RuntimeError(f"could not download {name}")
        paths.append(target)
        LOG.info("FinePDFs subset file ready: %s", name)
    return paths


def build_manifest(dest: Path, source_glob: str | None = None, limit: int = 0,
                   ca_cert_file: str | None = None) -> Path:
    """Write a text-free manifest with script composition (streams the text once)."""
    import duckdb

    if source_glob is None:
        cache = dest.parent / "hf-cache"
        download_subset(cache, ca_cert_file)
        source_glob = str(cache / HF_SUBSET / "*" / "*.parquet")
    con = duckdb.connect()
    columns = ", ".join(f'"{c}"' for c in META_COLUMNS)
    query = f"SELECT {columns}, len(page_ends) AS pages, text FROM read_parquet('{source_glob}')"
    if limit:
        query += f" LIMIT {int(limit)}"
    reader = con.sql(query).fetch_record_batch(20_000)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".partial")
    writer = None
    rows = 0
    for batch in reader:
        texts = batch.column("text").to_pylist()
        scripts = [script_composition(t or "") for t in texts]
        table = pa.Table.from_batches([batch]).drop_columns(["text"])
        for name in ("bengali", "latin", "arabic", "devanagari", "other"):
            table = table.append_column(f"script_{name}",
                                        pa.array([s[name] for s in scripts], pa.float64()))
        table = table.append_column("script_letters",
                                    pa.array([s["letters"] for s in scripts], pa.int64()))
        table = table.append_column("order_key", pa.array(
            [order_key(i) for i in table.column("id").to_pylist()], pa.string()))
        if writer is None:
            writer = pq.ParquetWriter(tmp, table.schema, compression="zstd")
        writer.write_table(table)
        rows += table.num_rows
        LOG.info("FinePDFs manifest: %d documents", rows)
    if writer is None:
        raise RuntimeError("FinePDFs returned no rows")
    writer.close()
    # Sort once by order_key so discovery can stream it in a stable, host-mixing order.
    sorted_table = pq.read_table(tmp).sort_by("order_key")
    pq.write_table(sorted_table, dest, compression="zstd")
    tmp.unlink()
    if source_glob.startswith(str(dest.parent / "hf-cache")):
        import shutil

        shutil.rmtree(dest.parent / "hf-cache", ignore_errors=True)
    return dest


def _host(url: str) -> str:
    return (urllib.parse.urlsplit(url).hostname or "").lower()


class FinePDFsSource(Source):
    name = "finepdfs"

    def __init__(self, manifest: Path, max_items: int = 0, include_live: bool = True):
        self.manifest = manifest
        self.max_items = max_items
        self.include_live = include_live

    def candidate(self, row: dict) -> DocCandidate:
        truncated = bool(row["is_truncated"])
        script = {k.removeprefix("script_"): row[k] for k in row if k.startswith("script_")}
        metadata = {k: row[k] for k in (
            "dump", "date", "extractor", "pages", "language", "page_average_lid",
            "page_average_lid_score", "full_doc_lid", "full_doc_lid_score",
            "per_page_languages", "token_count", "ocr_quality_scores",
            "minhash_cluster_size", "duplicate_count", "is_truncated")}
        metadata["host"] = _host(row["url"])
        return DocCandidate(
            source_item_id=row["id"],
            source_filename="document.pdf",
            source_url=row["url"],
            fetch_method="live" if truncated else "cc_warc",
            warc_path=row["file_path"],
            warc_offset=int(row["offset"]),
            script=script,
            script_method=SCRIPT_METHOD,
            rights_raw={**RIGHTS, "host": metadata["host"]},
            source_metadata=metadata,
            store_pdf=truncated,
        )

    def discover(self, ctx: DiscoveryContext) -> Iterator[ItemCheck]:
        parquet = pq.ParquetFile(self.manifest)
        examined = 0
        for batch in parquet.iter_batches(batch_size=10_000):
            for row in batch.to_pylist():
                if ctx.stop_event.is_set():
                    return
                item_id = row["id"]
                if self.max_items and examined >= self.max_items:
                    LOG.info("FinePDFs discovery stopped at max_items=%s", self.max_items)
                    return
                examined += 1  # counts items checked before a restart, too
                if ctx.already_checked(item_id):
                    continue
                raw = {k: v for k, v in row.items() if k != "order_key"}
                candidate = self.candidate(row)
                if "archive.org" in candidate.source_metadata["host"]:
                    yield ItemCheck(item_id, "skipped", error="hosted on archive.org (IA source)",
                                    raw_metadata=raw)
                elif candidate.fetch_method == "live" and not self.include_live:
                    yield ItemCheck(item_id, "skipped", error="live fetch disabled",
                                    raw_metadata=raw)
                else:
                    yield ItemCheck(item_id, "accepted", script=candidate.script,
                                    script_method=SCRIPT_METHOD, raw_metadata=raw,
                                    candidates=[candidate])
