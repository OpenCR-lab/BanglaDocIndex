"""Local SQLite state for one ingestion run: work queue, retries, resume, open shards, and
catalog rows produced but not yet written to the catalog.

This is operational state only. The catalog in object storage is the dataset record.

Document lifecycle:
    pending -> downloading -> downloaded -> processing -> staged -> complete
                                                        \\-> duplicate (identical PDF seen)
    any step -> retryable_failed -> ... -> permanent_failed

`staged` means the document's pages (and kept PDF) are in the open local shard. Only when
that shard is sealed and uploaded do its documents become `complete`. On restart, staged
documents of the unsealed shard are reset to `pending` and redone: a bounded amount of
rework that keeps every committed shard complete and every catalog row truthful.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS items (
    source_item_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    row_json TEXT NOT NULL,
    flushed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS jobs (
    doc_id TEXT PRIMARY KEY,
    source_item_id TEXT NOT NULL,
    candidate_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_epoch REAL NOT NULL DEFAULT 0,
    local_pdf TEXT,
    fetch_json TEXT,
    page_count INTEGER,
    shard_key TEXT,
    error TEXT,
    doc_row_json TEXT,
    flushed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(status, next_attempt_epoch, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_shard ON jobs(shard_key, status);
-- Partial indexes over rows not yet written to the catalog: unflushed_counts() runs after every
-- document, and without these it scanned every item (540k rows for FinePDFs) under the lock.
CREATE INDEX IF NOT EXISTS idx_items_unflushed ON items(flushed) WHERE flushed=0;
CREATE INDEX IF NOT EXISTS idx_jobs_unflushed ON jobs(status) WHERE flushed=0;
-- Claimable jobs in claim order. The planner prefers idx_jobs_claim and then sorts every
-- pending job (500 ms per claim with 540k FinePDFs jobs), so claim_next_job names this index.
CREATE INDEX IF NOT EXISTS idx_jobs_ready ON jobs(created_at, doc_id)
    WHERE status IN ('pending','retryable_failed');

CREATE TABLE IF NOT EXISTS page_rows (
    page_id TEXT PRIMARY KEY,
    doc_id TEXT NOT NULL,
    image_bytes INTEGER NOT NULL,
    row_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_page_rows_doc ON page_rows(doc_id);

CREATE TABLE IF NOT EXISTS shards (
    shard_key TEXT PRIMARY KEY,
    row_json TEXT NOT NULL,
    flushed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS seen_pdfs (
    digest TEXT PRIMARY KEY,  -- 'md5:<hex>' or 'sha256:<hex>'
    doc_id TEXT NOT NULL
);
"""

DONE = ("complete", "duplicate", "permanent_failed")


class StateStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False, timeout=60)
        self.conn.row_factory = sqlite3.Row
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.executescript(SCHEMA)
            self.conn.commit()

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    # -- meta -------------------------------------------------------------------------

    def get_meta(self, key: str) -> str | None:
        with self.lock:
            row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return None if row is None else row["value"]

    def set_meta(self, key: str, value: str) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    def next_counter(self, name: str) -> int:
        with self.lock:
            current = int(self.get_meta(name) or "0")
            self.set_meta(name, str(current + 1))
            return current

    # -- discovery ----------------------------------------------------------------------

    def item_checked(self, source_item_id: str) -> bool:
        """True once an item has a definite outcome. Items that errored are retried."""
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM items WHERE source_item_id=? AND status!='error'",
                (source_item_id,),
            ).fetchone()
            return row is not None

    def errored_items(self) -> list[str]:
        with self.lock:
            return [r["source_item_id"] for r in self.conn.execute(
                "SELECT source_item_id FROM items WHERE status='error' ORDER BY source_item_id")]

    def record_item(self, row: dict, candidates: list[tuple[str, dict]]) -> int:
        """Store an item check and its document jobs atomically. Returns new job count."""
        now = utc_now()
        inserted = 0
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO items(source_item_id, status, row_json, flushed) "
                "VALUES (?,?,?,0)",
                (row["source_item_id"], row["status"], json.dumps(row, ensure_ascii=False)),
            )
            for doc_id, candidate in candidates:
                cursor = self.conn.execute(
                    "INSERT OR IGNORE INTO jobs(doc_id, source_item_id, candidate_json, "
                    "created_at, updated_at) VALUES (?,?,?,?,?)",
                    (doc_id, row["source_item_id"], json.dumps(candidate, ensure_ascii=False),
                     now, now),
                )
                inserted += cursor.rowcount
        return inserted

    # -- jobs ---------------------------------------------------------------------------

    def recover_after_restart(self) -> int:
        """Reset in-flight work, including documents staged in a shard that never sealed.

        The interrupted attempt is refunded: it was counted when the job was claimed but did
        not fail. Without the refund, a job claimed on its last attempt came back as 'pending'
        with no attempts left, which nothing could claim, so the run never finished (main run,
        2026-10-25)."""
        with self.lock, self.conn:
            staged = [r["doc_id"] for r in self.conn.execute(
                "SELECT doc_id FROM jobs WHERE status='staged'")]
            self.conn.executemany("DELETE FROM page_rows WHERE doc_id=?", [(d,) for d in staged])
            self.conn.execute(
                "UPDATE jobs SET status='pending', shard_key=NULL, doc_row_json=NULL, "
                "attempts=MAX(attempts - 1, 0), updated_at=? "
                "WHERE status IN ('downloading','downloaded','processing','staged')",
                (utc_now(),),
            )
            return len(staged)

    def claim_next_job(self, max_attempts: int) -> dict[str, Any] | None:
        with self.lock, self.conn:
            row = self.conn.execute(
                "SELECT * FROM jobs INDEXED BY idx_jobs_ready "
                "WHERE status IN ('pending','retryable_failed') "
                "AND attempts < ? AND next_attempt_epoch <= ? "
                "ORDER BY created_at, doc_id LIMIT 1",
                (max_attempts, time.time()),
            ).fetchone()
            if row is None:
                return None
            self.conn.execute(
                "UPDATE jobs SET status='downloading', attempts=attempts+1, error=NULL, "
                "updated_at=? WHERE doc_id=?",
                (utc_now(), row["doc_id"]),
            )
            job = dict(row)
            job["attempts"] += 1
            job["candidate"] = json.loads(job.pop("candidate_json"))
            return job

    def set_job(self, doc_id: str, **fields: Any) -> None:
        fields["updated_at"] = utc_now()
        columns = ", ".join(f"{k}=?" for k in fields)
        with self.lock, self.conn:
            self.conn.execute(
                f"UPDATE jobs SET {columns} WHERE doc_id=?", (*fields.values(), doc_id)
            )

    def get_job(self, doc_id: str) -> dict[str, Any]:
        with self.lock:
            row = self.conn.execute("SELECT * FROM jobs WHERE doc_id=?", (doc_id,)).fetchone()
            if row is None:
                raise KeyError(doc_id)
            job = dict(row)
            job["candidate"] = json.loads(job.pop("candidate_json"))
            return job

    def mark_failed(self, doc_id: str, error: str, retryable: bool, max_attempts: int,
                    base_delay: float = 30) -> None:
        job = self.get_job(doc_id)
        permanent = not retryable or job["attempts"] >= max_attempts
        delay = 0 if permanent else min(3600, base_delay * 2 ** max(0, job["attempts"] - 1))
        self.set_job(
            doc_id,
            status="permanent_failed" if permanent else "retryable_failed",
            error=error[:4000],
            next_attempt_epoch=time.time() + delay,
        )

    def reset_permanent_failures(self) -> int:
        with self.lock, self.conn:
            cursor = self.conn.execute(
                "UPDATE jobs SET status='pending', attempts=0, next_attempt_epoch=0, error=NULL, "
                "updated_at=? WHERE status='permanent_failed'",
                (utc_now(),),
            )
            return cursor.rowcount

    def unfinished_count(self) -> int:
        """Documents still needing download or rendering (staged ones only await a seal)."""
        with self.lock:
            row = self.conn.execute(
                "SELECT COUNT(*) n FROM jobs WHERE status NOT IN "
                "('complete','duplicate','permanent_failed','staged')"
            ).fetchone()
            return int(row["n"])

    def next_retry_in(self) -> float | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT MIN(next_attempt_epoch) t FROM jobs WHERE status='retryable_failed'"
            ).fetchone()
            return None if row["t"] is None else max(0.0, row["t"] - time.time())

    # -- duplicates -----------------------------------------------------------------------

    def seen(self, digest: str) -> str | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT doc_id FROM seen_pdfs WHERE digest=?", (digest,)).fetchone()
            return None if row is None else row["doc_id"]

    def remember(self, digest: str, doc_id: str) -> None:
        with self.lock, self.conn:
            self.conn.execute("INSERT OR IGNORE INTO seen_pdfs(digest, doc_id) VALUES (?,?)",
                              (digest, doc_id))

    def mark_duplicate(self, doc_id: str, doc_row: dict) -> None:
        now = utc_now()
        with self.lock, self.conn:
            self.conn.execute(
                "UPDATE jobs SET status='duplicate', doc_row_json=?, local_pdf=NULL, "
                "updated_at=?, completed_at=? WHERE doc_id=?",
                (json.dumps(doc_row, ensure_ascii=False), now, now, doc_id),
            )

    # -- staging and sealing --------------------------------------------------------------

    def stage_document(self, doc_id: str, shard_key: str, doc_row: dict,
                       page_rows: list[dict]) -> None:
        """Pages are in the open local shard; they become real only when it is committed."""
        with self.lock, self.conn:
            self.conn.executemany(
                "INSERT OR REPLACE INTO page_rows(page_id, doc_id, image_bytes, row_json) "
                "VALUES (?,?,?,?)",
                [(r["page_id"], doc_id, r["length"], json.dumps(r, ensure_ascii=False))
                 for r in page_rows],
            )
            self.conn.execute(
                "UPDATE jobs SET status='staged', shard_key=?, doc_row_json=?, local_pdf=NULL, "
                "page_count=?, updated_at=? WHERE doc_id=?",
                (shard_key, json.dumps(doc_row, ensure_ascii=False), len(page_rows), utc_now(),
                 doc_id),
            )

    def commit_shards(self, shard_rows: list[dict], staged_shard_key: str) -> int:
        """After upload: record the shards and complete the documents staged in them."""
        now = utc_now()
        with self.lock, self.conn:
            self.conn.executemany(
                "INSERT OR REPLACE INTO shards(shard_key, row_json, flushed) VALUES (?,?,0)",
                [(r["shard_key"], json.dumps(r, ensure_ascii=False)) for r in shard_rows],
            )
            cursor = self.conn.execute(
                "UPDATE jobs SET status='complete', updated_at=?, completed_at=? "
                "WHERE status='staged' AND shard_key=?",
                (now, now, staged_shard_key),
            )
            return cursor.rowcount

    # -- catalog flushing -----------------------------------------------------------------

    def unflushed_counts(self) -> tuple[int, int]:
        with self.lock:
            items = self.conn.execute("SELECT COUNT(*) n FROM items WHERE flushed=0").fetchone()
            docs = self.conn.execute(
                "SELECT COUNT(*) n FROM jobs WHERE status IN ('complete','duplicate') "
                "AND flushed=0").fetchone()
            return int(items["n"]), int(docs["n"])

    def pending_catalog_rows(self) -> dict[str, list[dict]]:
        """Rows ready for the catalog: only committed documents, their pages and shards."""
        with self.lock:
            items = [json.loads(r["row_json"]) for r in self.conn.execute(
                "SELECT row_json FROM items WHERE flushed=0 ORDER BY source_item_id")]
            docs = [json.loads(r["doc_row_json"]) for r in self.conn.execute(
                "SELECT doc_row_json FROM jobs WHERE status IN ('complete','duplicate') "
                "AND flushed=0 ORDER BY doc_id")]
            pages = [json.loads(r["row_json"]) for r in self.conn.execute(
                "SELECT p.row_json FROM page_rows p JOIN jobs j ON j.doc_id=p.doc_id "
                "WHERE j.status='complete' AND j.flushed=0 ORDER BY p.page_id")]
            shards = [json.loads(r["row_json"]) for r in self.conn.execute(
                "SELECT row_json FROM shards WHERE flushed=0 ORDER BY shard_key")]
            return {"source_items": items, "documents": docs, "pages": pages, "shards": shards}

    def mark_flushed(self, rows: dict[str, list[dict]]) -> None:
        with self.lock, self.conn:
            self.conn.executemany(
                "UPDATE items SET flushed=1 WHERE source_item_id=?",
                [(r["source_item_id"],) for r in rows["source_items"]])
            self.conn.executemany(
                "UPDATE jobs SET flushed=1 WHERE doc_id=?",
                [(r["doc_id"],) for r in rows["documents"]])
            self.conn.executemany(
                "UPDATE shards SET flushed=1 WHERE shard_key=?",
                [(r["shard_key"],) for r in rows["shards"]])

    # -- reporting ------------------------------------------------------------------------

    def stats(self) -> dict[str, int]:
        with self.lock:
            out: dict[str, int] = {}
            for row in self.conn.execute("SELECT status, COUNT(*) n FROM items GROUP BY status"):
                out[f"items_{row['status']}"] = row["n"]
            for row in self.conn.execute("SELECT status, COUNT(*) n FROM jobs GROUP BY status"):
                out[f"docs_{row['status']}"] = row["n"]
            row = self.conn.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(p.image_bytes),0) b FROM page_rows p "
                "JOIN jobs j ON j.doc_id=p.doc_id WHERE j.status='complete'").fetchone()
            out["pages_committed"] = row["n"]
            out["page_bytes_committed"] = row["b"]
            row = self.conn.execute("SELECT COUNT(*) n FROM shards").fetchone()
            out["shards_committed"] = row["n"]
            return out

    def backup_to(self, destination: Path) -> None:
        with self.lock:
            target = sqlite3.connect(destination)
            try:
                self.conn.backup(target)
            finally:
                target.close()
