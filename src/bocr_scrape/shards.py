"""Write-once tar shards (DESIGN.md section 6).

Shards are plain, uncompressed POSIX tar files (JPEG and PDF are already compressed), so they
are readable by `tar`, Python's tarfile and WebDataset. Members are grouped by key: a page is
stored as `<page_id>.jpg` plus `<page_id>.json`, which WebDataset reads as one sample.

The writer records each member's byte offset and length, so any single member can be read
later with one byte-range request, without reading or unpacking the rest of the shard.
A shard is assembled on local disk and uploaded only once complete (sealed), so object
storage never holds a partial shard.
"""

from __future__ import annotations

import io
import os
import secrets
import tarfile
import time
from pathlib import Path

BLOCK = tarfile.BLOCKSIZE  # 512


def new_shard_key(kind: str, source: str, run_id: str, sequence: int) -> str:
    """Relative key, unique even if a sequence number is ever reused after a restore."""
    token = secrets.token_hex(4)
    return f"blobs/{kind}/source={source}/run={run_id}/shard-{sequence:06d}-{token}.tar"


class ShardWriter:
    """Appends members to a local tar file and reports where each one landed."""

    def __init__(self, path: Path, key: str):
        self.path = path
        self.key = key
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("wb")
        self.size = 0
        self.members = 0

    def add(self, name: str, data: bytes) -> tuple[int, int]:
        """Append one member; returns (offset of its data in the shard, length)."""
        info = tarfile.TarInfo(name)
        info.size = len(data)
        info.mtime = int(time.time())
        info.mode = 0o644
        header = info.tobuf(format=tarfile.PAX_FORMAT)
        offset = self.size + len(header)
        padding = (BLOCK - len(data) % BLOCK) % BLOCK
        self._file.write(header)
        self._file.write(data)
        self._file.write(b"\0" * padding)
        self.size += len(header) + len(data) + padding
        self.members += 1
        return offset, len(data)

    def seal(self) -> Path:
        """Finish the archive (two zero blocks) and make it durable on disk."""
        self._file.write(b"\0" * (2 * BLOCK))
        self.size += 2 * BLOCK
        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()
        return self.path

    def discard(self) -> None:
        if not self._file.closed:
            self._file.close()
        self.path.unlink(missing_ok=True)


def read_member(shard: Path | bytes, offset: int, length: int) -> bytes:
    """Read one member's bytes from a local shard file or an in-memory shard."""
    if isinstance(shard, bytes | bytearray):
        return bytes(shard[offset:offset + length])
    with Path(shard).open("rb") as handle:
        handle.seek(offset)
        return handle.read(length)


def list_members(shard: Path | bytes) -> list[tuple[str, int, int]]:
    """(name, offset, length) for every member, using the standard tar reader."""
    fileobj = io.BytesIO(shard) if isinstance(shard, bytes | bytearray) else None
    with tarfile.open(name=None if fileobj else str(shard), fileobj=fileobj, mode="r:") as tar:
        return [(m.name, m.offset_data, m.size) for m in tar.getmembers() if m.isfile()]
