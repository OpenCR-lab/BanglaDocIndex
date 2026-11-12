"""Object storage behind one small interface: Google Cloud Storage or a local directory.

The local backend mirrors the bucket layout on disk so the whole pipeline can be tested
without cloud credentials. Keys are always POSIX-style paths relative to the dataset root.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Iterator
from pathlib import Path, PurePosixPath


class Storage(ABC):
    @abstractmethod
    def uri(self, key: str) -> str: ...

    @abstractmethod
    def put_file(
        self,
        local_path: Path,
        key: str,
        *,
        content_type: str,
        if_absent: bool = False,
        storage_class: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> bool:
        """Upload a file. Returns False if if_absent was set and the object already existed."""

    @abstractmethod
    def put_bytes(
        self, data: bytes, key: str, *, content_type: str, if_absent: bool = False
    ) -> bool: ...

    @abstractmethod
    def get_bytes(self, key: str) -> bytes | None:
        """Return the object's bytes, or None if it does not exist."""

    @abstractmethod
    def get_range(self, key: str, offset: int, length: int) -> bytes:
        """Read `length` bytes starting at `offset` (one range request on GCS)."""

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def list(self, prefix: str) -> Iterator[str]:
        """Yield keys under a prefix. Only for verification and tooling, never for discovery."""


class LocalStorage(Storage):
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        parts = PurePosixPath(key).parts
        if not parts or key.startswith("/") or ".." in parts or ":" in parts[0]:
            raise ValueError(f"invalid storage key: {key}")
        return self.root.joinpath(*parts)

    def uri(self, key: str) -> str:
        return self._path(key).as_uri()

    def _write(self, key: str, writer, if_absent: bool) -> bool:
        target = self._path(key)
        if if_absent and target.exists():
            return False
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=".tmp-")
        os.close(fd)
        try:
            writer(Path(tmp))
            if if_absent and target.exists():
                return False
            os.replace(tmp, target)
            return True
        finally:
            Path(tmp).unlink(missing_ok=True)

    def put_file(self, local_path, key, *, content_type, if_absent=False, storage_class=None,
                 metadata=None) -> bool:
        return self._write(key, lambda tmp: shutil.copyfile(local_path, tmp), if_absent)

    def put_bytes(self, data, key, *, content_type, if_absent=False) -> bool:
        return self._write(key, lambda tmp: tmp.write_bytes(data), if_absent)

    def get_bytes(self, key):
        path = self._path(key)
        return path.read_bytes() if path.is_file() else None

    def get_range(self, key, offset, length):
        with self._path(key).open("rb") as handle:
            handle.seek(offset)
            return handle.read(length)

    def exists(self, key):
        return self._path(key).is_file()

    def list(self, prefix):
        base = self.root / prefix
        search_root = base if base.is_dir() else base.parent
        if not search_root.exists():
            return
        for path in sorted(search_root.rglob("*")):
            if path.is_file() and not path.name.startswith(".tmp-"):
                key = path.relative_to(self.root).as_posix()
                if key.startswith(prefix):
                    yield key


class GCSStorage(Storage):
    def __init__(self, bucket_name: str, prefix: str = ""):
        from google.cloud import storage

        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.bucket_name = bucket_name
        self.prefix = prefix.strip("/")

    def _name(self, key: str) -> str:
        return f"{self.prefix}/{key}" if self.prefix else key

    def uri(self, key: str) -> str:
        return f"gs://{self.bucket_name}/{self._name(key)}"

    def put_file(self, local_path, key, *, content_type, if_absent=False, storage_class=None,
                 metadata=None) -> bool:
        from google.api_core.exceptions import PreconditionFailed
        from google.cloud.storage.retry import DEFAULT_RETRY

        blob = self.bucket.blob(self._name(key))
        if storage_class:
            blob.storage_class = storage_class
        if metadata:
            blob.metadata = metadata
        try:
            if if_absent:
                # Conditional writes are idempotent, so the library retries them by default.
                blob.upload_from_filename(
                    str(local_path), content_type=content_type, if_generation_match=0,
                    timeout=300,
                )
            else:
                blob.upload_from_filename(
                    str(local_path), content_type=content_type, retry=DEFAULT_RETRY, timeout=300
                )
        except PreconditionFailed:
            return False
        return True

    def put_bytes(self, data, key, *, content_type, if_absent=False) -> bool:
        from google.api_core.exceptions import PreconditionFailed
        from google.cloud.storage.retry import DEFAULT_RETRY

        blob = self.bucket.blob(self._name(key))
        try:
            if if_absent:
                blob.upload_from_string(
                    data, content_type=content_type, if_generation_match=0, timeout=120
                )
            else:
                blob.upload_from_string(
                    data, content_type=content_type, retry=DEFAULT_RETRY, timeout=120
                )
        except PreconditionFailed:
            return False
        return True

    def get_bytes(self, key):
        from google.api_core.exceptions import NotFound

        try:
            return self.bucket.blob(self._name(key)).download_as_bytes(timeout=300)
        except NotFound:
            return None

    def get_range(self, key, offset, length):
        blob = self.bucket.blob(self._name(key))
        return blob.download_as_bytes(start=offset, end=offset + length - 1, timeout=300)

    def exists(self, key):
        return self.bucket.blob(self._name(key)).exists(timeout=60)

    def list(self, prefix):
        strip = len(self.prefix) + 1 if self.prefix else 0
        for blob in self.client.list_blobs(self.bucket, prefix=self._name(prefix)):
            yield blob.name[strip:]


def open_storage(data_uri: str) -> Storage:
    if data_uri.startswith("gs://"):
        bucket, _, prefix = data_uri[len("gs://"):].partition("/")
        return GCSStorage(bucket, prefix)
    if data_uri.startswith("file://"):
        data_uri = data_uri[len("file://"):]
    return LocalStorage(Path(data_uri).expanduser())
