from __future__ import annotations

import functools
import http.server
import threading
from pathlib import Path

import pytest
from PIL import Image, ImageDraw

from bocr_scrape.config import Settings


def make_scanned_pdf(path: Path, pages: int, dpi: int = 150, seed: int = 0) -> Path:
    """A PDF whose pages are full-page raster images at a known resolution (like a scan)."""
    images = []
    for n in range(pages):
        image = Image.new("RGB", (int(8.27 * dpi), int(11.69 * dpi)), "white")
        draw = ImageDraw.Draw(image)
        for row in range(12):
            y = 80 + row * 90 + (seed * 7 + n * 13) % 40
            draw.rectangle([60, y, 60 + 300 + (row * 37 + n * 11 + seed) % 700, y + 30],
                           fill="black")
        images.append(image)
    images[0].save(path, "PDF", resolution=dpi, save_all=True, append_images=images[1:])
    return path


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        data_uri=str(tmp_path / "bucket"),
        work_dir=tmp_path / "work",
        render_workers=1,
        upload_workers=2,
        batch_pages=2,
        catalog_flush_docs=1,
        progress_interval_seconds=3600,
        checkpoint_interval_seconds=3600,
        disk_high_bytes=10 * 1024**3,
        min_free_bytes=0,
        retry_base_seconds=0,
        error_retry_delay_seconds=0,
        shard_target_bytes=100_000,
    )


@pytest.fixture
def http_dir(tmp_path: Path):
    """Serve a directory over HTTP (with Range support via SimpleHTTPRequestHandler)."""
    root = tmp_path / "www"
    root.mkdir()
    handler = functools.partial(QuietHandler, directory=str(root))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield root, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass
