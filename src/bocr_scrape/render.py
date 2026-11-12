"""PDF page rendering with pypdfium2 (DESIGN.md section 9.2).

Rendering runs in worker processes (pdfium is not thread-safe), one batch of pages per task.
Each page is rendered at the target DPI, but never above the native resolution of a scan,
encoded once as the master JPEG, hashed, and perceptually hashed.
"""

from __future__ import annotations

import dataclasses
import hashlib
import threading
from pathlib import Path

import numpy as np
import pypdfium2 as pdfium
import pypdfium2.raw as pdfium_c
from PIL import Image

# A page is treated as a scan when one image covers at least this share of the page.
SCAN_COVERAGE = 0.5


@dataclasses.dataclass(frozen=True)
class RenderOptions:
    target_dpi: int
    jpeg_quality: int
    max_pixels: int

    @property
    def version(self) -> str:
        return (
            f"r2-{self.target_dpi}dpi-noupsample-{self.max_pixels // 10**6}mp-q{self.jpeg_quality}"
            f"-pdfium{pdfium.PDFIUM_INFO.build}"
        )

    def recipe(self) -> dict:
        """Everything needed to reproduce the page images byte for byte (rehydration)."""
        from PIL import features

        return {
            "render_recipe": self.version,
            "target_dpi": self.target_dpi,
            "native_dpi_rule": "render at min(target, native) for scans (one image covering "
                               f">= {SCAN_COVERAGE:.0%} of the page): no upsampling",
            "max_pixels": self.max_pixels,
            "color": "RGB",
            "jpeg_quality": self.jpeg_quality,
            "jpeg_optimize": True,
            "pdfium_build": pdfium.PDFIUM_INFO.build,
            "pypdfium2": pdfium.V_PYPDFIUM2,
            "pillow": __import__("PIL").__version__,
            "libjpeg_turbo": features.version("libjpeg_turbo"),
        }


_IN_PROCESS_PDFIUM = threading.Lock()


def page_count(pdf_path: Path) -> int:
    """Count pages in the calling process. pdfium is not thread-safe and several consumer
    threads call this concurrently (PROCESS_WORKERS > 1); without the lock, valid PDFs failed
    with "Data format error" (main run, 2026-10-24)."""
    with _IN_PROCESS_PDFIUM:
        pdf = pdfium.PdfDocument(str(pdf_path))
        try:
            return len(pdf)
        finally:
            pdf.close()


def native_dpi(page: pdfium.PdfPage) -> float | None:
    """DPI of the dominant raster image on the page, or None for born-digital pages."""
    try:
        width_pt, height_pt = page.get_size()
        page_area = width_pt * height_pt
        best_area, best_dpi = 0.0, None
        for obj in page.get_objects(filter=(pdfium_c.FPDF_PAGEOBJ_IMAGE,), max_depth=2):
            left, bottom, right, top = obj.get_pos()
            area = max(0.0, right - left) * max(0.0, top - bottom)
            if area > best_area:
                meta = obj.get_metadata()
                dpi = max(float(meta.horizontal_dpi), float(meta.vertical_dpi))
                best_area, best_dpi = area, dpi if dpi > 0 else None
        if page_area > 0 and best_area / page_area >= SCAN_COVERAGE:
            return best_dpi
    except Exception:  # noqa: BLE001 - malformed objects must never stop rendering
        return None
    return None


def choose_dpi(native: float | None, width_pt: float, height_pt: float,
               options: RenderOptions) -> int:
    """Target DPI, never above a scan's own resolution (no upsampling), and capped so the
    page stays within max_pixels (about a newspaper broadsheet at 150 DPI)."""
    dpi = float(options.target_dpi)
    if native:
        dpi = min(dpi, native)
    pixels = (width_pt / 72 * dpi) * (height_pt / 72 * dpi)
    if pixels > options.max_pixels:
        dpi *= (options.max_pixels / pixels) ** 0.5
    return max(1, int(dpi))


_DCT_N = 32


def _dct_matrix(n: int) -> np.ndarray:
    k = np.arange(n)[:, None]
    i = np.arange(n)[None, :]
    matrix = np.cos(np.pi * (2 * i + 1) * k / (2 * n)) * np.sqrt(2 / n)
    matrix[0] /= np.sqrt(2)
    return matrix


_DCT = _dct_matrix(_DCT_N)


def phash(image: Image.Image) -> str:
    """64-bit DCT perceptual hash as 16 hex characters."""
    small = image.convert("L").resize((_DCT_N, _DCT_N), Image.Resampling.LANCZOS)
    pixels = np.asarray(small, dtype=np.float64)
    coefficients = _DCT @ pixels @ _DCT.T
    low = coefficients[:8, :8].flatten()[1:]  # drop the DC term
    bits = np.concatenate([[False], low > np.median(low)])
    value = 0
    for bit in bits:
        value = (value << 1) | int(bit)
    return f"{value:016x}"


def render_pages(pdf_path: str, page_numbers: list[int], out_dir: str,
                 options: RenderOptions) -> list[dict]:
    """Render 1-based page numbers to JPEG files. Runs inside a worker process.

    A page pdfium cannot load or render (a corrupt object in one page of a scanned book) is
    returned as {"page_number": n, "error": ...} instead of failing the whole document: the
    error is deterministic, so retrying cannot help, and the other pages are still good.
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    results = []
    pdf = pdfium.PdfDocument(pdf_path)
    try:
        for number in page_numbers:
            try:
                page = pdf[number - 1]
            except pdfium.PdfiumError as exc:
                results.append({"page_number": number, "error": str(exc)})
                continue
            try:
                width_pt, height_pt = page.get_size()
                native = native_dpi(page)
                dpi = choose_dpi(native, width_pt, height_pt, options)
                bitmap = page.render(scale=dpi / 72, may_draw_forms=True)
                image = bitmap.to_pil().convert("RGB")
            except pdfium.PdfiumError as exc:
                results.append({"page_number": number, "error": str(exc)})
                continue
            finally:
                page.close()
            path = out / f"page-{number:06d}.jpg"
            image.save(path, "JPEG", quality=options.jpeg_quality, optimize=True,
                       dpi=(dpi, dpi))
            data = path.read_bytes()
            results.append(
                {
                    "page_number": number,
                    "path": str(path),
                    "width": image.width,
                    "height": image.height,
                    "dpi": dpi,
                    "native_dpi": int(round(native)) if native else None,
                    "image_bytes": len(data),
                    "image_sha256": hashlib.sha256(data).hexdigest(),
                    "phash": phash(image),
                }
            )
    finally:
        pdf.close()
    return results
