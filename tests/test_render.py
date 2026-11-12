from PIL import Image

from bocr_scrape.render import RenderOptions, choose_dpi, page_count, phash, render_pages

from .conftest import make_scanned_pdf

OPTIONS = RenderOptions(target_dpi=200, jpeg_quality=88, max_pixels=60_000_000)


def test_choose_dpi_never_upsamples():
    assert choose_dpi(None, 595, 842, OPTIONS) == 200
    assert choose_dpi(150.0, 595, 842, OPTIONS) == 150
    assert choose_dpi(300.0, 595, 842, OPTIONS) == 200
    assert choose_dpi(72.0, 595, 842, OPTIONS) == 72  # low-resolution scan: native pixels


def test_choose_dpi_limits_huge_pages():
    dpi = choose_dpi(None, 72 * 100, 72 * 100, OPTIONS)  # a 100 x 100 inch poster
    assert (100 * dpi) ** 2 <= OPTIONS.max_pixels


def test_render_scanned_pdf_uses_native_resolution(tmp_path):
    pdf = make_scanned_pdf(tmp_path / "scan.pdf", pages=3, dpi=150)
    assert page_count(pdf) == 3
    results = render_pages(str(pdf), [1, 3], str(tmp_path / "out"), OPTIONS)
    assert [r["page_number"] for r in results] == [1, 3]
    for result in results:
        assert result["native_dpi"] == 150
        assert result["dpi"] == 150
        assert abs(result["width"] - int(8.27 * 150)) <= 2
        assert len(result["image_sha256"]) == 64
        assert len(result["phash"]) == 16
        with Image.open(result["path"]) as image:
            assert image.format == "JPEG"


def test_phash_is_stable_and_discriminative():
    a = Image.new("RGB", (400, 600), "white")
    b = a.copy()
    b.paste((0, 0, 0), (0, 0, 200, 600))
    assert phash(a) == phash(a.copy())
    assert phash(a) != phash(b)


def test_render_skips_unloadable_page(tmp_path, monkeypatch):
    import pypdfium2 as pdfium

    pdf = make_scanned_pdf(tmp_path / "scan.pdf", pages=3, dpi=150)
    original = pdfium.PdfDocument.__getitem__

    def flaky(self, index):
        if index == 1:
            raise pdfium.PdfiumError("Failed to load page.")
        return original(self, index)

    monkeypatch.setattr(pdfium.PdfDocument, "__getitem__", flaky)
    results = render_pages(str(pdf), [1, 2, 3], str(tmp_path / "out"), OPTIONS)
    assert [r["page_number"] for r in results] == [1, 2, 3]
    assert "error" in results[1] and "error" not in results[0] and "error" not in results[2]


def test_page_count_is_safe_from_many_threads(tmp_path):
    import concurrent.futures as cf

    pdf = make_scanned_pdf(tmp_path / "scan.pdf", pages=4, dpi=100)
    with cf.ThreadPoolExecutor(8) as pool:
        counts = list(pool.map(lambda _: page_count(pdf), range(200)))
    assert counts == [4] * 200
