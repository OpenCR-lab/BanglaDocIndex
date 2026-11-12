import collections

import pytest

from bocr_scrape.ids import doc_id, page_id, split_for


def test_doc_id_is_deterministic_and_source_scoped():
    a = doc_id("ia", "item1", "book.pdf")
    assert a == doc_id("ia", "item1", "book.pdf")
    assert len(a) == 32
    assert a != doc_id("nctb", "item1", "book.pdf")
    assert a != doc_id("ia", "item1", "book2.pdf")


def test_page_id_format():
    assert page_id("abc", 7) == "abc-p000007"
    with pytest.raises(ValueError):
        page_id("abc", 0)


def test_split_is_stable_and_roughly_proportional():
    docs = [doc_id("ia", f"item{i}", "x.pdf") for i in range(20000)]
    first = [split_for(d, 0.05, 0.05) for d in docs]
    assert first == [split_for(d, 0.05, 0.05) for d in docs]
    counts = collections.Counter(first)
    assert 0.04 < counts["val"] / len(docs) < 0.06
    assert 0.04 < counts["test"] / len(docs) < 0.06


def test_split_rejects_bad_fractions():
    with pytest.raises(ValueError):
        split_for("x", 0.6, 0.5)
