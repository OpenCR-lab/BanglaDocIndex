from bocr_scrape.state import StateStore


def _row(item_id: str, status: str) -> dict:
    return {"source_item_id": item_id, "status": status}


def test_errored_items_are_rechecked_but_decided_items_are_not(tmp_path):
    state = StateStore(tmp_path / "s.sqlite")
    state.record_item(_row("a", "error"), [])
    state.record_item(_row("b", "no_pdf"), [])
    assert not state.item_checked("a")
    assert state.item_checked("b")
    state.record_item(_row("a", "accepted"), [("doc1", {"source_item_id": "a"})])
    assert state.item_checked("a")
    assert state.stats()["items_accepted"] == 1


def test_failed_jobs_back_off_then_become_permanent(tmp_path):
    state = StateStore(tmp_path / "s.sqlite")
    state.record_item(_row("a", "accepted"), [("doc1", {"source_item_id": "a"})])
    for attempt in range(1, 4):
        job = state.claim_next_job(max_attempts=3)
        assert job["attempts"] == attempt
        state.mark_failed("doc1", "boom", retryable=True, max_attempts=3, base_delay=0)
    assert state.get_job("doc1")["status"] == "permanent_failed"
    assert state.claim_next_job(max_attempts=3) is None
    assert state.reset_permanent_failures() == 1
    assert state.claim_next_job(max_attempts=3)["attempts"] == 1


def test_staged_documents_reset_on_restart(tmp_path):
    state = StateStore(tmp_path / "s.sqlite")
    state.record_item(_row("a", "accepted"), [("doc1", {"source_item_id": "a"})])
    state.stage_document("doc1", "blobs/pages/x.tar", {"doc_id": "doc1"},
                         [{"page_id": "doc1-p000001", "length": 10}])
    assert state.unfinished_count() == 0  # staged documents only await a seal
    assert state.recover_after_restart() == 1
    assert state.get_job("doc1")["status"] == "pending"
    assert state.pending_catalog_rows()["pages"] == []


def test_commit_completes_only_that_shard(tmp_path):
    state = StateStore(tmp_path / "s.sqlite")
    state.record_item(_row("a", "accepted"), [("d1", {"source_item_id": "a"}),
                                              ("d2", {"source_item_id": "a"})])
    state.stage_document("d1", "s1", {"doc_id": "d1"}, [{"page_id": "d1-p000001", "length": 1}])
    state.stage_document("d2", "s2", {"doc_id": "d2"}, [{"page_id": "d2-p000001", "length": 1}])
    assert state.commit_shards([{"shard_key": "s1"}], "s1") == 1
    rows = state.pending_catalog_rows()
    assert [d["doc_id"] for d in rows["documents"]] == ["d1"]
    assert [p["page_id"] for p in rows["pages"]] == ["d1-p000001"]
    assert [s["shard_key"] for s in rows["shards"]] == ["s1"]


def test_restart_refunds_the_interrupted_attempt(tmp_path):
    state = StateStore(tmp_path / "s.sqlite")
    state.record_item(_row("a", "accepted"), [("doc1", {"source_item_id": "a"})])
    for _ in range(2):
        state.claim_next_job(max_attempts=3)
        state.mark_failed("doc1", "boom", retryable=True, max_attempts=3, base_delay=0)
    job = state.claim_next_job(max_attempts=3)  # last attempt, interrupted by a restart
    assert job["attempts"] == 3
    state.recover_after_restart()
    again = state.claim_next_job(max_attempts=3)  # still claimable: the run can finish
    assert again is not None and again["attempts"] == 3
