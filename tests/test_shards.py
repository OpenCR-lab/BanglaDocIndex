import tarfile

from bocr_scrape.shards import ShardWriter, list_members, new_shard_key, read_member


def test_shard_offsets_match_tar_reader(tmp_path):
    key = new_shard_key("pages", "ia", "run-1", 7)
    assert key.startswith("blobs/pages/source=ia/run=run-1/shard-000007-")
    writer = ShardWriter(tmp_path / "s.tar", key)
    payloads = {f"p{i}.jpg": bytes([i]) * (100 + 700 * i) for i in range(5)}
    offsets = {name: writer.add(name, data) for name, data in payloads.items()}
    path = writer.seal()
    with tarfile.open(path) as tar:
        assert sorted(tar.getnames()) == sorted(payloads)
        for name, data in payloads.items():
            assert tar.extractfile(name).read() == data
    for name, (offset, length) in offsets.items():
        assert read_member(path, offset, length) == payloads[name]
    listed = {name: (offset, length) for name, offset, length in list_members(path)}
    assert listed == offsets
    assert writer.size == path.stat().st_size


def test_long_names_are_fine(tmp_path):
    writer = ShardWriter(tmp_path / "s.tar", "k")
    name = "x" * 150 + ".jpg"
    offset, length = writer.add(name, b"abc")
    path = writer.seal()
    assert read_member(path, offset, length) == b"abc"
    assert list_members(path)[0][0] == name
