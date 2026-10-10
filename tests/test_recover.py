import stat

from tgfs.recover import find_recoverable, recover_stale
from tgfs.meta import ROOT_INO


def _file(meta, name):
    return meta.create(ROOT_INO, name, stat.S_IFREG | 0o644, "f")


def test_recovers_never_flushed_file(store, meta, tmp_path):
    wb = tmp_path / "wb"
    wb.mkdir()
    n = _file(meta, "a.bin")
    data = bytes(range(256)) * 3  # 768 bytes -> 48 chunks of 16
    (wb / f"{n.id}.wb").write_bytes(data)
    meta.setattr(n.id, mtime=1_700_000_000.0)  # client mtime stored earlier

    ok, bad = recover_stale(store, meta, wb, workers=3)
    assert (ok, bad) == (1, 0)
    assert store.read_range(n.id, 0, len(data)) == data
    got = meta.get_inode(n.id)
    assert got.size == len(data)
    assert got.mtime == 1_700_000_000.0  # not bumped to "now"
    assert not list(wb.glob("*.wb"))  # cleaned up


def test_skips_modification_overlay_and_deleted(store, meta, tmp_path):
    wb = tmp_path / "wb"
    wb.mkdir()
    old = _file(meta, "old.bin")
    import io
    store.put_file(old.id, io.BytesIO(b"x" * 40), 40)  # already has chunks
    (wb / f"{old.id}.wb").write_bytes(b"\0" * 10 + b"y" * 30)  # sparse overlay: unsafe
    (wb / "9999.wb").write_bytes(b"orphan")  # inode no longer exists
    (wb / "junk.wb").write_bytes(b"?")
    empty = _file(meta, "empty.bin")
    (wb / f"{empty.id}.wb").write_bytes(b"")
    assert find_recoverable(meta, wb) == []
    assert recover_stale(store, meta, wb) == (0, 0)
    assert store.read_range(old.id, 0, 40) == b"x" * 40  # untouched


def test_many_files_in_parallel(store, meta, tmp_path):
    wb = tmp_path / "wb"
    wb.mkdir()
    want = {}
    for i in range(20):
        n = _file(meta, f"f{i}.bin")
        d = bytes([i]) * (30 + i)
        (wb / f"{n.id}.wb").write_bytes(d)
        want[n.id] = d
    # Store has 1 upload worker in the fixture; recovery still completes correctly
    assert recover_stale(store, meta, wb, workers=4) == (20, 0)
    for ino, d in want.items():
        assert store.read_range(ino, 0, len(d)) == d
