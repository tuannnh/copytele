import io
import stat

import pytest

from tgfs.cache import ReadCache, WritebackManager
from tgfs.meta import ROOT_INO


# --------------------------- ReadCache --------------------------------------
def test_readcache_miss_then_hit(tmp_path):
    c = ReadCache(tmp_path, cap_bytes=1024)
    calls = []

    def loader():
        calls.append(1)
        return b"payload"

    assert c.get("sha_a", loader) == b"payload"
    assert c.get("sha_a", loader) == b"payload"
    assert len(calls) == 1  # second call served from disk


def test_readcache_evicts_lru(tmp_path):
    c = ReadCache(tmp_path, cap_bytes=20)
    c.get("a", lambda: b"X" * 10)
    c.get("b", lambda: b"Y" * 10)
    c.get("a", lambda: b"X" * 10)  # touch a -> b is now LRU
    c.get("c", lambda: b"Z" * 10)  # over cap -> evict b
    assert (tmp_path / "blobs" / "b").exists() is False
    assert (tmp_path / "blobs" / "a").exists()


def test_readcache_persists_index(tmp_path):
    ReadCache(tmp_path, cap_bytes=1024).get("k", lambda: b"hi")
    c2 = ReadCache(tmp_path, cap_bytes=1024)
    assert c2.get("k", lambda: pytest.fail("should be cached")) == b"hi"


# --------------------------- Writeback --------------------------------------
@pytest.fixture
def wb(store, meta, tmp_path):
    return WritebackManager(store, meta, tmp_path)


def _mkfile(meta, name="f"):
    return meta.create(ROOT_INO, name, stat.S_IFREG | 0o644, "f").id


def _read_all(store, ino, size):
    return store.read_range(ino, 0, size)


def test_write_new_file(wb, store, meta):
    ino = _mkfile(meta)
    st = wb.open(ino, for_write=True)
    wb.write(st, b"hello world", 0)
    wb.release(st)
    assert meta.get_inode(ino).size == 11
    assert _read_all(store, ino, 11) == b"hello world"


def test_random_write_into_existing(wb, store, meta):
    ino = _mkfile(meta)
    st = wb.open(ino, for_write=True)
    wb.write(st, b"A" * 50, 0)
    wb.release(st)
    # reopen, overwrite middle bytes
    st = wb.open(ino, for_write=True)
    wb.write(st, b"BBB", 10)
    wb.release(st)
    got = _read_all(store, ino, 50)
    assert got == b"A" * 10 + b"BBB" + b"A" * 37


def test_append_extends_file(wb, store, meta):
    ino = _mkfile(meta)
    st = wb.open(ino, for_write=True)
    wb.write(st, b"12345", 0)
    wb.release(st)
    st = wb.open(ino, for_write=True)
    wb.write(st, b"6789", 5)  # write past current end
    wb.release(st)
    assert _read_all(store, ino, 9) == b"123456789"


def test_truncate_grow_and_shrink(wb, store, meta):
    ino = _mkfile(meta)
    st = wb.open(ino, for_write=True)
    wb.write(st, b"abcdefgh", 0)
    wb.release(st)

    wb.truncate_path(ino, 3)
    assert meta.get_inode(ino).size == 3
    assert _read_all(store, ino, 10) == b"abc"

    wb.truncate_path(ino, 5)  # grow -> zero-padded
    assert meta.get_inode(ino).size == 5
    assert _read_all(store, ino, 10) == b"abc\x00\x00"


def test_concurrent_opens_share_tempfile(wb, store, meta):
    ino = _mkfile(meta)
    w = wb.open(ino, for_write=True)
    r = wb.open(ino, for_write=False)  # second handle, same inode
    wb.write(w, b"shared-data", 0)
    # reader sees the in-progress write via the shared temp file
    assert wb.read(r, 11, 0) == b"shared-data"
    wb.release(r)
    wb.release(w)
    assert _read_all(store, ino, 11) == b"shared-data"


def test_readonly_open_serves_from_store(wb, store, meta):
    ino = _mkfile(meta)
    # seed content through the store directly
    store.put_file(ino, io.BytesIO(b"D" * 40), 40)
    st = wb.open(ino, for_write=False)
    assert not st.materialized  # no temp file for pure reads
    assert wb.read(st, 10, 5) == b"D" * 10
    wb.release(st)


def test_flush_persists_without_close(wb, store, meta):
    ino = _mkfile(meta)
    st = wb.open(ino, for_write=True)
    wb.write(st, b"persist-me", 0)
    wb.flush(st)  # fsync-like: visible before release
    assert _read_all(store, ino, 10) == b"persist-me"
    wb.release(st)
