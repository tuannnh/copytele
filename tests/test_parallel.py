import io
import stat
import threading
import time

import pytest

from tgfs.backend import MemoryBackend
from tgfs.cache import ReadCache
from tgfs.meta import ROOT_INO
from tgfs.store import Store


class SlowBackend(MemoryBackend):
    """Tracks peak concurrent uploads/downloads."""

    def __init__(self):
        super().__init__()
        self.active = 0
        self.peak = 0
        self._m = threading.Lock()

    def _enter(self):
        with self._m:
            self.active += 1
            self.peak = max(self.peak, self.active)
        time.sleep(0.05)

    def _exit(self):
        with self._m:
            self.active -= 1

    def upload(self, data):
        self._enter()
        try:
            return super().upload(data)
        finally:
            self._exit()

    def download(self, handle):
        self._enter()
        try:
            return super().download(handle)
        finally:
            self._exit()


def _mk(meta, name="f"):
    return meta.create(ROOT_INO, name, stat.S_IFREG | 0o644, "f").id


def _put(store, ino, data):
    store.put_file(ino, io.BytesIO(data), len(data))


@pytest.fixture
def pstore(meta, tmp_path):
    be = SlowBackend()
    cache = ReadCache(tmp_path / "c", 1 << 20)
    st = Store(meta, be, 16, cache, upload_workers=4, download_workers=4,
               readahead_chunks=2)
    yield st, be
    st.close()


def test_parallel_upload_ordered_and_concurrent(pstore, meta):
    store, be = pstore
    ino = _mk(meta)
    data = b"".join(bytes([i]) * 16 for i in range(12))  # 12 distinct chunks
    _put(store, ino, data)
    assert be.peak > 1
    assert be.peak <= 4
    assert be.uploads == 12
    assert [c["off"] for c in meta.get_chunks(ino)] == [i * 16 for i in range(12)]
    assert store.read_range(ino, 0, len(data)) == data


def test_parallel_upload_dedups_within_file(pstore, meta):
    store, be = pstore
    ino = _mk(meta)
    _put(store, ino, b"A" * 16 * 6 + b"B" * 16)
    assert be.uploads == 2
    shas = meta.get_chunk_shas(ino)
    assert meta.blob_get(shas[0])["refcount"] == 6
    assert meta.blob_get(shas[-1])["refcount"] == 1


def test_upload_failure_rolls_back(meta):
    class Boom(MemoryBackend):
        def upload(self, data):
            if data.startswith(b"C"):
                raise RuntimeError("boom")
            return super().upload(data)

    be = Boom()
    store = Store(meta, be, 16, upload_workers=2)
    ino = _mk(meta)
    with pytest.raises(RuntimeError):
        _put(store, ino, b"A" * 16 + b"B" * 16 + b"C" * 16)
    assert meta.get_chunks(ino) == []
    # no refs leak: every uploaded blob was released
    assert meta.db.execute("SELECT COUNT(*) FROM blobs").fetchone()[0] == 0
    assert not be._blobs
    store.close()


def test_iter_chunks_parallel_in_order(pstore, meta):
    store, be = pstore
    ino = _mk(meta)
    data = b"".join(bytes([i]) * 16 for i in range(8))
    _put(store, ino, data)
    store.cache = None  # force real downloads
    be.peak = 0
    out = b"".join(store.iter_chunks(meta.get_chunk_shas(ino)))
    assert out == data
    assert be.peak > 1


def test_cache_range_read_and_singleflight(tmp_path):
    c = ReadCache(tmp_path, 1 << 20)
    calls = []
    gate = threading.Event()

    def loader():
        calls.append(1)
        gate.wait(1)
        return bytes(range(100))

    results = []
    ts = [
        threading.Thread(target=lambda: results.append(c.get_range("s", 10, 20, loader)))
        for _ in range(5)
    ]
    for t in ts:
        t.start()
    time.sleep(0.1)
    gate.set()
    for t in ts:
        t.join()
    assert len(calls) == 1  # one download shared by all waiters
    assert results == [bytes(range(10, 20))] * 5
    assert c.get_range("s", 95, 200, loader) == bytes(range(95, 100))  # clamps
    assert len(calls) == 1


def test_readahead_prefetches_next_chunks(pstore, meta):
    store, be = pstore
    ino = _mk(meta)
    _put(store, ino, b"".join(bytes([i]) * 16 for i in range(6)))
    shas = meta.get_chunk_shas(ino)
    for sha in shas:  # drop cache so reads hit the backend
        p = store.cache._path(sha)
        if p.exists():
            p.unlink()
            store.cache._total -= store.cache._index.pop(sha)
    d0 = be.downloads
    store.read_range(ino, 0, 4)  # first read: not yet sequential
    store.read_range(ino, 4, 4)  # sequential -> chunks 1,2 prefetched
    deadline = time.time() + 2
    while be.downloads < d0 + 3 and time.time() < deadline:
        time.sleep(0.02)
    assert be.downloads == d0 + 3


def test_readahead_not_flooded_and_sequential_only(pstore, meta, monkeypatch):
    store, be = pstore
    ino = _mk(meta)
    _put(store, ino, b"".join(bytes([i]) * 16 for i in range(6)))
    calls = []
    real = store.cache.prefetch
    monkeypatch.setattr(
        store.cache, "prefetch", lambda sha, ld: (calls.append(sha), real(sha, ld))
    )
    for sha in meta.get_chunk_shas(ino):  # cold cache
        p = store.cache._path(sha)
        if p.exists():
            p.unlink()
            store.cache._total -= store.cache._index.pop(sha)
    store.read_range(ino, 50, 2)  # random read: no readahead
    store.read_range(ino, 0, 1)  # random again
    time.sleep(0.1)
    assert calls == []
    for off in range(1, 15):  # 14 tiny sequential reads inside chunk 0
        store.read_range(ino, off, 1)
    time.sleep(0.2)
    assert len(calls) <= store.readahead


# --------------------------- writeback (up2k-style) ---------------------------
from tgfs.cache import WritebackManager  # noqa: E402


@pytest.fixture
def wbm(meta, tmp_path):
    be = MemoryBackend()
    store = Store(meta, be, 16, upload_workers=2, download_workers=2)
    wb = WritebackManager(store, meta, tmp_path / "wb", flush_delay=0.3)
    yield wb, store, be
    wb.shutdown()
    store.close()


def _wait(pred, t=3.0):
    end = time.time() + t
    while time.time() < end:
        if pred():
            return True
        time.sleep(0.02)
    return False


def test_reopen_cycle_defers_flush_and_never_redownloads(wbm, meta):
    wb, store, be = wbm
    ino = _mk(meta)
    n = 40  # 40 chunks of 16 bytes
    st = wb.open(ino, True)
    wb.truncate(st, 16 * n)  # up2k preallocates
    wb.release(st)
    for k in range(n):  # one open/write/close per piece, like up2k
        st = wb.open(ino, True)
        wb.write(st, bytes([k + 1]) * 16, 16 * k)
        wb.flush(st)  # FUSE flush on close: must be lazy
        wb.release(st)
        assert wb.live_size(ino) == 16 * n
    assert be.uploads == 0 and be.downloads == 0  # nothing hit Telegram yet
    assert _wait(lambda: meta.get_inode(ino).size == 16 * n)  # idle -> flushed
    assert be.uploads == n  # each distinct chunk once
    want = b"".join(bytes([k + 1]) * 16 for k in range(n))
    assert store.read_range(ino, 0, len(want)) == want


def test_partial_edit_uploads_only_dirty_chunk(wbm, meta):
    wb, store, be = wbm
    ino = _mk(meta)
    base = b"".join(bytes([i]) * 16 for i in range(10))
    st = wb.open(ino, True)
    wb.write(st, base, 0)
    wb.sync(st)
    wb.release(st)
    assert _wait(lambda: ino not in wb._states)
    up0, down0 = be.uploads, be.downloads
    st = wb.open(ino, True)  # fresh state: must not download everything
    wb.write(st, b"XX", 5 * 16 + 3)  # inside chunk 5
    wb.sync(st)
    assert be.downloads - down0 == 1  # only chunk 5 fetched
    assert be.uploads - up0 == 1  # only chunk 5 re-uploaded
    wb.release(st)
    exp = bytearray(base)
    exp[83:85] = b"XX"
    assert store.read_range(ino, 0, len(exp)) == bytes(exp)


def test_truncate_and_grow_roundtrip(wbm, meta):
    wb, store, be = wbm
    ino = _mk(meta)
    st = wb.open(ino, True)
    wb.write(st, b"A" * 40, 0)
    wb.sync(st)
    wb.truncate(st, 20)
    wb.sync(st)
    assert store.read_range(ino, 0, 100) == b"A" * 20
    wb.truncate(st, 50)  # grow -> zero padded
    wb.write(st, b"Z", 49)
    wb.sync(st)
    wb.release(st)
    assert store.read_range(ino, 0, 100) == b"A" * 20 + b"\x00" * 29 + b"Z"


def test_discard_drops_pending_without_upload(wbm, meta):
    wb, store, be = wbm
    ino = _mk(meta)
    st = wb.open(ino, True)
    wb.write(st, b"q" * 32, 0)
    wb.release(st)
    wb.discard(ino)
    time.sleep(0.6)
    assert be.uploads == 0
    assert ino not in wb._states
