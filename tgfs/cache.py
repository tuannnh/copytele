"""Local read cache + writeback manager.

ReadCache
    A bounded, on-disk LRU cache of chunk content keyed by sha256, so repeated
    reads of the same data don't re-fetch from Telegram.

WritebackManager
    Gives the filesystem full POSIX write semantics on top of immutable
    Telegram blobs. On the first write-open of an inode, its current content is
    materialized into a local temp file; all reads/writes/truncates then hit
    that temp file. On final close (or fsync/flush) the temp file is handed to
    :meth:`Store.put_file`, which re-chunks and dedups it back into Telegram.

    State is kept per-inode and refcounted, so concurrent opens of the same
    file (e.g. copyparty's up2k writing chunks at offsets) share one temp file
    and therefore one consistent view.
"""

from __future__ import annotations

import os
import threading
from collections import OrderedDict
from pathlib import Path
from typing import Callable

from .meta import Meta
from .store import Store


class ReadCache:
    def __init__(self, cache_dir: str | Path, cap_bytes: int) -> None:
        self.dir = Path(cache_dir) / "blobs"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.cap = cap_bytes
        self.lock = threading.Lock()
        self._index: OrderedDict[str, int] = OrderedDict()
        self._total = 0
        self._inflight: dict[str, threading.Event] = {}
        for p in sorted(self.dir.iterdir(), key=lambda x: x.stat().st_mtime):
            if p.is_file():
                sz = p.stat().st_size
                self._index[p.name] = sz
                self._total += sz

    def _path(self, sha: str) -> Path:
        return self.dir / sha

    def _read_cached(self, sha: str, lo: int | None, hi: int | None) -> bytes | None:
        """Read (a slice of) a cached blob, or None on miss."""
        with self.lock:
            if sha not in self._index:
                return None
            self._index.move_to_end(sha)
        try:
            with open(self._path(sha), "rb") as f:
                if lo is None:
                    return f.read()
                f.seek(lo)
                return f.read(max(0, hi - lo))
        except OSError:
            with self.lock:  # vanished; treat as a miss
                sz = self._index.pop(sha, None)
                if sz is not None:
                    self._total -= sz
            return None

    def _load_once(self, sha: str, loader: Callable[[], bytes]) -> bytes | None:
        """Singleflight: concurrent misses on one sha share a single download.

        Returns the blob when this caller downloaded it, else None (another
        caller did; the data is now on disk).
        """
        with self.lock:
            ev = self._inflight.get(sha)
            leader = ev is None
            if leader:
                ev = self._inflight[sha] = threading.Event()
        if not leader:
            ev.wait()
            return None
        try:
            data = loader()
            self._store(sha, data)
            return data
        finally:
            with self.lock:
                self._inflight.pop(sha, None)
            ev.set()

    def get(self, sha: str, loader: Callable[[], bytes]) -> bytes:
        return self.get_range(sha, None, None, loader)

    def get_range(
        self, sha: str, lo: int | None, hi: int | None,
        loader: Callable[[], bytes],
    ) -> bytes:
        """Return blob[lo:hi] (whole blob if lo is None), fetching on a miss."""
        for _ in range(3):
            got = self._read_cached(sha, lo, hi)
            if got is not None:
                return got
            data = self._load_once(sha, loader)
            if data is not None:
                return data if lo is None else data[lo:hi]
        # waiter's leader failed or cache can't hold it: fetch directly
        data = loader()
        return data if lo is None else data[lo:hi]

    def has(self, sha: str) -> bool:
        with self.lock:
            return sha in self._index or sha in self._inflight

    def prefetch(self, sha: str, loader: Callable[[], bytes]) -> None:
        """Best-effort background fill; never waits on another download."""
        if not self.has(sha):
            self._load_once(sha, loader)

    def _store(self, sha: str, data: bytes) -> None:
        with self.lock:
            if sha not in self._index:
                tmp = self._path(sha).with_suffix(".tmp")
                try:
                    tmp.write_bytes(data)
                    os.replace(tmp, self._path(sha))
                except OSError:
                    return
                self._index[sha] = len(data)
                self._total += len(data)
            else:
                self._index.move_to_end(sha)
            self._evict()

    def _evict(self) -> None:
        while self._total > self.cap and len(self._index) > 1:
            old_sha, old_sz = self._index.popitem(last=False)
            try:
                self._path(old_sha).unlink()
            except OSError:
                pass
            self._total -= old_sz


class _FileState:
    """Per-inode writeback state, shared across concurrent open handles."""

    def __init__(self, ino: int, temp_path: Path) -> None:
        self.ino = ino
        self.temp_path = temp_path
        self.fobj = open(temp_path, "w+b", buffering=0)
        self.refcount = 0
        self.dirty = False
        self.materialized = False
        self.lock = threading.RLock()


class WritebackManager:
    def __init__(self, store: Store, meta: Meta, cache_dir: str | Path) -> None:
        self.store = store
        self.meta = meta
        self.wb_dir = Path(cache_dir) / "wb"
        self.wb_dir.mkdir(parents=True, exist_ok=True)
        for p in self.wb_dir.iterdir():  # clear stale temp files from a crash
            try:
                p.unlink()
            except OSError:
                pass
        self.lock = threading.Lock()
        self._states: dict[int, _FileState] = {}

    # ----- per-inode state -------------------------------------------------
    def _materialize(self, st: _FileState) -> None:
        """Fill the temp file with the inode's current Telegram content."""
        if st.materialized:
            return
        st.fobj.seek(0)
        st.fobj.truncate(0)
        shas = [ch["sha"] for ch in self.meta.get_chunks(st.ino)]
        for data in self.store.iter_chunks(shas):
            st.fobj.write(data)
        st.fobj.flush()
        st.materialized = True

    def open(self, ino: int, for_write: bool) -> _FileState:
        with self.lock:
            st = self._states.get(ino)
            if st is None:
                tmp = self.wb_dir / f"{ino}.wb"
                st = _FileState(ino, tmp)
                self._states[ino] = st
            st.refcount += 1
        if for_write:
            with st.lock:
                self._materialize(st)
        return st

    # ----- io --------------------------------------------------------------
    def read(self, st: _FileState, size: int, offset: int) -> bytes:
        with st.lock:
            if st.materialized:
                st.fobj.seek(offset)
                return st.fobj.read(size)
        # read-only fast path: assemble from chunks (cache-backed), no temp file
        return self.store.read_range(st.ino, offset, size)

    def write(self, st: _FileState, data: bytes, offset: int) -> int:
        with st.lock:
            self._materialize(st)
            st.fobj.seek(offset)
            st.fobj.write(data)
            st.dirty = True
            return len(data)

    def truncate(self, st: _FileState, length: int) -> None:
        with st.lock:
            self._materialize(st)
            st.fobj.truncate(length)
            st.dirty = True

    def _flush_locked(self, st: _FileState) -> None:
        if not st.dirty:
            return
        st.fobj.flush()
        size = st.fobj.seek(0, os.SEEK_END)
        st.fobj.seek(0)
        self.store.put_file(st.ino, st.fobj, size)
        st.dirty = False

    def flush(self, st: _FileState) -> None:
        with st.lock:
            self._flush_locked(st)

    def release(self, st: _FileState) -> None:
        with self.lock:
            st.refcount -= 1
            if st.refcount > 0:
                return
            self._states.pop(st.ino, None)
        with st.lock:
            self._flush_locked(st)
            st.fobj.close()
            try:
                st.temp_path.unlink()
            except OSError:
                pass

    # ----- truncate without an open handle (FUSE truncate on a path) -------
    def truncate_path(self, ino: int, length: int) -> None:
        st = self.open(ino, for_write=True)
        try:
            self.truncate(st, length)
            self.flush(st)
        finally:
            self.release(st)
