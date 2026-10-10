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

    Copy-on-write at chunk granularity: the temp file is sparse and a chunk is
    only downloaded when a read/partial write touches it. Flushing re-hashes and
    uploads only dirty chunks; clean ones keep their existing blob. With
    ``flush_delay`` > 0 the state outlives the last close, so a client that
    closes and reopens the file for every piece (up2k) neither re-downloads nor
    re-uploads anything until the file has been idle for ``flush_delay`` seconds
    (or fsync / unmount).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable

from .meta import Meta
from .store import Store

log = logging.getLogger("tgfs.wb")


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
        self.materialized = False  # write mode active: temp file is authoritative
        self.lock = threading.RLock()
        self.size = 0
        self.base: list[tuple[int, int, str]] = []  # (off, len, sha) as stored
        self.loaded: set[int] = set()  # chunk idx whose bytes are in the temp file
        self.dirty_idx: set[int] = set()  # base chunks modified (new ones implicit)
        self.idle_since = 0.0
        self.mtime_pinned = False  # utimens after the last write: flush keeps mtime


class WritebackManager:
    def __init__(
        self, store: Store, meta: Meta, cache_dir: str | Path,
        flush_delay: float = 0,
    ) -> None:
        self.store = store
        self.meta = meta
        self.flush_delay = flush_delay
        self.wb_dir = Path(cache_dir) / "wb"
        self.wb_dir.mkdir(parents=True, exist_ok=True)
        for p in self.wb_dir.iterdir():  # clear stale temp files from a crash
            try:
                p.unlink()
            except OSError:
                pass
        self.lock = threading.Lock()
        self._states: dict[int, _FileState] = {}
        self._stop = threading.Event()
        self._reaper = None
        if flush_delay > 0:
            self._reaper = threading.Thread(
                target=self._reap_loop, name="tgfs-wb-reaper", daemon=True
            )
            self._reaper.start()

    # ----- per-inode state -------------------------------------------------
    @property
    def _cs(self) -> int:
        return self.store.chunk_size

    def _materialize(self, st: _FileState) -> None:
        """Switch the inode to write mode (cheap: nothing is downloaded yet)."""
        if st.materialized:
            return
        rows = self.meta.get_chunks(st.ino)
        st.size = self.meta.get_inode(st.ino).size
        cs = self._cs
        st.base = [(r["off"], r["len"], r["sha"]) for r in rows]
        st.loaded = set()
        st.dirty_idx = set()
        aligned = all(
            off == i * cs and (ln == cs or i == len(st.base) - 1)
            for i, (off, ln, _) in enumerate(st.base)
        )
        if not aligned:
            # stored with a different chunk size: load everything, re-chunk all
            self._load(st, 0, len(st.base))
            st.dirty_idx = set(range(len(st.base)))
            st.base = []
            st.loaded = set()
        st.materialized = True

    def _load(self, st: _FileState, lo: int, hi: int, skip=()) -> None:
        """Pull base chunks [lo, hi) not yet in the temp file into it."""
        need = [
            i for i in range(lo, min(hi, len(st.base)))
            if i not in st.loaded and i not in skip
        ]
        if not need:
            return
        shas = [st.base[i][2] for i in need]
        for i, data in zip(need, self.store.iter_chunks(shas)):
            st.fobj.seek(st.base[i][0])
            st.fobj.write(data)
            st.loaded.add(i)
        # (dirty is *not* set: loading doesn't change content)

    def _prepare(self, st: _FileState, start: int, end: int, overwrite: bool) -> None:
        """Make bytes [start, end) of the temp file valid."""
        end = min(end, st.size)
        if end <= start or not st.base:
            return
        cs = self._cs
        lo, hi = start // cs, (end - 1) // cs + 1
        skip = set()
        if overwrite:  # fully covered chunks need no download
            for i in range(lo, min(hi, len(st.base))):
                off, ln, _ = st.base[i]
                if start <= off and end >= off + ln:
                    skip.add(i)
                    st.loaded.add(i)
        self._load(st, lo, hi, skip)

    def _grow(self, st: _FileState, new_size: int) -> None:
        """Logically extend the file with zeros (partial tail chunk changes)."""
        if new_size <= st.size:
            return
        cs = self._cs
        if st.base and st.size % cs != 0 and len(st.base) == st.size // cs + 1:
            b = len(st.base) - 1
            self._load(st, b, b + 1)
            st.dirty_idx.add(b)
        st.size = new_size

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

    def live_size(self, ino: int) -> int | None:
        """Current size if a write-mode state exists (meta may lag the flush)."""
        st = self._states.get(ino)
        return st.size if st is not None and st.materialized else None

    # ----- io --------------------------------------------------------------
    def read(self, st: _FileState, size: int, offset: int) -> bytes:
        with st.lock:
            if st.materialized:
                end = min(offset + size, st.size)
                if end <= offset:
                    return b""
                self._prepare(st, offset, end, overwrite=False)
                st.fobj.seek(offset)
                return st.fobj.read(end - offset)
        # read-only fast path: assemble from chunks (cache-backed), no temp file
        return self.store.read_range(st.ino, offset, size)

    def write(self, st: _FileState, data: bytes, offset: int) -> int:
        with st.lock:
            self._materialize(st)
            end = offset + len(data)
            self._prepare(st, offset, end, overwrite=True)
            self._grow(st, end)
            st.fobj.seek(offset)
            st.fobj.write(data)
            cs = self._cs
            if data:
                st.dirty_idx.update(range(offset // cs, (end - 1) // cs + 1))
            st.dirty = True
            st.mtime_pinned = False
            return len(data)

    def truncate(self, st: _FileState, length: int) -> None:
        with st.lock:
            self._materialize(st)
            cs = self._cs
            if length < st.size:
                b = length // cs
                if length % cs and b < len(st.base):
                    self._load(st, b, b + 1)  # keep the surviving prefix
                    st.dirty_idx.add(b)
                keep = -(-length // cs)
                st.base = st.base[:keep]
                st.loaded = {i for i in st.loaded if i < keep}
                st.dirty_idx = {i for i in st.dirty_idx if i < keep}
                st.size = length
            else:
                self._grow(st, length)
            st.fobj.truncate(length)
            st.dirty = True
            st.mtime_pinned = False

    def _flush_locked(self, st: _FileState) -> None:
        if not st.dirty:
            return
        st.fobj.truncate(st.size)  # extend with zeros up to the logical size
        st.fobj.flush()
        st.fobj.seek(0)
        old_n = len(st.base)
        self.store.put_file(st.ino, st.fobj, st.size, dirty=st.dirty_idx,
                            keep_mtime=st.mtime_pinned)
        st.mtime_pinned = False
        rows = self.meta.get_chunks(st.ino)
        st.base = [(r["off"], r["len"], r["sha"]) for r in rows]
        st.loaded |= set(range(old_n, len(st.base)))  # fresh chunks live in temp
        st.dirty_idx = set()
        st.dirty = False

    def flush(self, st: _FileState) -> None:
        """FUSE flush (runs on every close): lazy when a flush delay is set."""
        if self.flush_delay > 0:
            return
        with st.lock:
            self._flush_locked(st)

    def sync(self, st: _FileState) -> None:
        """fsync: always push to Telegram now."""
        with st.lock:
            self._flush_locked(st)

    def _close_state(self, st: _FileState) -> None:
        with st.lock:
            st.fobj.close()
            try:
                st.temp_path.unlink()
            except OSError:
                pass

    def release(self, st: _FileState) -> None:
        with self.lock:
            st.refcount -= 1
            if st.refcount > 0:
                return
            if self.flush_delay > 0 and st.dirty:
                st.idle_since = time.monotonic()  # reaper flushes it later
                return
            self._states.pop(st.ino, None)
        with st.lock:
            self._flush_locked(st)
        self._close_state(st)

    def discard(self, ino: int) -> None:
        """Drop an inode's pending state without flushing (it was deleted)."""
        with self.lock:
            st = self._states.pop(ino, None)
        if st is not None:
            st.dirty = False
            self._close_state(st)

    # ----- delayed flush ---------------------------------------------------
    def _reap_loop(self) -> None:
        while not self._stop.wait(1.0):
            try:
                self._reap_once()
            except Exception:  # never kill the reaper
                log.exception("writeback reaper error")

    def _reap_once(self, force: bool = False) -> None:
        now = time.monotonic()
        with self.lock:
            cands = [
                st for st in self._states.values()
                if st.refcount == 0 and (force or now - st.idle_since >= self.flush_delay)
            ]
        if not cands:
            return

        def flush_one(st: _FileState) -> bool:
            """Flush one idle file; True if it failed (it will be retried)."""
            try:
                with st.lock:
                    self._flush_locked(st)
            except Exception:
                log.exception("flush of inode %d failed; will retry", st.ino)
                st.idle_since = time.monotonic()
                return True
            with self.lock:
                done = st.refcount == 0 and self._states.get(st.ino) is st
                if done:
                    del self._states[st.ino]
            if done:
                self._close_state(st)
            return False

        # Small files upload over one connection each (~2 MB/s), so flush several
        # files at once (bounded by the upload workers; the store's own pool still
        # caps the number of chunk uploads in flight).
        workers = max(1, min(len(cands), self.store.upload_workers))
        if workers == 1:
            failed = [flush_one(st) for st in cands]
        else:
            with ThreadPoolExecutor(workers, thread_name_prefix="tgfs-flush") as ex:
                failed = list(ex.map(flush_one, cands))
        if force and any(failed):
            raise OSError(f"{sum(failed)} file(s) could not be flushed")

    def shutdown(self) -> None:
        """Flush everything pending (unmount / SIGTERM)."""
        self._stop.set()
        with self.lock:
            for st in self._states.values():
                st.refcount = 0
        self._reap_once(force=True)

    def pin_mtime(self, ino: int) -> None:
        """An explicit mtime was just stored: a pending flush must not clobber it."""
        st = self._states.get(ino)
        if st is not None:
            with st.lock:
                if st.dirty:
                    st.mtime_pinned = True

    # ----- truncate without an open handle (FUSE truncate on a path) -------
    def truncate_path(self, ino: int, length: int) -> None:
        st = self.open(ino, for_write=True)
        try:
            self.truncate(st, length)
            self.flush(st)
        finally:
            self.release(st)
