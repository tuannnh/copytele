"""Chunking + content-addressed dedup, bridging the metadata index and the
blob backend.

A file's content is split into fixed-size chunks. Each chunk is hashed
(sha256); identical chunks share one Telegram blob (refcounted), so duplicate
data — across files or re-uploads — is stored once. Writes reconcile the new
chunk list against the old one: shared blobs survive untouched, genuinely new
chunks are uploaded, and orphaned blobs are deleted from Telegram.
"""

from __future__ import annotations

import hashlib
import threading
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import BinaryIO, Iterator

from .backend import Backend
from .meta import Meta


class Store:
    def __init__(
        self, meta: Meta, backend: Backend, chunk_size: int, cache=None,
        *, upload_workers: int = 1, download_workers: int = 1,
        readahead_chunks: int = 0,
    ) -> None:
        self.meta = meta
        self.backend = backend
        self.chunk_size = chunk_size
        self.cache = cache  # optional ReadCache
        self.upload_workers = max(1, upload_workers)
        self.download_workers = max(1, download_workers)
        self.readahead = max(0, readahead_chunks) if cache is not None else 0
        self._up = ThreadPoolExecutor(self.upload_workers, thread_name_prefix="tgfs-up")
        self._down = ThreadPoolExecutor(
            self.download_workers, thread_name_prefix="tgfs-down"
        )
        self._blob_lock = threading.Lock()  # serializes create/incref/decref races
        self._ra_lock = threading.Lock()
        self._ra_queued: set[str] = set()  # shas with a prefetch task queued
        self._last_end: dict[int, int] = {}  # ino -> end offset of last read

    def close(self) -> None:
        self._up.shutdown(wait=False, cancel_futures=True)
        self._down.shutdown(wait=False, cancel_futures=True)

    # ----- writing -----------------------------------------------------------
    def _upload_blob(self, sha: str, data: bytes) -> None:
        """Upload a new chunk and register it (refcount 1), or, if a concurrent
        writer registered the same sha first, drop our copy and incref theirs."""
        handle = self.backend.upload(data)
        with self._blob_lock:
            if self.meta.blob_get(sha) is None:
                self.meta.blob_create(sha, handle, len(data))
                return
            self.meta.blob_incref(sha)
        self.backend.delete(handle)

    def _release(self, sha: str) -> None:
        with self._blob_lock:  # vs. put_file's check-then-incref
            handle = self.meta.blob_decref(sha)
        if handle is not None:
            self.backend.delete(handle)

    def put_file(
        self, ino: int, src: BinaryIO, size: int, dirty: set[int] | None = None
    ) -> None:
        """Replace inode `ino`'s content with the bytes read from `src`.

        `src` is positioned at 0 and yields exactly `size` bytes. Chunks upload
        concurrently (bounded by `upload_workers`, so at most that many chunks
        are held in memory); the chunk list keeps file order regardless.

        If `dirty` is given, only those chunk indexes (plus any chunk without a
        matching stored one) are read/hashed/uploaded; the rest reuse their
        existing blob and `src` is never read for them (it may be sparse).
        """
        old_rows = self.meta.get_chunks(ino)
        old_shas = [r["sha"] for r in old_rows]

        new: list[tuple[int, int, int, str]] = []  # (idx, off, len, sha)
        acquired: list[str] = []  # refs taken so far (rolled back on failure)
        pending: dict[str, Future] = {}  # sha -> upload in flight in this call
        dups: list[str] = []  # repeats of a pending sha; incref'd after upload
        running: set[Future] = set()
        try:
            idx = 0
            off = 0
            while off < size:
                want = min(self.chunk_size, size - off)
                if (
                    dirty is not None and idx not in dirty and idx < len(old_rows)
                    and old_rows[idx]["off"] == off and old_rows[idx]["len"] == want
                ):
                    sha = old_rows[idx]["sha"]
                    with self._blob_lock:
                        known = self.meta.blob_get(sha) is not None
                        if known:
                            self.meta.blob_incref(sha)
                    if not known:
                        raise OSError(f"clean chunk {idx} of inode {ino}: blob missing")
                    acquired.append(sha)
                    new.append((idx, off, want, sha))
                    idx += 1
                    off += want
                    src.seek(off)
                    continue
                data = src.read(want)
                if not data:
                    break
                sha = hashlib.sha256(data).hexdigest()
                # take the ref BEFORE dropping the old ones, so a blob shared
                # between old and new content is never deleted then re-uploaded.
                if sha in pending:
                    dups.append(sha)
                else:
                    with self._blob_lock:
                        known = self.meta.blob_get(sha) is not None
                        if known:
                            self.meta.blob_incref(sha)
                    if known:
                        acquired.append(sha)
                    else:
                        while len(running) >= self.upload_workers:
                            done, running = wait(running, return_when=FIRST_COMPLETED)
                            for f in done:
                                f.result()
                        fut = self._up.submit(self._upload_blob, sha, data)
                        pending[sha] = fut
                        running.add(fut)
                        acquired.append(sha)
                new.append((idx, off, len(data), sha))
                idx += 1
                off += len(data)
            for fut in pending.values():
                fut.result()
            for sha in dups:
                self.meta.blob_incref(sha)
                acquired.append(sha)
        except BaseException:
            for fut in pending.values():
                fut.cancel()
            wait(pending.values())
            for sha in acquired:
                if sha in pending and (pending[sha].cancelled() or pending[sha].exception()):
                    continue  # never registered
                self._release(sha)
            raise

        self.meta.set_chunks(ino, new)
        self.meta.set_size(ino, off)

        for sha in old_shas:
            self._release(sha)

    def free_file(self, ino: int) -> None:
        """Release all blobs referenced by an inode (used when nlink hits 0)."""
        for sha in self.meta.get_chunk_shas(ino):
            self._release(sha)
        self.meta.set_chunks(ino, [])

    # ----- reading -----------------------------------------------------------
    def _fetcher(self, sha: str):
        row = self.meta.blob_get(sha)
        if row is None:
            raise FileNotFoundError(f"blob {sha} not in index")
        handle = row["handle"]

        def load() -> bytes:
            data = self.backend.download(handle)
            if hashlib.sha256(data).hexdigest() != sha:  # catches torn transfers
                raise OSError(f"blob {sha[:12]} failed integrity check")
            return data

        return load

    def read_chunk(self, sha: str, lo: int | None = None, hi: int | None = None) -> bytes:
        """Return a chunk (or its [lo:hi] slice), via the cache when present."""
        loader = self._fetcher(sha)
        if self.cache is not None:
            return self.cache.get_range(sha, lo, hi, loader)
        data = loader()
        return data if lo is None else data[lo:hi]

    def iter_chunks(self, shas: list[str]) -> Iterator[bytes]:
        """Yield chunk contents in order, downloading `download_workers` ahead."""
        window: deque[Future] = deque()
        it = iter(shas)
        try:
            for sha in it:
                window.append(self._down.submit(self.read_chunk, sha))
                if len(window) >= self.download_workers:
                    yield window.popleft().result()
            while window:
                yield window.popleft().result()
        finally:
            for f in window:
                f.cancel()

    def _prefetch(self, shas: list[str]) -> None:
        for sha in shas:
            with self._ra_lock:
                if sha in self._ra_queued or self.cache.has(sha):
                    continue
                self._ra_queued.add(sha)
            try:
                loader = self._fetcher(sha)
            except FileNotFoundError:
                with self._ra_lock:
                    self._ra_queued.discard(sha)
                continue
            self._down.submit(self._safe_prefetch, sha, loader)

    def _safe_prefetch(self, sha: str, loader) -> None:
        try:
            self.cache.prefetch(sha, loader)
        except Exception:
            pass  # best-effort; a real read will surface any error
        finally:
            with self._ra_lock:
                self._ra_queued.discard(sha)

    def read_range(self, ino: int, offset: int, length: int) -> bytes:
        """Assemble a byte range from the inode's chunks.

        Cached chunks are range-read from disk (not loaded whole); missing
        chunks needed by this read download in parallel, and the next
        `readahead_chunks` chunks are prefetched in the background.
        """
        if length <= 0:
            return b""
        end = offset + length
        chunks = self.meta.get_chunks(ino)
        hit = [
            (i, ch) for i, ch in enumerate(chunks)
            if ch["off"] + ch["len"] > offset and ch["off"] < end
        ]
        if not hit:
            return b""

        def part(ch):
            lo = max(offset, ch["off"]) - ch["off"]
            hi = min(end, ch["off"] + ch["len"]) - ch["off"]
            return self.read_chunk(ch["sha"], lo, hi)

        if len(hit) == 1:
            out = part(hit[0][1])
        else:
            futs = [self._down.submit(part, ch) for _, ch in hit]
            out = b"".join(f.result() for f in futs)

        sequential = self._last_end.get(ino) == offset
        if len(self._last_end) > 4096:
            self._last_end.clear()
        self._last_end[ino] = end
        if self.readahead and sequential:
            nxt = hit[-1][0] + 1
            self._prefetch([c["sha"] for c in chunks[nxt : nxt + self.readahead]])
        return out
