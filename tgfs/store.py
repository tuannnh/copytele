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
from typing import BinaryIO

from .backend import Backend
from .meta import Meta


class Store:
    def __init__(
        self, meta: Meta, backend: Backend, chunk_size: int, cache=None
    ) -> None:
        self.meta = meta
        self.backend = backend
        self.chunk_size = chunk_size
        self.cache = cache  # optional ReadCache

    def put_file(self, ino: int, src: BinaryIO, size: int) -> None:
        """Replace inode `ino`'s content with the bytes read from `src`.

        `src` is positioned at 0 and yields exactly `size` bytes.
        """
        old_shas = self.meta.get_chunk_shas(ino)

        new: list[tuple[int, int, int, str]] = []  # (idx, off, len, sha)
        idx = 0
        off = 0
        while True:
            data = src.read(self.chunk_size)
            if not data:
                break
            sha = hashlib.sha256(data).hexdigest()
            # incref-or-upload BEFORE we drop the old refs, so a blob shared
            # between old and new content is never deleted then re-uploaded.
            if self.meta.blob_get(sha) is None:
                handle = self.backend.upload(data)
                # a concurrent writer may have created it meanwhile; tolerate
                if self.meta.blob_get(sha) is None:
                    self.meta.blob_create(sha, handle, len(data))
                else:
                    self.backend.delete(handle)
                    self.meta.blob_incref(sha)
            else:
                self.meta.blob_incref(sha)
            new.append((idx, off, len(data), sha))
            idx += 1
            off += len(data)

        self.meta.set_chunks(ino, new)
        self.meta.set_size(ino, off)

        for sha in old_shas:
            handle = self.meta.blob_decref(sha)
            if handle is not None:
                self.backend.delete(handle)

    def free_file(self, ino: int) -> None:
        """Release all blobs referenced by an inode (used when nlink hits 0)."""
        for sha in self.meta.get_chunk_shas(ino):
            handle = self.meta.blob_decref(sha)
            if handle is not None:
                self.backend.delete(handle)
        self.meta.set_chunks(ino, [])

    def read_chunk(self, sha: str) -> bytes:
        row = self.meta.blob_get(sha)
        if row is None:
            raise FileNotFoundError(f"blob {sha} not in index")
        handle = row["handle"]
        if self.cache is not None:
            return self.cache.get(sha, lambda: self.backend.download(handle))
        return self.backend.download(handle)

    def read_range(self, ino: int, offset: int, length: int) -> bytes:
        """Assemble a byte range from the inode's chunks (no cache)."""
        if length <= 0:
            return b""
        out = bytearray()
        end = offset + length
        for ch in self.meta.get_chunks(ino):
            c_off, c_len, sha = ch["off"], ch["len"], ch["sha"]
            c_end = c_off + c_len
            if c_end <= offset or c_off >= end:
                continue
            data = self.read_chunk(sha)
            lo = max(offset, c_off) - c_off
            hi = min(end, c_end) - c_off
            out += data[lo:hi]
        return bytes(out)
