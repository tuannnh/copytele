"""Crash recovery for the write-back cache.

New files are written into ``<cache>/wb/<inode>.wb`` and uploaded to Telegram a
little later (the lazy flush). If the process is killed in between -- or a
graceful stop runs out of time while a big backlog is still draining -- those
files only exist on disk. Plain startup deletes leftover temp files, so without
this step every unflushed upload would be lost.

A leftover is safe to upload when its inode exists, is a regular file and has
**no chunks stored yet**: for such a never-flushed file the temp file *is* the
whole content. A temp file of an inode that already has chunks is a sparse
overlay of a modification, which cannot be reconstructed after a crash, so it
is skipped (and cleaned up by the normal startup).
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

log = logging.getLogger("tgfs.recover")


def find_recoverable(meta, wb_dir) -> list[tuple[int, Path, int]]:
    out = []
    for p in sorted(Path(wb_dir).glob("*.wb")):
        try:
            ino = int(p.stem)
        except ValueError:
            continue
        node = meta.get_inode(ino)
        if node is None or node.is_dir:
            continue  # deleted meanwhile, or not a file
        if meta.get_chunks(ino):
            continue  # modification overlay of an existing file: not recoverable
        size = p.stat().st_size
        if size == 0:
            continue
        out.append((ino, p, size))
    return out


def recover_stale(store, meta, wb_dir, workers: int = 8) -> tuple[int, int]:
    """Upload leftover write-back files. Returns (recovered, failed)."""
    todo = find_recoverable(meta, wb_dir)
    if not todo:
        return 0, 0
    total = sum(s for _, _, s in todo)
    log.info("recovering %d unflushed file(s), %.2f GB", len(todo), total / 1e9)

    def one(item):
        ino, path, size = item
        with open(path, "rb") as f:
            # the client's mtime was already stored via utimens: keep it
            store.put_file(ino, f, size, keep_mtime=True)
        path.unlink()
        return size

    ok = bad = done_bytes = 0
    with ThreadPoolExecutor(max(1, workers), thread_name_prefix="tgfs-recover") as ex:
        futs = {ex.submit(one, it): it for it in todo}
        for n, fut in enumerate(as_completed(futs), 1):
            try:
                done_bytes += fut.result()
                ok += 1
            except Exception:
                bad += 1
                log.exception("could not recover inode %d; leaving %s", futs[fut][0], futs[fut][1])
            if n % 25 == 0 or n == len(todo):
                log.info("recovery: %d/%d files, %.2f/%.2f GB", n, len(todo), done_bytes / 1e9, total / 1e9)
    log.info("recovery done: %d recovered, %d failed", ok, bad)
    return ok, bad
