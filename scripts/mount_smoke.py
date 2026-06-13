#!/usr/bin/env python3
"""Real-kernel FUSE smoke test using the in-memory backend (no Telegram).

Mounts tgfs at a temp dir, then drives it through actual filesystem syscalls
from a worker thread to prove the whole pyfuse3 -> FsCore -> store -> backend
stack works under the real kernel. Exits non-zero on any failure.

Run:  .venv/bin/python scripts/mount_smoke.py
"""

from __future__ import annotations

import os
import shutil
import time
import sys
import tempfile
import threading
import traceback

import pyfuse3
import trio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# filled in once the trio loop is running, so the worker thread can stop it
_trio_token: dict[str, object] = {}
_token_ready = threading.Event()

from tgfs.backend import MemoryBackend
from tgfs.cache import ReadCache, WritebackManager
from tgfs.fuse_ops import FsCore
from tgfs.meta import Meta
from tgfs.mount import TgfsOperations
from tgfs.store import Store

failures: list[str] = []


def check(cond: bool, msg: str) -> None:
    print(("  ok  " if cond else " FAIL ") + msg)
    if not cond:
        failures.append(msg)


def workload(mnt: str, ready: threading.Event) -> None:
    ready.wait()
    try:
        # 1. write + read back a small file
        p = os.path.join(mnt, "hello.txt")
        with open(p, "wb") as f:
            f.write(b"hello telegram")
        with open(p, "rb") as f:
            check(f.read() == b"hello telegram", "small file roundtrip")
        check(os.path.getsize(p) == 14, "size reported correctly")

        # 2. mkdir + nested file + listdir
        d = os.path.join(mnt, "sub")
        os.mkdir(d)
        with open(os.path.join(d, "a"), "wb") as f:
            f.write(b"A")
        check(sorted(os.listdir(mnt)) == ["hello.txt", "sub"], "readdir top")
        check(os.listdir(d) == ["a"], "readdir sub")

        # 3. random write into the middle of an existing file
        with open(p, "r+b") as f:
            f.seek(6)
            f.write(b"WORLD!")
        # "hello telegram" with "WORLD!" written at offset 6 -> "hello WORLD!am"
        with open(p, "rb") as f:
            check(f.read() == b"hello WORLD!am", "random write")

        # 4. truncate
        os.truncate(p, 5)
        check(os.path.getsize(p) == 5, "truncate shrinks")

        # 5. a multi-chunk large file (chunk_size is tiny in this test)
        big = os.urandom(200_000)
        bp = os.path.join(mnt, "big.bin")
        with open(bp, "wb") as f:
            f.write(big)
        with open(bp, "rb") as f:
            check(f.read() == big, "large multi-chunk file roundtrip")
        # partial read at an offset
        with open(bp, "rb") as f:
            f.seek(123_456)
            check(f.read(1000) == big[123_456:124_456], "large file partial read")

        # 6. rename
        np = os.path.join(mnt, "renamed.bin")
        os.rename(bp, np)
        check(os.path.exists(np) and not os.path.exists(bp), "rename")

        # 7. symlink + readlink
        lp = os.path.join(mnt, "link")
        os.symlink("renamed.bin", lp)
        check(os.readlink(lp) == "renamed.bin", "symlink/readlink")

        # 8. hardlink
        hp = os.path.join(mnt, "hardlink.txt")
        os.link(p, hp)
        check(os.stat(p).st_nlink == 2, "hardlink bumps nlink")
        with open(hp, "rb") as f:
            check(f.read() == b"hello", "hardlink shares content")

        # 9. unlink
        os.unlink(p)
        check(not os.path.exists(p), "unlink removes name")
        check(os.path.exists(hp), "hardlink survives unlink of other name")

        # 10. chmod
        os.chmod(hp, 0o600)
        check((os.stat(hp).st_mode & 0o777) == 0o600, "chmod")
    except Exception:
        traceback.print_exc()
        failures.append("exception in workload")
    finally:
        # terminate must run inside the trio loop; hop over from this thread
        _token_ready.wait(5)
        trio.from_thread.run_sync(
            pyfuse3.terminate, trio_token=_trio_token.get("t")
        )


def main() -> int:
    work = tempfile.mkdtemp(prefix="tgfs-smoke-")
    mnt = os.path.join(work, "mnt")
    os.makedirs(mnt)

    def watchdog():
        import subprocess
        time.sleep(30)
        print("WATCHDOG: forcing unmount + exit", flush=True)
        subprocess.run(["fusermount3", "-uz", mnt])
        os._exit(3)

    threading.Thread(target=watchdog, daemon=True).start()

    meta = Meta(os.path.join(work, "meta.db"))
    backend = MemoryBackend()
    cache = ReadCache(os.path.join(work, "cache"), cap_bytes=1 << 20)
    store = Store(meta, backend, chunk_size=64 * 1024, cache=cache)
    wb = WritebackManager(store, meta, os.path.join(work, "cache"))
    core = FsCore(meta, store, wb)
    ops = TgfsOperations(core)

    opts = set(pyfuse3.default_options)
    opts.add("fsname=tgfs-smoke")
    pyfuse3.init(ops, mnt, opts)

    ready = threading.Event()
    t = threading.Thread(target=workload, args=(mnt, ready), daemon=True)
    t.start()
    ready.set()

    async def _main():
        _trio_token["t"] = trio.lowlevel.current_trio_token()
        _token_ready.set()
        await pyfuse3.main()

    try:
        trio.run(_main)
    finally:
        pyfuse3.close(unmount=True)
        meta.close()
        t.join(timeout=5)
        shutil.rmtree(work, ignore_errors=True)

    print()
    if failures:
        print(f"FAILED ({len(failures)}): " + "; ".join(failures))
        return 1
    print(f"ALL CHECKS PASSED (backend: {backend.uploads} uploads, "
          f"{backend.downloads} downloads, {backend.deletes} deletes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
