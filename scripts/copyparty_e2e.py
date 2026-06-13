#!/usr/bin/env python3
"""End-to-end: run unmodified copyparty on top of a tgfs mount (in-memory
backend, no Telegram) and exercise it over HTTP.

Proves the integration: copyparty's reads/writes go through the kernel -> FUSE
-> FsCore -> store -> backend. Because the mount runs in *this* process, we can
also confirm the bytes really landed in the backend (upload count > 0).

Run:  .venv/bin/python scripts/copyparty_e2e.py
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

import pyfuse3
import trio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tgfs.backend import MemoryBackend
from tgfs.cache import ReadCache, WritebackManager
from tgfs.fuse_ops import FsCore
from tgfs.meta import Meta
from tgfs.mount import TgfsOperations
from tgfs.store import Store

PORT = 3939
failures: list[str] = []
_token: dict[str, object] = {}
_token_ready = threading.Event()


def check(cond: bool, msg: str) -> None:
    print(("  ok  " if cond else " FAIL ") + msg, flush=True)
    if not cond:
        failures.append(msg)


def http(method: str, path: str, data: bytes | None = None) -> tuple[int, bytes]:
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}{path}", data=data, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def wait_port(port: int, timeout: float = 20) -> bool:
    import socket

    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(0.5)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.2)
    return False


def run_copyparty_and_test(mnt: str, hist: str, backend: MemoryBackend) -> None:
    _token_ready.wait(5)
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "copyparty",
            "-i", "127.0.0.1", "-p", str(PORT),
            "-v", f"{mnt}::A",        # share the mount at /, all perms for anon
            "--hist", hist,            # copyparty index/thumbs on LOCAL disk
            "-q", "--no-thumb", "--no-mtag",
        ],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        if not wait_port(PORT):
            failures.append("copyparty did not start listening")
            return

        payload = os.urandom(150_000)  # multi-chunk
        st, _ = http("PUT", "/report.bin", payload)
        check(st in (200, 201), f"PUT upload (status {st})")

        # the bytes really went through FUSE into the backend
        check(backend.uploads > 0, f"data reached backend ({backend.uploads} blobs)")

        st, body = http("GET", "/report.bin")
        check(st == 200 and body == payload, "GET download byte-identical")

        # directory listing (copyparty's JSON view) shows the file
        st, body = http("GET", "/?ls")
        check(st == 200 and b"report.bin" in body, "directory listing shows file")

        # the file is visible on the actual mountpoint too
        check(os.path.exists(os.path.join(mnt, "report.bin")), "file present on mount")

        # delete through copyparty
        st, _ = http("POST", "/report.bin?delete")
        check(st in (200, 302), f"delete via copyparty (status {st})")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        trio.from_thread.run_sync(pyfuse3.terminate, trio_token=_token.get("t"))


def main() -> int:
    work = tempfile.mkdtemp(prefix="tgfs-e2e-")
    mnt = os.path.join(work, "mnt")
    hist = os.path.join(work, "hist")
    os.makedirs(mnt)
    os.makedirs(hist)

    def watchdog():
        time.sleep(60)
        print("WATCHDOG: forcing unmount + exit", flush=True)
        subprocess.run(["fusermount3", "-uz", mnt])
        os._exit(3)

    threading.Thread(target=watchdog, daemon=True).start()

    meta = Meta(os.path.join(work, "meta.db"))
    backend = MemoryBackend()
    store = Store(meta, backend, chunk_size=64 * 1024,
                  cache=ReadCache(os.path.join(work, "cache"), 1 << 20))
    wb = WritebackManager(store, meta, os.path.join(work, "cache"))
    core = FsCore(meta, store, wb)
    ops = TgfsOperations(core)

    opts = set(pyfuse3.default_options)
    opts.add("fsname=tgfs-e2e")
    pyfuse3.init(ops, mnt, opts)

    threading.Thread(
        target=run_copyparty_and_test, args=(mnt, hist, backend), daemon=True
    ).start()

    async def _main():
        _token["t"] = trio.lowlevel.current_trio_token()
        _token_ready.set()
        await pyfuse3.main()

    try:
        trio.run(_main)
    finally:
        pyfuse3.close(unmount=True)
        meta.close()
        shutil.rmtree(work, ignore_errors=True)

    print()
    if failures:
        print(f"FAILED ({len(failures)}): " + "; ".join(failures))
        return 1
    print("COPYPARTY-ON-TGFS E2E PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
