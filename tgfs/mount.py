"""pyfuse3 adapter + mount entrypoint.

Thin translation layer between pyfuse3's asynchronous, inode-based low-level API
and the synchronous :class:`~tgfs.fuse_ops.FsCore`. Each operation is offloaded
to a worker thread with ``trio.to_thread.run_sync`` so that blocking Telegram
round-trips (made via the AsyncLoop bridge) never stall pyfuse3's trio event
loop — keeping many concurrent reads/writes in flight.
"""

from __future__ import annotations

import errno
import functools
import logging
import os
import stat
import time

import pyfuse3
import trio

from .asyncbridge import AsyncLoop
from .cache import ReadCache, WritebackManager
from .config import Config
from .fuse_ops import FsCore
from .meta import Inode, Meta
from .store import Store
from .telegram import TelegramBackend

log = logging.getLogger("tgfs.mount")

ATTR_TIMEOUT = 5.0


def _name(b: bytes) -> str:
    return os.fsdecode(b)


def _bname(s: str) -> bytes:
    return os.fsencode(s)


class TgfsOperations(pyfuse3.Operations):
    enable_writeback_cache = False
    supports_dot_lookup = True

    def __init__(self, core: FsCore) -> None:
        super().__init__()
        self.core = core

    # ----- helpers ---------------------------------------------------------
    async def _call(self, fn, *args, **kwargs):
        try:
            return await trio.to_thread.run_sync(
                functools.partial(fn, *args, **kwargs)
            )
        except OSError as e:
            raise pyfuse3.FUSEError(e.errno or errno.EIO)

    def _attr(self, n: Inode) -> pyfuse3.EntryAttributes:
        a = pyfuse3.EntryAttributes()
        a.st_ino = n.id
        a.st_mode = n.mode
        a.st_nlink = n.nlink
        a.st_uid = n.uid
        a.st_gid = n.gid
        a.st_rdev = 0
        if n.is_link and n.target is not None:
            a.st_size = len(_bname(n.target))
        else:
            a.st_size = n.size
        a.st_blksize = 4096
        a.st_blocks = (n.size + 511) // 512
        a.st_atime_ns = int(n.atime * 1e9)
        a.st_mtime_ns = int(n.mtime * 1e9)
        a.st_ctime_ns = int(n.ctime * 1e9)
        a.generation = 0
        a.entry_timeout = ATTR_TIMEOUT
        a.attr_timeout = ATTR_TIMEOUT
        return a

    # ----- metadata --------------------------------------------------------
    async def lookup(self, parent_inode, name, ctx=None):
        nm = _name(name)
        if nm == ".":
            n = await self._call(self.core.getattr, parent_inode)
        elif nm == "..":
            n = await self._call(self.core.getattr, parent_inode)  # best-effort
        else:
            n = await self._call(self.core.lookup, parent_inode, nm)
        return self._attr(n)

    async def getattr(self, inode, ctx=None):
        n = await self._call(self.core.getattr, inode)
        return self._attr(n)

    async def setattr(self, inode, attr, fields, fh, ctx):
        kw = {}
        if fields.update_mode:
            kw["mode"] = attr.st_mode
        if fields.update_uid:
            kw["uid"] = attr.st_uid
        if fields.update_gid:
            kw["gid"] = attr.st_gid
        if fields.update_size:
            kw["size"] = attr.st_size
        if fields.update_atime:
            kw["atime"] = attr.st_atime_ns / 1e9
        elif getattr(fields, "update_atime_now", False):
            kw["atime"] = time.time()
        if fields.update_mtime:
            kw["mtime"] = attr.st_mtime_ns / 1e9
        elif getattr(fields, "update_mtime_now", False):
            kw["mtime"] = time.time()
        n = await self._call(self.core.setattr, inode, **kw)
        return self._attr(n)

    async def forget(self, inode_list):
        # inodes are persistent DB ids; lifetime is managed by nlink/open state.
        return None

    # ----- directories -----------------------------------------------------
    async def opendir(self, inode, ctx):
        await self._call(self.core.getattr, inode)  # ENOENT/validation
        return inode

    async def readdir(self, fh, start_id, token):
        entries = await self._call(self.core.readdir, fh)
        for i, (name, child) in enumerate(entries):
            nid = i + 1
            if nid <= start_id:
                continue
            node = await self._call(self.core.getattr, child)
            if not pyfuse3.readdir_reply(token, _bname(name), self._attr(node), nid):
                break

    async def releasedir(self, fh):
        return None

    async def mkdir(self, parent_inode, name, mode, ctx):
        n = await self._call(
            self.core.mkdir, parent_inode, _name(name), mode, ctx.uid, ctx.gid
        )
        return self._attr(n)

    async def rmdir(self, parent_inode, name, ctx):
        await self._call(self.core.rmdir, parent_inode, _name(name))

    # ----- files -----------------------------------------------------------
    async def create(self, parent_inode, name, mode, flags, ctx):
        fh, node = await self._call(
            self.core.create, parent_inode, _name(name), mode, flags, ctx.uid, ctx.gid
        )
        return pyfuse3.FileInfo(fh=fh), self._attr(node)

    async def open(self, inode, flags, ctx):
        fh = await self._call(self.core.open, inode, flags)
        return pyfuse3.FileInfo(fh=fh)

    async def read(self, fh, off, size):
        return await self._call(self.core.read, fh, off, size)

    async def write(self, fh, off, buf):
        return await self._call(self.core.write, fh, off, bytes(buf))

    async def flush(self, fh):
        await self._call(self.core.flush, fh)

    async def fsync(self, fh, datasync):
        await self._call(self.core.fsync, fh)

    async def release(self, fh):
        await self._call(self.core.release, fh)

    async def unlink(self, parent_inode, name, ctx):
        await self._call(self.core.unlink, parent_inode, _name(name))

    async def rename(self, parent_old, name_old, parent_new, name_new, flags, ctx):
        await self._call(
            self.core.rename, parent_old, _name(name_old), parent_new, _name(name_new)
        )

    # ----- links -----------------------------------------------------------
    async def symlink(self, parent_inode, name, target, ctx):
        n = await self._call(
            self.core.symlink, parent_inode, _name(name), _name(target),
            ctx.uid, ctx.gid,
        )
        return self._attr(n)

    async def readlink(self, inode, ctx):
        target = await self._call(self.core.readlink, inode)
        return _bname(target)

    async def link(self, inode, new_parent_inode, new_name, ctx):
        n = await self._call(self.core.link, inode, new_parent_inode, _name(new_name))
        return self._attr(n)

    # ----- misc ------------------------------------------------------------
    async def access(self, inode, mode, ctx):
        return True

    async def statfs(self, ctx):
        d = self.core.statfs()
        s = pyfuse3.StatvfsData()
        for k, v in d.items():
            setattr(s, k, v)
        return s


def build_core(cfg: Config, *, connect: bool = True):
    """Wire backend + meta + cache + store + writeback into an FsCore.

    Returns (core, cleanup_callable). Used by mount() and by tests/tools.
    """
    loop = AsyncLoop()
    loop.start()
    backend = TelegramBackend(cfg, loop, connect=connect)
    meta = Meta(cfg.meta_db)
    cache = ReadCache(cfg.cache_dir, cfg.cache_cap)
    store = Store(meta, backend, cfg.chunk_size, cache)
    wb = WritebackManager(store, meta, cfg.cache_dir)
    core = FsCore(meta, store, wb)

    def cleanup():
        try:
            meta.close()
        finally:
            backend.close()
            loop.stop()

    return core, cleanup


def mount(cfg: Config, mountpoint: str, *, foreground: bool = True, debug: bool = False) -> int:
    os.makedirs(mountpoint, exist_ok=True)
    core, cleanup = build_core(cfg)
    ops = TgfsOperations(core)

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add("fsname=tgfs")
    if debug:
        fuse_options.add("debug")

    log.info("mounting tgfs at %s", mountpoint)
    pyfuse3.init(ops, mountpoint, fuse_options)
    try:
        trio.run(pyfuse3.main)
    except KeyboardInterrupt:
        log.info("interrupted; unmounting")
    finally:
        pyfuse3.close(unmount=True)
        cleanup()
    return 0
