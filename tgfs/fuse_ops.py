"""Synchronous filesystem core.

All filesystem semantics live here, expressed over integer inode numbers and
plain bytes/str — with no dependency on any particular FUSE binding. This makes
the logic directly unit-testable (see tests/test_fuse_posix.py) and keeps the
pyfuse3 adapter (mount.py) a thin translation layer.

Errors are raised as ``OSError`` with a proper ``errno`` so the adapter can map
them straight onto ``FUSEError``.
"""

from __future__ import annotations

import errno
import itertools
import os
import stat
import threading

from .cache import WritebackManager
from .meta import Inode, Meta
from .store import Store


def _err(code: int, msg: str = "") -> OSError:
    return OSError(code, msg or errno.errorcode.get(code, str(code)))


class FsCore:
    def __init__(self, meta: Meta, store: Store, wb: WritebackManager) -> None:
        self.meta = meta
        self.store = store
        self.wb = wb
        self._fh_seq = itertools.count(1)
        self._fh: dict[int, object] = {}  # fh -> _FileState
        self._fh_lock = threading.Lock()
        # inodes unlinked while still open: free their blobs once last handle closes
        self._pending_unlink: set[int] = set()

    # ----- attributes ------------------------------------------------------
    def getattr(self, ino: int) -> Inode:
        n = self.meta.get_inode(ino)
        if n is None:
            raise _err(errno.ENOENT)
        return n

    def lookup(self, parent: int, name: str) -> Inode:
        n = self.meta.lookup(parent, name)
        if n is None:
            raise _err(errno.ENOENT)
        return n

    def readdir(self, ino: int) -> list[tuple[str, int]]:
        n = self.meta.get_inode(ino)
        if n is None:
            raise _err(errno.ENOENT)
        if not n.is_dir:
            raise _err(errno.ENOTDIR)
        return self.meta.readdir(ino)

    def setattr(
        self, ino: int, *, mode: int | None = None, uid: int | None = None,
        gid: int | None = None, size: int | None = None,
        atime: float | None = None, mtime: float | None = None,
    ) -> Inode:
        n = self.meta.get_inode(ino)
        if n is None:
            raise _err(errno.ENOENT)
        if size is not None and not n.is_dir and size != n.size:
            self.wb.truncate_path(ino, size)
        self.meta.setattr(
            ino, mode=mode, uid=uid, gid=gid,
            size=None,  # size handled via truncate above
            atime=atime, mtime=mtime,
        )
        return self.getattr(ino)

    # ----- directory mutations ---------------------------------------------
    def _ensure_absent(self, parent: int, name: str) -> None:
        if self.meta.lookup(parent, name) is not None:
            raise _err(errno.EEXIST)

    def mkdir(self, parent: int, name: str, mode: int, uid: int, gid: int) -> Inode:
        self._ensure_absent(parent, name)
        return self.meta.create(
            parent, name, stat.S_IFDIR | (mode & 0o7777), "d", uid, gid
        )

    def rmdir(self, parent: int, name: str) -> None:
        n = self.meta.lookup(parent, name)
        if n is None:
            raise _err(errno.ENOENT)
        if not n.is_dir:
            raise _err(errno.ENOTDIR)
        if self.meta.child_count(n.id) > 0:
            raise _err(errno.ENOTEMPTY)
        self.meta.unlink_dirent(parent, name)

    def create_node(
        self, parent: int, name: str, mode: int, uid: int, gid: int
    ) -> Inode:
        self._ensure_absent(parent, name)
        return self.meta.create(
            parent, name, stat.S_IFREG | (mode & 0o7777), "f", uid, gid
        )

    def symlink(
        self, parent: int, name: str, target: str, uid: int, gid: int
    ) -> Inode:
        self._ensure_absent(parent, name)
        return self.meta.create(
            parent, name, stat.S_IFLNK | 0o777, "l", uid, gid, target=target
        )

    def readlink(self, ino: int) -> str:
        n = self.meta.get_inode(ino)
        if n is None or not n.is_link or n.target is None:
            raise _err(errno.EINVAL)
        return n.target

    def link(self, ino: int, new_parent: int, new_name: str) -> Inode:
        n = self.meta.get_inode(ino)
        if n is None:
            raise _err(errno.ENOENT)
        if n.is_dir:
            raise _err(errno.EPERM)
        self._ensure_absent(new_parent, new_name)
        self.meta.link(new_parent, new_name, ino)
        return self.getattr(ino)

    def unlink(self, parent: int, name: str) -> None:
        n = self.meta.lookup(parent, name)
        if n is None:
            raise _err(errno.ENOENT)
        if n.is_dir:
            raise _err(errno.EISDIR)
        ino = n.id
        _, nlink = self.meta.unlink_dirent(parent, name)
        if nlink <= 0:
            if self._is_open(ino):
                self._pending_unlink.add(ino)  # defer until last close
            else:
                self.store.free_file(ino)

    def rename(self, oldp: int, oldname: str, newp: int, newname: str) -> None:
        if self.meta.lookup(oldp, oldname) is None:
            raise _err(errno.ENOENT)
        dst = self.meta.lookup(newp, newname)
        if dst is not None and dst.is_dir and self.meta.child_count(dst.id) > 0:
            raise _err(errno.ENOTEMPTY)
        replaced = self.meta.rename(oldp, oldname, newp, newname)
        if replaced is not None:  # destination inode became unreferenced
            if self._is_open(replaced):
                self._pending_unlink.add(replaced)
            else:
                self.store.free_file(replaced)

    # ----- file handles & io ----------------------------------------------
    def _is_open(self, ino: int) -> bool:
        with self._fh_lock:
            return any(getattr(st, "ino", None) == ino for st in self._fh.values())

    def _new_fh(self, state) -> int:
        fh = next(self._fh_seq)
        with self._fh_lock:
            self._fh[fh] = state
        return fh

    def _get_state(self, fh: int):
        with self._fh_lock:
            st = self._fh.get(fh)
        if st is None:
            raise _err(errno.EBADF)
        return st

    @staticmethod
    def _wants_write(flags: int) -> bool:
        acc = flags & os.O_ACCMODE
        return acc in (os.O_WRONLY, os.O_RDWR) or bool(flags & os.O_APPEND)

    def open(self, ino: int, flags: int) -> int:
        n = self.meta.get_inode(ino)
        if n is None:
            raise _err(errno.ENOENT)
        if n.is_dir:
            raise _err(errno.EISDIR)
        for_write = self._wants_write(flags)
        if flags & os.O_TRUNC:
            self.wb.truncate_path(ino, 0)
        st = self.wb.open(ino, for_write=for_write)
        return self._new_fh(st)

    def create(
        self, parent: int, name: str, mode: int, flags: int, uid: int, gid: int
    ) -> tuple[int, Inode]:
        existing = self.meta.lookup(parent, name)
        if existing is not None:
            if flags & os.O_EXCL:
                raise _err(errno.EEXIST)
            node = existing
        else:
            node = self.meta.create(
                parent, name, stat.S_IFREG | (mode & 0o7777), "f", uid, gid
            )
        st = self.wb.open(node.id, for_write=True)
        return self._new_fh(st), node

    def read(self, fh: int, off: int, size: int) -> bytes:
        st = self._get_state(fh)
        return self.wb.read(st, size, off)

    def write(self, fh: int, off: int, data: bytes) -> int:
        st = self._get_state(fh)
        return self.wb.write(st, data, off)

    def flush(self, fh: int) -> None:
        st = self._get_state(fh)
        self.wb.flush(st)

    def fsync(self, fh: int) -> None:
        st = self._get_state(fh)
        self.wb.flush(st)

    def release(self, fh: int) -> None:
        with self._fh_lock:
            st = self._fh.pop(fh, None)
        if st is None:
            return
        self.wb.release(st)
        ino = getattr(st, "ino")
        if ino in self._pending_unlink and not self._is_open(ino):
            self._pending_unlink.discard(ino)
            self.store.free_file(ino)

    # ----- misc ------------------------------------------------------------
    def statfs(self) -> dict:
        bsize = 4096
        return {
            "f_bsize": bsize,
            "f_frsize": bsize,
            # advertise a large, mostly-free store (Telegram is effectively huge)
            "f_blocks": (1 << 50) // bsize,
            "f_bfree": (1 << 49) // bsize,
            "f_bavail": (1 << 49) // bsize,
            "f_files": 1 << 30,
            "f_ffree": 1 << 29,
            "f_favail": 1 << 29,
            "f_namemax": 255,
        }
