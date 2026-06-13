"""SQLite metadata index — the source of truth for the filesystem tree.

Telegram only holds opaque content blobs; everything that makes those blobs a
*filesystem* (names, the directory tree, permissions, timestamps, the
file->chunk->blob mapping, hardlink/refcounts) lives here.

Concurrency: fusepy dispatches callbacks on many threads. We use one connection
(``check_same_thread=False``) guarded by a single re-entrant lock, in WAL mode.
Metadata ops are tiny, so the coarse lock is plenty fast and trivially correct.
"""

from __future__ import annotations

import os
import sqlite3
import stat
import threading
import time
from dataclasses import dataclass
from pathlib import Path

ROOT_INO = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS inodes (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    kind   TEXT NOT NULL,            -- 'f' file, 'd' dir, 'l' symlink
    mode   INTEGER NOT NULL,         -- full st_mode (type bits + perms)
    uid    INTEGER NOT NULL DEFAULT 0,
    gid    INTEGER NOT NULL DEFAULT 0,
    size   INTEGER NOT NULL DEFAULT 0,
    atime  REAL NOT NULL,
    mtime  REAL NOT NULL,
    ctime  REAL NOT NULL,
    nlink  INTEGER NOT NULL DEFAULT 1,
    target TEXT                       -- symlink target, else NULL
);
CREATE TABLE IF NOT EXISTS dirents (
    parent INTEGER NOT NULL,
    name   TEXT NOT NULL,
    child  INTEGER NOT NULL,
    PRIMARY KEY (parent, name)
);
CREATE INDEX IF NOT EXISTS ix_dirents_child ON dirents(child);
CREATE TABLE IF NOT EXISTS chunks (
    inode INTEGER NOT NULL,
    idx   INTEGER NOT NULL,
    off   INTEGER NOT NULL,
    len   INTEGER NOT NULL,
    sha   TEXT NOT NULL,
    PRIMARY KEY (inode, idx)
);
CREATE TABLE IF NOT EXISTS blobs (
    sha      TEXT PRIMARY KEY,
    handle   INTEGER NOT NULL,        -- Telegram message id
    len      INTEGER NOT NULL,
    refcount INTEGER NOT NULL DEFAULT 0
);
"""


@dataclass
class Inode:
    id: int
    kind: str
    mode: int
    uid: int
    gid: int
    size: int
    atime: float
    mtime: float
    ctime: float
    nlink: int
    target: str | None

    @property
    def is_dir(self) -> bool:
        return self.kind == "d"

    @property
    def is_link(self) -> bool:
        return self.kind == "l"


class Meta:
    def __init__(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript(_SCHEMA)
        self.lock = threading.RLock()
        self._ensure_root()

    def close(self) -> None:
        with self.lock:
            self.db.commit()
            self.db.close()

    def _ensure_root(self) -> None:
        with self.lock:
            row = self.db.execute(
                "SELECT id FROM inodes WHERE id=?", (ROOT_INO,)
            ).fetchone()
            if row:
                return
            now = time.time()
            mode = stat.S_IFDIR | 0o755
            self.db.execute(
                "INSERT INTO inodes(id,kind,mode,uid,gid,size,atime,mtime,ctime,nlink,target)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (ROOT_INO, "d", mode, os.getuid(), os.getgid(), 0, now, now, now, 2, None),
            )
            self.db.commit()

    # ----- inode access ----------------------------------------------------
    def get_inode(self, ino: int) -> Inode | None:
        with self.lock:
            r = self.db.execute("SELECT * FROM inodes WHERE id=?", (ino,)).fetchone()
        return self._row_to_inode(r) if r else None

    @staticmethod
    def _row_to_inode(r: sqlite3.Row) -> Inode:
        return Inode(
            id=r["id"], kind=r["kind"], mode=r["mode"], uid=r["uid"], gid=r["gid"],
            size=r["size"], atime=r["atime"], mtime=r["mtime"], ctime=r["ctime"],
            nlink=r["nlink"], target=r["target"],
        )

    def lookup(self, parent: int, name: str) -> Inode | None:
        with self.lock:
            r = self.db.execute(
                "SELECT child FROM dirents WHERE parent=? AND name=?", (parent, name)
            ).fetchone()
            if not r:
                return None
            return self.get_inode(r["child"])

    def resolve(self, path: str) -> Inode | None:
        """Resolve an absolute path (no symlink following) to an inode."""
        ino = ROOT_INO
        cur = self.get_inode(ino)
        for part in [p for p in path.split("/") if p]:
            if cur is None or not cur.is_dir:
                return None
            cur = self.lookup(cur.id, part)
            if cur is None:
                return None
        return cur

    def readdir(self, parent: int) -> list[tuple[str, int]]:
        with self.lock:
            rows = self.db.execute(
                "SELECT name, child FROM dirents WHERE parent=? ORDER BY name",
                (parent,),
            ).fetchall()
        return [(r["name"], r["child"]) for r in rows]

    def child_count(self, parent: int) -> int:
        with self.lock:
            r = self.db.execute(
                "SELECT COUNT(*) c FROM dirents WHERE parent=?", (parent,)
            ).fetchone()
        return r["c"]

    # ----- mutations -------------------------------------------------------
    def create(
        self, parent: int, name: str, mode: int, kind: str,
        uid: int = 0, gid: int = 0, target: str | None = None,
    ) -> Inode:
        now = time.time()
        nlink = 2 if kind == "d" else 1
        with self.lock:
            cur = self.db.execute(
                "INSERT INTO inodes(kind,mode,uid,gid,size,atime,mtime,ctime,nlink,target)"
                " VALUES(?,?,?,?,?,?,?,?,?,?)",
                (kind, mode, uid, gid, 0, now, now, now, nlink, target),
            )
            ino = cur.lastrowid
            self.db.execute(
                "INSERT INTO dirents(parent,name,child) VALUES(?,?,?)",
                (parent, name, ino),
            )
            if kind == "d":
                # new subdir bumps parent's link count (its '..' entry)
                self.db.execute(
                    "UPDATE inodes SET nlink=nlink+1 WHERE id=?", (parent,)
                )
            self.db.commit()
        return self.get_inode(ino)  # type: ignore[return-value]

    def link(self, parent: int, name: str, target_ino: int) -> None:
        """Add a hardlink (new dirent) to an existing file inode."""
        with self.lock:
            self.db.execute(
                "INSERT INTO dirents(parent,name,child) VALUES(?,?,?)",
                (parent, name, target_ino),
            )
            self.db.execute(
                "UPDATE inodes SET nlink=nlink+1, ctime=? WHERE id=?",
                (time.time(), target_ino),
            )
            self.db.commit()

    def unlink_dirent(self, parent: int, name: str) -> tuple[int, int]:
        """Remove a dirent. Returns (child_ino, remaining_nlink).

        The caller is responsible for freeing chunks/blobs when nlink hits 0.
        """
        with self.lock:
            r = self.db.execute(
                "SELECT child FROM dirents WHERE parent=? AND name=?", (parent, name)
            ).fetchone()
            if not r:
                raise FileNotFoundError(name)
            child = r["child"]
            kind = self.db.execute(
                "SELECT kind FROM inodes WHERE id=?", (child,)
            ).fetchone()["kind"]
            self.db.execute(
                "DELETE FROM dirents WHERE parent=? AND name=?", (parent, name)
            )
            self.db.execute(
                "UPDATE inodes SET nlink=nlink-1, ctime=? WHERE id=?",
                (time.time(), child),
            )
            if kind == "d":
                self.db.execute(
                    "UPDATE inodes SET nlink=nlink-1 WHERE id=?", (parent,)
                )
            nlink = self.db.execute(
                "SELECT nlink FROM inodes WHERE id=?", (child,)
            ).fetchone()["nlink"]
            if nlink <= 0:
                self.db.execute("DELETE FROM inodes WHERE id=?", (child,))
            self.db.commit()
            return child, nlink

    def rename(self, op: int, oname: str, np: int, nname: str) -> int | None:
        """Move/rename a dirent. If the destination exists it is replaced.

        Returns the inode that the destination name previously pointed at (so
        the caller can GC its chunks if it became unreferenced), else None.
        """
        with self.lock:
            r = self.db.execute(
                "SELECT child FROM dirents WHERE parent=? AND name=?", (op, oname)
            ).fetchone()
            if not r:
                raise FileNotFoundError(oname)
            src = r["child"]
            src_kind = self.db.execute(
                "SELECT kind FROM inodes WHERE id=?", (src,)
            ).fetchone()["kind"]

            replaced: int | None = None
            dst = self.db.execute(
                "SELECT child FROM dirents WHERE parent=? AND name=?", (np, nname)
            ).fetchone()
            if dst:
                replaced = dst["child"]
                rk = self.db.execute(
                    "SELECT kind FROM inodes WHERE id=?", (replaced,)
                ).fetchone()["kind"]
                self.db.execute(
                    "DELETE FROM dirents WHERE parent=? AND name=?", (np, nname)
                )
                self.db.execute(
                    "UPDATE inodes SET nlink=nlink-1 WHERE id=?", (replaced,)
                )
                if rk == "d":
                    self.db.execute(
                        "UPDATE inodes SET nlink=nlink-1 WHERE id=?", (np,)
                    )
                rem = self.db.execute(
                    "SELECT nlink FROM inodes WHERE id=?", (replaced,)
                ).fetchone()["nlink"]
                if rem > 0:
                    replaced = None  # still referenced elsewhere; nothing to GC
                else:
                    self.db.execute("DELETE FROM inodes WHERE id=?", (replaced,))

            self.db.execute(
                "UPDATE dirents SET parent=?, name=? WHERE parent=? AND name=?",
                (np, nname, op, oname),
            )
            if src_kind == "d" and op != np:
                self.db.execute("UPDATE inodes SET nlink=nlink-1 WHERE id=?", (op,))
                self.db.execute("UPDATE inodes SET nlink=nlink+1 WHERE id=?", (np,))
            self.db.commit()
            return replaced

    def setattr(
        self, ino: int, *, mode: int | None = None, uid: int | None = None,
        gid: int | None = None, size: int | None = None,
        atime: float | None = None, mtime: float | None = None,
    ) -> None:
        sets, vals = [], []
        for col, val in (
            ("mode", mode), ("uid", uid), ("gid", gid), ("size", size),
            ("atime", atime), ("mtime", mtime),
        ):
            if val is not None:
                sets.append(f"{col}=?")
                vals.append(val)
        if not sets:
            return
        sets.append("ctime=?")
        vals.append(time.time())
        vals.append(ino)
        with self.lock:
            self.db.execute(f"UPDATE inodes SET {','.join(sets)} WHERE id=?", vals)
            self.db.commit()

    def set_size(self, ino: int, size: int) -> None:
        with self.lock:
            self.db.execute(
                "UPDATE inodes SET size=?, mtime=?, ctime=? WHERE id=?",
                (size, time.time(), time.time(), ino),
            )
            self.db.commit()

    # ----- chunks ----------------------------------------------------------
    def get_chunks(self, ino: int) -> list[sqlite3.Row]:
        with self.lock:
            return self.db.execute(
                "SELECT idx,off,len,sha FROM chunks WHERE inode=? ORDER BY idx", (ino,)
            ).fetchall()

    def get_chunk_shas(self, ino: int) -> list[str]:
        with self.lock:
            rows = self.db.execute(
                "SELECT sha FROM chunks WHERE inode=? ORDER BY idx", (ino,)
            ).fetchall()
        return [r["sha"] for r in rows]

    def set_chunks(self, ino: int, chunks: list[tuple[int, int, int, str]]) -> None:
        """Replace the chunk list for an inode. chunks = [(idx,off,len,sha),...]."""
        with self.lock:
            self.db.execute("DELETE FROM chunks WHERE inode=?", (ino,))
            if chunks:
                self.db.executemany(
                    "INSERT INTO chunks(inode,idx,off,len,sha) VALUES(?,?,?,?,?)",
                    [(ino, *c) for c in chunks],
                )
            self.db.commit()

    # ----- blobs (content-addressed, refcounted, for dedup) ----------------
    def blob_get(self, sha: str) -> sqlite3.Row | None:
        with self.lock:
            return self.db.execute(
                "SELECT sha,handle,len,refcount FROM blobs WHERE sha=?", (sha,)
            ).fetchone()

    def blob_create(self, sha: str, handle: int, length: int) -> None:
        with self.lock:
            self.db.execute(
                "INSERT INTO blobs(sha,handle,len,refcount) VALUES(?,?,?,1)",
                (sha, handle, length),
            )
            self.db.commit()

    def blob_incref(self, sha: str) -> None:
        with self.lock:
            self.db.execute(
                "UPDATE blobs SET refcount=refcount+1 WHERE sha=?", (sha,)
            )
            self.db.commit()

    def blob_decref(self, sha: str) -> int | None:
        """Decrement a blob's refcount; if it reaches 0, delete the row and
        return its Telegram handle so the caller can remove the message."""
        with self.lock:
            r = self.db.execute(
                "SELECT handle,refcount FROM blobs WHERE sha=?", (sha,)
            ).fetchone()
            if not r:
                return None
            if r["refcount"] <= 1:
                self.db.execute("DELETE FROM blobs WHERE sha=?", (sha,))
                self.db.commit()
                return r["handle"]
            self.db.execute(
                "UPDATE blobs SET refcount=refcount-1 WHERE sha=?", (sha,)
            )
            self.db.commit()
            return None
