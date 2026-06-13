"""POSIX-semantics tests driven directly against the synchronous FsCore.

These exercise the same code paths the pyfuse3 adapter calls, without needing
a real kernel mount — fast, deterministic, and CI-friendly.
"""

import errno
import os
import stat

import pytest

from tgfs.meta import ROOT_INO


def _create(core, parent, name, mode=0o644):
    fh, node = core.create(parent, name, mode, os.O_RDWR | os.O_CREAT, 0, 0)
    return fh, node


def _write(core, parent, name, data):
    fh, node = _create(core, parent, name)
    core.write(fh, 0, data)
    core.release(fh)
    return node.id


def test_root_getattr(core):
    n = core.getattr(ROOT_INO)
    assert n.is_dir and stat.S_ISDIR(n.mode)


def test_lookup_missing_raises_enoent(core):
    with pytest.raises(OSError) as ei:
        core.lookup(ROOT_INO, "nope")
    assert ei.value.errno == errno.ENOENT


def test_create_write_read_roundtrip(core):
    fh, node = _create(core, ROOT_INO, "f.txt")
    assert core.write(fh, 0, b"hello world") == 11
    assert core.read(fh, 0, 11) == b"hello world"
    core.release(fh)
    # reopen read-only
    fh2 = core.open(node.id, os.O_RDONLY)
    assert core.read(fh2, 0, 11) == b"hello world"
    core.release(fh2)
    assert core.getattr(node.id).size == 11


def test_random_write_and_partial_read(core):
    fh, _ = _create(core, ROOT_INO, "f")
    core.write(fh, 0, b"A" * 100)
    core.write(fh, 50, b"BBB")
    assert core.read(fh, 50, 3) == b"BBB"
    assert core.read(fh, 0, 5) == b"AAAAA"
    core.release(fh)


def test_o_trunc_clears(core):
    nid = _write(core, ROOT_INO, "f", b"0123456789")
    fh = core.open(nid, os.O_WRONLY | os.O_TRUNC)
    core.release(fh)
    assert core.getattr(nid).size == 0


def test_setattr_truncate_grow_shrink(core):
    nid = _write(core, ROOT_INO, "f", b"abcdefgh")
    core.setattr(nid, size=3)
    assert core.getattr(nid).size == 3
    fh = core.open(nid, os.O_RDONLY)
    assert core.read(fh, 0, 10) == b"abc"
    core.release(fh)
    core.setattr(nid, size=5)
    assert core.getattr(nid).size == 5


def test_setattr_chmod_chown(core):
    nid = _write(core, ROOT_INO, "f", b"x")
    core.setattr(nid, mode=stat.S_IFREG | 0o600, uid=1000, gid=2000)
    n = core.getattr(nid)
    assert stat.S_IMODE(n.mode) == 0o600 and n.uid == 1000 and n.gid == 2000


def test_mkdir_rmdir(core):
    d = core.mkdir(ROOT_INO, "sub", 0o755, 0, 0)
    assert core.getattr(d.id).is_dir
    core.rmdir(ROOT_INO, "sub")
    with pytest.raises(OSError) as ei:
        core.lookup(ROOT_INO, "sub")
    assert ei.value.errno == errno.ENOENT


def test_rmdir_nonempty_fails(core):
    d = core.mkdir(ROOT_INO, "sub", 0o755, 0, 0)
    _write(core, d.id, "inner", b"data")
    with pytest.raises(OSError) as ei:
        core.rmdir(ROOT_INO, "sub")
    assert ei.value.errno == errno.ENOTEMPTY


def test_create_existing_dir_eexist(core):
    core.mkdir(ROOT_INO, "d", 0o755, 0, 0)
    with pytest.raises(OSError) as ei:
        core.mkdir(ROOT_INO, "d", 0o755, 0, 0)
    assert ei.value.errno == errno.EEXIST


def test_open_dir_eisdir_and_readdir_file_enotdir(core):
    d = core.mkdir(ROOT_INO, "d", 0o755, 0, 0)
    with pytest.raises(OSError) as ei:
        core.open(d.id, os.O_RDONLY)
    assert ei.value.errno == errno.EISDIR
    nid = _write(core, ROOT_INO, "f", b"x")
    with pytest.raises(OSError) as ei:
        core.readdir(nid)
    assert ei.value.errno == errno.ENOTDIR


def test_unlink_frees_blobs(core, backend):
    nid = _write(core, ROOT_INO, "f", b"Z" * 40)
    deletes0 = backend.deletes
    core.unlink(ROOT_INO, "f")
    assert backend.deletes > deletes0
    with pytest.raises(OSError):
        core.lookup(ROOT_INO, "f")


def test_unlink_while_open_defers_free(core, backend):
    fh, node = _create(core, ROOT_INO, "f")
    core.write(fh, 0, b"Q" * 40)
    core.flush(fh)
    deletes0 = backend.deletes
    core.unlink(ROOT_INO, "f")  # still open -> deferred
    assert backend.deletes == deletes0
    # data still readable through the open handle
    assert core.read(fh, 0, 40) == b"Q" * 40
    core.release(fh)  # last close -> blobs freed now
    assert backend.deletes > deletes0


def test_hardlink_shares_content(core):
    fh, node = _create(core, ROOT_INO, "a")
    core.write(fh, 0, b"shared")
    core.release(fh)
    core.link(node.id, ROOT_INO, "b")
    assert core.getattr(node.id).nlink == 2
    core.unlink(ROOT_INO, "a")
    # content still reachable via "b"
    b = core.lookup(ROOT_INO, "b")
    fh = core.open(b.id, os.O_RDONLY)
    assert core.read(fh, 0, 6) == b"shared"
    core.release(fh)


def test_symlink_readlink(core):
    ln = core.symlink(ROOT_INO, "ln", "/some/target", 0, 0)
    assert stat.S_ISLNK(core.getattr(ln.id).mode)
    assert core.readlink(ln.id) == "/some/target"


def test_rename_file(core):
    nid = _write(core, ROOT_INO, "a", b"data")
    core.rename(ROOT_INO, "a", ROOT_INO, "c")
    with pytest.raises(OSError):
        core.lookup(ROOT_INO, "a")
    assert core.lookup(ROOT_INO, "c").id == nid


def test_rename_over_existing_frees_target(core, backend):
    a = _write(core, ROOT_INO, "a", b"A" * 20)
    b = _write(core, ROOT_INO, "b", b"B" * 20)
    deletes0 = backend.deletes
    core.rename(ROOT_INO, "a", ROOT_INO, "b")  # b's content should be freed
    assert backend.deletes > deletes0
    assert core.lookup(ROOT_INO, "b").id == a


def test_rename_into_subdir(core):
    d = core.mkdir(ROOT_INO, "d", 0o755, 0, 0)
    nid = _write(core, ROOT_INO, "f", b"x")
    core.rename(ROOT_INO, "f", d.id, "f")
    assert core.lookup(d.id, "f").id == nid


def test_statfs(core):
    s = core.statfs()
    assert s["f_namemax"] == 255 and s["f_bsize"] > 0
