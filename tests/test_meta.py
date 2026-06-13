import stat

from tgfs.meta import ROOT_INO, Meta


def test_root_exists(meta):
    root = meta.get_inode(ROOT_INO)
    assert root is not None and root.is_dir
    assert root.nlink == 2


def test_create_and_lookup(meta):
    f = meta.create(ROOT_INO, "hello.txt", stat.S_IFREG | 0o644, "f")
    assert meta.lookup(ROOT_INO, "hello.txt").id == f.id
    assert meta.resolve("/hello.txt").id == f.id
    assert meta.resolve("/nope") is None


def test_mkdir_bumps_parent_nlink(meta):
    before = meta.get_inode(ROOT_INO).nlink
    d = meta.create(ROOT_INO, "sub", stat.S_IFDIR | 0o755, "d")
    assert meta.get_inode(ROOT_INO).nlink == before + 1
    assert d.nlink == 2
    # nested resolve
    f = meta.create(d.id, "x", stat.S_IFREG | 0o644, "f")
    assert meta.resolve("/sub/x").id == f.id


def test_readdir(meta):
    meta.create(ROOT_INO, "b", stat.S_IFREG | 0o644, "f")
    meta.create(ROOT_INO, "a", stat.S_IFREG | 0o644, "f")
    names = [n for n, _ in meta.readdir(ROOT_INO)]
    assert names == ["a", "b"]  # sorted


def test_hardlink_nlink_and_unlink(meta):
    f = meta.create(ROOT_INO, "a", stat.S_IFREG | 0o644, "f")
    meta.link(ROOT_INO, "b", f.id)
    assert meta.get_inode(f.id).nlink == 2

    child, nlink = meta.unlink_dirent(ROOT_INO, "a")
    assert child == f.id and nlink == 1
    assert meta.get_inode(f.id) is not None  # still linked as "b"

    child, nlink = meta.unlink_dirent(ROOT_INO, "b")
    assert nlink == 0
    assert meta.get_inode(f.id) is None  # gone


def test_rmdir_restores_parent_nlink(meta):
    base = meta.get_inode(ROOT_INO).nlink
    meta.create(ROOT_INO, "d", stat.S_IFDIR | 0o755, "d")
    assert meta.get_inode(ROOT_INO).nlink == base + 1
    meta.unlink_dirent(ROOT_INO, "d")
    assert meta.get_inode(ROOT_INO).nlink == base


def test_rename_simple(meta):
    f = meta.create(ROOT_INO, "a", stat.S_IFREG | 0o644, "f")
    replaced = meta.rename(ROOT_INO, "a", ROOT_INO, "c")
    assert replaced is None
    assert meta.lookup(ROOT_INO, "a") is None
    assert meta.lookup(ROOT_INO, "c").id == f.id


def test_rename_over_existing_returns_replaced(meta):
    a = meta.create(ROOT_INO, "a", stat.S_IFREG | 0o644, "f")
    b = meta.create(ROOT_INO, "b", stat.S_IFREG | 0o644, "f")
    replaced = meta.rename(ROOT_INO, "a", ROOT_INO, "b")
    assert replaced == b.id
    assert meta.lookup(ROOT_INO, "b").id == a.id
    assert meta.get_inode(b.id) is None


def test_rename_across_dirs_updates_dir_nlink(meta):
    d1 = meta.create(ROOT_INO, "d1", stat.S_IFDIR | 0o755, "d")
    d2 = meta.create(ROOT_INO, "d2", stat.S_IFDIR | 0o755, "d")
    sub = meta.create(d1.id, "s", stat.S_IFDIR | 0o755, "d")
    n1, n2 = meta.get_inode(d1.id).nlink, meta.get_inode(d2.id).nlink
    meta.rename(d1.id, "s", d2.id, "s")
    assert meta.get_inode(d1.id).nlink == n1 - 1
    assert meta.get_inode(d2.id).nlink == n2 + 1
    assert meta.resolve("/d2/s").id == sub.id


def test_setattr(meta):
    f = meta.create(ROOT_INO, "a", stat.S_IFREG | 0o644, "f")
    meta.setattr(f.id, mode=stat.S_IFREG | 0o600, uid=1000, gid=1000)
    g = meta.get_inode(f.id)
    assert stat.S_IMODE(g.mode) == 0o600 and g.uid == 1000 and g.gid == 1000


def test_symlink(meta):
    link = meta.create(ROOT_INO, "ln", stat.S_IFLNK | 0o777, "l", target="/target")
    assert link.is_link and link.target == "/target"


def test_blob_refcount_lifecycle(meta):
    meta.blob_create("sha1", handle=10, length=5)
    assert meta.blob_get("sha1")["refcount"] == 1
    meta.blob_incref("sha1")
    assert meta.blob_get("sha1")["refcount"] == 2
    assert meta.blob_decref("sha1") is None  # 2 -> 1
    assert meta.blob_decref("sha1") == 10  # 1 -> 0, returns handle
    assert meta.blob_get("sha1") is None


def test_persistence(tmp_path):
    p = tmp_path / "m.db"
    m = Meta(p)
    f = m.create(ROOT_INO, "keep", stat.S_IFREG | 0o644, "f")
    m.close()
    m2 = Meta(p)
    assert m2.resolve("/keep").id == f.id
    m2.close()
