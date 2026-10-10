import importlib.util
import json
import pathlib
import subprocess
import sys

import pytest

HOOK = pathlib.Path(__file__).resolve().parent.parent / "hooks" / "sort-uploads.py"
spec = importlib.util.spec_from_file_location("sort_uploads", HOOK)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


@pytest.mark.parametrize(
    "vp,expect",
    [
        # device folder: sorted inside it, whatever sub-folder the app used
        ("iphone/Tuan\u2019s Iphone/Recents/IMG_0001.HEIC", "/iphone/Tuan\u2019s Iphone/photos"),
        ("iphone/Tuan\u2019s Iphone/Recents/clip.MOV", "/iphone/Tuan\u2019s Iphone/videos"),
        ("iphone/Tuan\u2019s Iphone/Recents/report.pdf", "/iphone/Tuan\u2019s Iphone/files"),
        ("iphone/Tuan\u2019s Iphone/Recents/noext", "/iphone/Tuan\u2019s Iphone/files"),
        ("iphone/Tuan\u2019s Iphone/IMG_0002.jpg", "/iphone/Tuan\u2019s Iphone/photos"),
        ("iphone/Dad/Albums/Trip/a.mp4", "/iphone/Dad/videos"),
        ("/iphone/Tuan\u2019s Iphone/x.png", "/iphone/Tuan\u2019s Iphone/photos"),  # leading slash
        # straight into the inbox
        ("iphone/IMG_0003.JPG", "/iphone/photos"),
        ("iphone/a.mov", "/iphone/videos"),
        ("iphone/doc.txt", "/iphone/files"),
        # already sorted -> leave (copyparty re-runs the hook for the relocated path)
        ("iphone/Tuan\u2019s Iphone/photos/IMG_0001.HEIC", None),
        ("iphone/Tuan\u2019s Iphone/videos/clip.MOV", None),
        ("iphone/Tuan\u2019s Iphone/files/report.pdf", None),
        ("iphone/Tuan\u2019s Iphone/photos/2024/x.jpg", None),
        ("iphone/photos/IMG_0003.JPG", None),
        # outside the inbox
        ("videos/dji/y.mp4", None),
        ("iphone2/x.jpg", None),
        ("x.jpg", None),
    ],
)
def test_target_folder(vp, expect):
    assert mod.target_folder(vp, "iphone") == expect


def test_idempotent():
    """The destination of every sorted file maps to 'leave it' on the second call."""
    for vp in ("iphone/Dev/Recents/a.heic", "iphone/Dev/b.mov", "iphone/c.pdf", "iphone/d.jpg"):
        dst = mod.target_folder(vp, "iphone")
        assert dst is not None
        assert mod.target_folder(dst.strip("/") + "/" + vp.rsplit("/", 1)[-1], "iphone") is None


def test_disabled_when_inbox_empty():
    assert mod.target_folder("iphone/x.jpg", "") is None


def test_custom_inbox():
    assert mod.target_folder("inbox/Dev/x.mov", "inbox") == "/inbox/Dev/videos"
    assert mod.target_folder("iphone/x.mov", "inbox") is None


def run(vp, **env):
    out = subprocess.run(
        [sys.executable, str(HOOK), json.dumps({"vp": vp})],
        capture_output=True, text=True, check=True, env={"PATH": "/usr/bin:/bin", **env},
    ).stdout
    return json.loads(out)


def test_cli_output_shape():
    assert run("iphone/Dev/Recents/a.heic") == {"reloc": {"vp": "/iphone/Dev/photos"}}
    assert run("other/a.heic") == {"reloc": {}}
    assert run("iphone/Dev/a.heic", CP_INBOX="") == {"reloc": {}}
