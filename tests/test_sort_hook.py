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
        ("iphone/IMG_0001.HEIC", "/photos"),
        ("iphone/IMG_0002.jpg", "/photos"),
        ("iphone/clip.MOV", "/videos"),
        ("iphone/a.mp4", "/videos"),
        ("iphone/report.pdf", "/files"),
        ("iphone/noext", "/files"),
        ("iphone/sub/x.jpg", "/photos"),  # subfolders of the inbox are sorted too
        ("/iphone/x.png", "/photos"),  # leading slash tolerated
        ("videos/dji/y.mp4", None),  # other folders are untouched
        ("iphone2/x.jpg", None),  # prefix match must be a whole folder name
        ("x.jpg", None),
    ],
)
def test_target_folder(vp, expect):
    assert mod.target_folder(vp, "iphone") == expect


def test_disabled_when_inbox_empty():
    assert mod.target_folder("iphone/x.jpg", "") is None


def test_custom_inbox():
    assert mod.target_folder("inbox/x.mov", "inbox") == "/videos"
    assert mod.target_folder("iphone/x.mov", "inbox") is None


def run(vp, **env):
    out = subprocess.run(
        [sys.executable, str(HOOK), json.dumps({"vp": vp})],
        capture_output=True, text=True, check=True, env={"PATH": "/usr/bin:/bin", **env},
    ).stdout
    return json.loads(out)


def test_cli_output_shape():
    assert run("iphone/a.heic") == {"reloc": {"vp": "/photos"}}
    assert run("other/a.heic") == {"reloc": {}}
    assert run("iphone/a.heic", CP_INBOX="") == {"reloc": {}}
