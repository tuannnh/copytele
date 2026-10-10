#!/usr/bin/env python3
"""Sort uploads that land in the inbox folder by file type.

copyparty runs this before every upload (``--xbu j,c1,<this file>``) with the
upload info as JSON in argv[1]. Uploads into ``/<inbox>/<device>/...`` are moved
into that device's own sorted folders, so every device keeps its files apart:

    /iphone/Tuan's Iphone/Recents/IMG_0001.HEIC -> /iphone/Tuan's Iphone/photos/IMG_0001.HEIC
    /iphone/Tuan's Iphone/Recents/clip.MOV      -> /iphone/Tuan's Iphone/videos/clip.MOV
    /iphone/Tuan's Iphone/Recents/report.pdf    -> /iphone/Tuan's Iphone/files/report.pdf

A file dropped straight into the inbox (no device folder) goes to
``/<inbox>/photos`` etc. Files already inside a ``photos``/``videos``/``files``
folder are left alone (copyparty calls the hook again for the relocated path),
and uploads to any other top-level folder are untouched.

Environment:
    CP_INBOX   inbox folder name (default "iphone"; set empty to disable sorting)
"""
import json
import os
import sys

PICS = set(
    "avif bmp gif heic heif jpeg jpg jxl png psd qoi raw dng arw cr2 nef "
    "orf rw2 tga tif tiff webp".split()
)
VIDS = set("3gp 3g2 avi flv m4v mkv mov mp4 mpeg mpg mts m2ts ts webm wmv".split())
CATS = ("photos", "videos", "files")


def category(fn: str) -> str:
    ext = fn.rsplit(".", 1)[-1].lower() if "." in fn else ""
    if ext in PICS:
        return "photos"
    if ext in VIDS:
        return "videos"
    return "files"


def target_folder(vp: str, inbox: str) -> str | None:
    """Return the folder (volume URL) to move an upload to, or None to leave it."""
    inbox = inbox.strip("/")
    if not inbox:
        return None
    parts = vp.strip("/").split("/")
    if len(parts) < 2 or parts[0] != inbox:
        return None  # not in the inbox
    folders, fn = parts[1:-1], parts[-1]
    cat = category(fn)
    if not folders:  # dropped straight into the inbox
        return f"/{inbox}/{cat}"
    if folders[0] in CATS:  # already sorted (no device folder)
        return None
    device = folders[0]
    if len(folders) > 1 and folders[1] in CATS:  # already sorted under the device
        return None
    return f"/{inbox}/{device}/{cat}"


def main() -> None:
    inf = json.loads(sys.argv[1])
    dst = target_folder(inf["vp"], os.environ.get("CP_INBOX", "iphone"))
    print(json.dumps({"reloc": {"vp": dst} if dst else {}}))


if __name__ == "__main__":
    main()
