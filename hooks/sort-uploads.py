#!/usr/bin/env python3
"""Sort uploads that land in the inbox folder by file type.

copyparty runs this before every upload (``--xbu j,c1,<this file>``) with the
upload info as JSON in argv[1]. Anything uploaded into ``/<inbox>/`` is moved:

    /iphone/IMG_0001.HEIC -> /photos/IMG_0001.HEIC
    /iphone/clip.MOV      -> /videos/clip.MOV
    /iphone/report.pdf    -> /files/report.pdf

Uploads to any other folder are left exactly where they were put.

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


def target_folder(vp: str, inbox: str) -> str | None:
    """Return the folder (volume URL) to move an upload to, or None to leave it."""
    inbox = inbox.strip("/")
    if not inbox:
        return None
    vdir, fn = os.path.split(vp.strip("/"))
    if vdir != inbox and not vdir.startswith(inbox + "/"):
        return None
    ext = fn.rsplit(".", 1)[-1].lower() if "." in fn else ""
    if ext in PICS:
        return "/photos"
    if ext in VIDS:
        return "/videos"
    return "/files"


def main() -> None:
    inf = json.loads(sys.argv[1])
    dst = target_folder(inf["vp"], os.environ.get("CP_INBOX", "iphone"))
    print(json.dumps({"reloc": {"vp": dst} if dst else {}}))


if __name__ == "__main__":
    main()
