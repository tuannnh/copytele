#!/usr/bin/env bash
# Build/install pyfuse3 WITHOUT root, when libfuse3-dev and pkg-config aren't
# installed system-wide. Fetches the dev headers + pkgconf into a local prefix
# (.localdeps) and builds pyfuse3 against them. The compiled extension links at
# runtime against the system libfuse3.so.3 (already present on FUSE3 hosts).
#
# Usage:  scripts/build_pyfuse3_nosudo.sh
# Requires: gcc, python dev headers, apt-get (download-only), dpkg-deb, a venv at .venv
set -euo pipefail
cd "$(dirname "$0")/.."

LD=.localdeps
PFX="$PWD/$LD/root/usr"

mkdir -p "$LD"
cd "$LD"
rm -rf root ./*.deb
echo "[deps] downloading libfuse3-dev + pkgconf ..."
apt-get download libfuse3-dev pkgconf pkgconf-bin libpkgconf3
for d in *.deb; do dpkg-deb -x "$d" root; done

# point the fuse3 pkg-config file at our local extraction
sed -i "s#^prefix=.*#prefix=$PFX#" \
    root/usr/lib/x86_64-linux-gnu/pkgconfig/fuse3.pc
cd ..

export PATH="$PFX/bin:$PATH"
export PKG_CONFIG_PATH="$PFX/lib/x86_64-linux-gnu/pkgconfig"
export LD_LIBRARY_PATH="$PFX/lib/x86_64-linux-gnu:${LD_LIBRARY_PATH:-}"

echo "[check] pkg-config fuse3: $(pkgconf --modversion fuse3)"
echo "[build] installing pyfuse3 + trio into .venv ..."
uv pip install --python .venv pyfuse3 trio 2>/dev/null \
    || .venv/bin/pip install pyfuse3 trio

echo "[done] pyfuse3 installed."
.venv/bin/python -c "import pyfuse3; print('pyfuse3', pyfuse3.__version__, 'ROOT_INODE', pyfuse3.ROOT_INODE)"
