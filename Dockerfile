# syntax=docker/dockerfile:1
# ---- build stage: compile pyfuse3 (+ cryptg) against fuse3 dev headers --------
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        pkg-config \
        libfuse3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH

WORKDIR /app
COPY pyproject.toml README.md ./
COPY tgfs ./tgfs
# install tgfs + the full feature set (copyparty, Pillow, mutagen, argon2, cryptg)
RUN pip install --no-cache-dir -U pip \
    && pip install --no-cache-dir ".[full]"

# ---- runtime stage ------------------------------------------------------------
FROM python:3.12-slim

# fuse3 -> fusermount3 + libfuse3.so.3 ; ffmpeg -> video/audio thumbnails ;
# tini -> proper PID 1 signal handling and zombie reaping
RUN apt-get update && apt-get install -y --no-install-recommends \
        fuse3 \
        ffmpeg \
        tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    TGFS_SESSION=/data/tgfs.session \
    TGFS_META_DB=/data/meta.db \
    TGFS_CACHE_DIR=/data/cache \
    TGFS_MOUNT=/mnt/tgfs \
    TGFS_HIST=/data/hist \
    TGFS_PORT=3923

WORKDIR /app
VOLUME /data
EXPOSE 3923

# everything under /data persists across restarts; meta.db is the source of truth
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
# default copyparty feature flags (override by setting `command:` in compose)
CMD ["-e2dsa", "-e2ts", "--dedup"]
