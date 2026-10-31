#!/usr/bin/env sh
# Prepare writable volumes and drop from root to the unprivileged gfs user.
set -eu

if [ "$(id -u)" = "0" ]; then
  if [ -d /data ]; then
    chown -R gfs:gfs /data 2>/dev/null || true
  fi
  if [ -d /certs ]; then
    chown -R gfs:gfs /certs 2>/dev/null || true
  fi
  exec su-exec gfs "$@"
fi

exec "$@"
