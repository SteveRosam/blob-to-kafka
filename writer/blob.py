"""
Generic blob storage client — a thin, backend-agnostic wrapper over quixportal.

quixportal's ``get_filesystem()`` already abstracts every provider (AWS S3, Azure,
GCP, MinIO, S3-compatible, Local) behind a single fsspec filesystem, configured
from the ``Quix__BlobStorage__Connection__Json`` env var. On Quix Cloud that var
is auto-injected when a deployment has ``blobStorage: bind: true``. Locally, point
it at whatever provider you like (e.g. a MinIO instance) via the same JSON — the
calling code below never changes.

This module keeps the rest of the application from touching a storage SDK directly.

Environment
-----------
- BLOB_BASE_PATH : path prefix all keys are scoped under (default "")
- Quix__BlobStorage__Connection__Json : provider connection config (see quixportal)

Paths passed to every function are relative to the configured base path, so calling
code stays identical regardless of which provider backs it.
"""

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

BLOB_BASE_PATH = os.environ.get("BLOB_BASE_PATH", "")

_fs = None


def get_fs():
    """Return the cached filesystem, creating it on first use. None if unavailable."""
    global _fs
    if _fs is None:
        try:
            from quixportal import get_filesystem
            _fs = get_filesystem(base_path=BLOB_BASE_PATH)
            logger.info("Blob storage ready (base_path=%r)", BLOB_BASE_PATH)
        except Exception as e:
            logger.warning("Blob storage not available: %s", e)
            return None
    return _fs


def reset_fs() -> None:
    """Drop the cached filesystem so the next get_fs() rebuilds it (useful in tests)."""
    global _fs
    _fs = None


def read_bytes(path: str, default: bytes | None = b"") -> bytes | None:
    """Read a file from blob storage and return raw bytes. Returns `default` if missing."""
    fs = get_fs()
    if fs is None:
        return default
    try:
        with fs.open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return default
    except Exception as e:
        logger.warning("Could not read %s: %s", path, e)
        return default


def write_bytes(path: str, data: bytes) -> None:
    """Write raw bytes to blob storage, overwriting any existing object."""
    fs = get_fs()
    if fs is None:
        raise RuntimeError("Blob storage is not configured")
    with fs.open(path, "wb") as f:
        f.write(data)


def list_keys(prefix: str = "") -> list[str]:
    """Return all object paths (not directories) under a prefix."""
    fs = get_fs()
    if fs is None:
        return []
    try:
        entries = fs.ls(prefix, detail=True)
        return [e["name"] for e in entries if e.get("type") != "directory"]
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.warning("Error listing %s: %s", prefix, e)
        return []


def exists(path: str) -> bool:
    fs = get_fs()
    if fs is None:
        return False
    try:
        return fs.exists(path)
    except Exception:
        return False


def file_size(path: str) -> int | None:
    """Return the byte size of a blob, or None if it doesn't exist."""
    fs = get_fs()
    if fs is None:
        return None
    try:
        info = fs.info(path)
        return info.get("size")
    except Exception:
        return None


def open_file(path: str, mode: str = "rb"):
    """Return an fsspec file-like object. Use as a context manager."""
    fs = get_fs()
    if fs is None:
        raise RuntimeError("Blob storage is not configured")
    return fs.open(path, mode)


def delete(path: str, recursive: bool = False) -> None:
    """Delete a single object (or a prefix when recursive=True)."""
    fs = get_fs()
    if fs is None:
        raise RuntimeError("Blob storage is not configured")
    fs.rm(path, recursive=recursive)


def clear_all(prefix: str = "") -> int:
    """Delete every object under a prefix (default: everything). Returns count deleted."""
    fs = get_fs()
    if fs is None:
        raise RuntimeError("Blob storage is not configured")
    try:
        entries = fs.ls(prefix, detail=True)
    except FileNotFoundError:
        return 0
    count = 0
    for entry in entries:
        name = entry["name"]
        is_dir = entry.get("type") == "directory"
        fs.rm(name, recursive=is_dir)
        count += 1
    return count


def upload_file(local_path: Path | str, blob_path: str) -> None:
    """Stream a local file up to blob storage in chunks."""
    fs = get_fs()
    if fs is None:
        raise RuntimeError("Blob storage is not configured")
    local_path = Path(local_path)
    with open(local_path, "rb") as src, fs.open(blob_path, "wb") as dst:
        while True:
            chunk = src.read(_CHUNK_SIZE)
            if not chunk:
                break
            dst.write(chunk)
    logger.info("Uploaded %s to blob (%d bytes)", blob_path, local_path.stat().st_size)


def download_file(blob_path: str, local_path: Path | str) -> bool:
    """Stream a file down from blob storage to a local path. Returns True on success."""
    fs = get_fs()
    if fs is None:
        return False
    local_path = Path(local_path)
    try:
        if not fs.exists(blob_path):
            logger.info("Blob not found: %s", blob_path)
            return False
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with fs.open(blob_path, "rb") as src, open(local_path, "wb") as dst:
            while True:
                chunk = src.read(_CHUNK_SIZE)
                if not chunk:
                    break
                dst.write(chunk)
        logger.info("Downloaded %s from blob (%d bytes)", blob_path, local_path.stat().st_size)
        return True
    except Exception as e:
        logger.warning("Failed to download %s from blob: %s", blob_path, e)
        return False
