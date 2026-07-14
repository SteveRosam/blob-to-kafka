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
- BLOB_DEBUG : set to "1"/"true" to log the resolved location of every read/write

Paths passed to every function are relative to BLOB_BASE_PATH. The filesystem is
created with a plain get_filesystem() (no base_path argument) to preserve
quixportal's implicit bucket scope; BLOB_BASE_PATH is applied client-side in
_key(). This mirrors the pattern proven to work in the Blob Storage Explorer —
passing base_path to get_filesystem() can re-root the DirFileSystem and cause
403 Forbidden errors.
"""

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB

BLOB_BASE_PATH = os.environ.get("BLOB_BASE_PATH", "")
BLOB_DEBUG = os.environ.get("BLOB_DEBUG", "").lower() in ("1", "true", "yes")

_fs = None
_bucket = None  # parsed from the connection JSON, for logging the full location


_BASE = BLOB_BASE_PATH.replace("\\", "/").strip("/")


def _norm(path: str) -> str:
    """Normalise a blob key: backslashes -> '/', strip leading slashes.

    Blob keys are always forward-slash separated. A Windows-style path such as
    ``folder\\file.csv`` becomes a literal key with a backslash and will never
    match ``folder/file.csv`` — so fix it and warn loudly.
    """
    fixed = path.replace("\\", "/").lstrip("/")
    if fixed != path:
        logger.warning("Normalised blob key %r -> %r (keys use '/', not '\\')", path, fixed)
    return fixed


def _key(path: str) -> str:
    """Resolve a caller path to a bucket-relative key, applying BLOB_BASE_PATH.

    We prefix BLOB_BASE_PATH here rather than passing base_path to
    get_filesystem() — passing base_path (especially an empty string) can re-root
    the DirFileSystem and lose quixportal's implicit bucket scope, which surfaces
    as 403 Forbidden. Keeping the filesystem plain and prefixing ourselves matches
    the pattern proven to work in the Blob Storage Explorer.
    """
    p = _norm(path)
    if _BASE:
        return f"{_BASE}/{p}" if p else _BASE
    return p


def _describe_config() -> None:
    """Log where we're pointed: provider, bucket, endpoint, base_path (no secrets)."""
    global _bucket
    raw = os.environ.get("Quix__BlobStorage__Connection__Json")
    if not raw:
        logger.warning(
            "Quix__BlobStorage__Connection__Json is NOT set — is 'blobStorage: bind: true' "
            "on this deployment? (base_path=%r)", BLOB_BASE_PATH,
        )
        return
    try:
        cfg = json.loads(raw)
        # quixportal accepts both camelCase and PascalCase keys.
        provider = cfg.get("provider") or cfg.get("Provider")
        s3 = cfg.get("s3Compatible") or cfg.get("S3Compatible") or {}
        _bucket = s3.get("bucketName") or s3.get("BucketName")
        service_url = s3.get("serviceUrl") or s3.get("ServiceUrl")
        logger.info(
            "Blob config: provider=%s bucket=%s serviceUrl=%s base_path=%r",
            provider, _bucket, service_url, BLOB_BASE_PATH,
        )
    except Exception as e:
        logger.warning("Could not parse blob connection JSON: %s", e)


def _full_location(key: str) -> str:
    """Human-readable full location for logging, e.g. s3://bucket/key (key already includes base)."""
    parts = [p for p in (_bucket, key) if p]
    return "s3://" + "/".join(parts)


def get_fs():
    """Return the cached filesystem, creating it on first use. None if unavailable."""
    global _fs
    if _fs is None:
        _describe_config()
        try:
            # Plain get_filesystem() (no base_path) so quixportal's implicit bucket
            # scope is preserved — BLOB_BASE_PATH is applied client-side in _key().
            from quixportal.storage import get_filesystem
            _fs = get_filesystem()
            logger.info(
                "Blob storage ready: fs=%s base_path=%r", type(_fs).__name__, BLOB_BASE_PATH,
            )
            # Show what's actually visible at the base path. If this itself is
            # Forbidden, the scope/base path is wrong; if it lists files, any later
            # "Forbidden" on a read is really a missing/misnamed key.
            listing_root = _key("")
            try:
                entries = _fs.ls(listing_root or "", detail=False)
                logger.info(
                    "Contents at %r (%d entries): %s",
                    listing_root, len(entries), entries[:25],
                )
            except Exception as e:
                logger.warning("Could not list %r (%s): %s", listing_root, type(e).__name__, e)
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
    key = _key(path)
    if BLOB_DEBUG:
        logger.info("Reading %s", _full_location(key))
    try:
        with fs.open(key, "rb") as f:
            return f.read()
    except FileNotFoundError:
        logger.warning("Not found: %s", _full_location(key))
        return default
    except Exception as e:
        # S3-scoped credentials return 403 Forbidden (not 404) for a missing key
        # when ListBucket is denied — so "Forbidden" here usually means the key is
        # absent or misnamed at this location, not a genuine permissions problem.
        logger.warning("Could not read %s: %s (%s)", _full_location(key), e, type(e).__name__)
        return default


def write_bytes(path: str, data: bytes) -> None:
    """Write raw bytes to blob storage, overwriting any existing object."""
    fs = get_fs()
    if fs is None:
        raise RuntimeError("Blob storage is not configured")
    key = _key(path)
    if BLOB_DEBUG:
        logger.info("Writing %d bytes to %s", len(data), _full_location(key))
    fs.pipe(key, data)


def list_keys(prefix: str = "") -> list[str]:
    """Return all object paths (not directories) under a prefix."""
    fs = get_fs()
    if fs is None:
        return []
    key = _key(prefix)
    try:
        entries = fs.ls(key or "", detail=True)
        return [e["name"] for e in entries if e.get("type") != "directory"]
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.warning("Error listing %s: %s", _full_location(key), e)
        return []


def exists(path: str) -> bool:
    fs = get_fs()
    if fs is None:
        return False
    try:
        return fs.exists(_key(path))
    except Exception:
        return False


def file_size(path: str) -> int | None:
    """Return the byte size of a blob, or None if it doesn't exist."""
    fs = get_fs()
    if fs is None:
        return None
    try:
        info = fs.info(_key(path))
        return info.get("size")
    except Exception:
        return None


def open_file(path: str, mode: str = "rb"):
    """Return an fsspec file-like object. Use as a context manager."""
    fs = get_fs()
    if fs is None:
        raise RuntimeError("Blob storage is not configured")
    return fs.open(_key(path), mode)


def delete(path: str, recursive: bool = False) -> None:
    """Delete a single object (or a prefix when recursive=True)."""
    fs = get_fs()
    if fs is None:
        raise RuntimeError("Blob storage is not configured")
    fs.rm(_key(path), recursive=recursive)


def clear_all(prefix: str = "") -> int:
    """Delete every object under a prefix (default: everything). Returns count deleted."""
    fs = get_fs()
    if fs is None:
        raise RuntimeError("Blob storage is not configured")
    try:
        entries = fs.ls(_key(prefix) or "", detail=True)
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
    key = _key(blob_path)
    with open(local_path, "rb") as src, fs.open(key, "wb") as dst:
        while True:
            chunk = src.read(_CHUNK_SIZE)
            if not chunk:
                break
            dst.write(chunk)
    logger.info("Uploaded %s to %s (%d bytes)", local_path, _full_location(key), local_path.stat().st_size)


def download_file(blob_path: str, local_path: Path | str) -> bool:
    """Stream a file down from blob storage to a local path. Returns True on success."""
    fs = get_fs()
    if fs is None:
        return False
    local_path = Path(local_path)
    key = _key(blob_path)
    try:
        if not fs.exists(key):
            logger.info("Blob not found: %s", _full_location(key))
            return False
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with fs.open(key, "rb") as src, open(local_path, "wb") as dst:
            while True:
                chunk = src.read(_CHUNK_SIZE)
                if not chunk:
                    break
                dst.write(chunk)
        logger.info("Downloaded %s from %s (%d bytes)", local_path, _full_location(key), local_path.stat().st_size)
        return True
    except Exception as e:
        logger.warning("Failed to download %s: %s", _full_location(key), e)
        return False
