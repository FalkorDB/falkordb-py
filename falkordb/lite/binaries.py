"""Resolve embedded binaries provided by the optional falkordb-bin package."""

from pathlib import Path


class BinaryNotFoundError(ImportError):
    """Raised when embedded binaries are missing."""


def _require_bin_package():
    try:
        import falkordb_bin
    except ImportError as exc:
        raise BinaryNotFoundError(
            "Embedded FalkorDB requires the optional 'lite' extra. "
            "Install with: pip install falkordb[lite]"
        ) from exc
    return falkordb_bin


def get_redis_server_path() -> Path:
    """Return path to the embedded redis-server binary."""
    falkordb_bin = _require_bin_package()
    path = Path(falkordb_bin.get_redis_server())
    if not path.exists():
        raise BinaryNotFoundError(f"redis-server binary was not found at: {path}")
    return path


def get_falkordb_module_path() -> Path:
    """Return path to the embedded FalkorDB module binary."""
    falkordb_bin = _require_bin_package()
    path = Path(falkordb_bin.get_falkordb_module())
    if not path.exists():
        raise BinaryNotFoundError(f"FalkorDB module binary was not found at: {path}")
    return path

