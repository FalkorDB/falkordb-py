"""Package version utilities."""

from importlib.metadata import PackageNotFoundError, version as get_version


def get_package_version() -> str:
    """Get the FalkorDB package version.

    Returns:
        The package version string, or "unknown" if the package is not installed.
    """
    try:
        return get_version("FalkorDB")
    except PackageNotFoundError:
        return "unknown"
