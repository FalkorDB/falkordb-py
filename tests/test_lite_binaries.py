import sys

import pytest

from falkordb.lite.binaries import (
    BinaryNotFoundError,
    get_falkordb_module_path,
    get_redis_server_path,
)


class DummyBinModule:
    def __init__(self, redis_path: str, module_path: str):
        self._redis_path = redis_path
        self._module_path = module_path

    def get_redis_server(self):
        return self._redis_path

    def get_falkordb_module(self):
        return self._module_path


def test_get_redis_server_path_success(monkeypatch, tmp_path):
    redis_path = tmp_path / "redis-server"
    redis_path.write_text("bin", encoding="utf-8")
    module = DummyBinModule(str(redis_path), str(tmp_path / "falkordb.so"))
    monkeypatch.setitem(sys.modules, "falkordb_bin", module)

    path = get_redis_server_path()
    assert path == redis_path


def test_get_redis_server_path_missing_file(monkeypatch, tmp_path):
    module = DummyBinModule(
        str(tmp_path / "missing-redis"), str(tmp_path / "falkordb.so")
    )
    monkeypatch.setitem(sys.modules, "falkordb_bin", module)

    with pytest.raises(BinaryNotFoundError, match="redis-server binary was not found"):
        get_redis_server_path()


def test_get_falkordb_module_path_success(monkeypatch, tmp_path):
    module_path = tmp_path / "falkordb.so"
    module_path.write_text("so", encoding="utf-8")
    module = DummyBinModule(str(tmp_path / "redis-server"), str(module_path))
    monkeypatch.setitem(sys.modules, "falkordb_bin", module)

    path = get_falkordb_module_path()
    assert path == module_path


def test_get_falkordb_module_path_missing_file(monkeypatch, tmp_path):
    module = DummyBinModule(
        str(tmp_path / "redis-server"), str(tmp_path / "missing-module.so")
    )
    monkeypatch.setitem(sys.modules, "falkordb_bin", module)

    with pytest.raises(
        BinaryNotFoundError, match="FalkorDB module binary was not found"
    ):
        get_falkordb_module_path()


def test_missing_lite_dependency_raises(monkeypatch):
    monkeypatch.delitem(sys.modules, "falkordb_bin", raising=False)

    with pytest.raises(
        BinaryNotFoundError, match="Install with: pip install falkordb\\[lite\\]"
    ):
        get_redis_server_path()
