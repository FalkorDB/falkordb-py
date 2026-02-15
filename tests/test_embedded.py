import sys

import pytest

from falkordb import FalkorDB
from falkordb.lite.binaries import BinaryNotFoundError, get_redis_server_path
from falkordb.lite.config import generate_config


class DummyServer:
    def __init__(self, db_path=None, config=None, startup_timeout=10.0):
        self.db_path = db_path
        self.config = config
        self.startup_timeout = startup_timeout
        self.unix_socket_path = "/tmp/falkordb-test.sock"
        self.stopped = False

    def stop(self):
        self.stopped = True


def test_embedded_defaults_to_pool_of_16(monkeypatch):
    dummy_server = DummyServer()

    def fake_server(*args, **kwargs):
        return dummy_server

    monkeypatch.setattr("falkordb.lite.server.EmbeddedServer", fake_server)
    monkeypatch.setattr("falkordb.falkordb.Is_Sentinel", lambda _conn: False)
    monkeypatch.setattr("falkordb.falkordb.Is_Cluster", lambda _conn: False)

    db = FalkorDB(embedded=True)
    assert db.connection.connection_pool.max_connections == 16
    assert db.connection.connection_pool.connection_kwargs["path"] == dummy_server.unix_socket_path

    db.close()
    assert dummy_server.stopped is True


def test_embedded_passes_config_and_db_path(monkeypatch):
    observed = {}

    def fake_server(*args, **kwargs):
        observed["kwargs"] = kwargs
        return DummyServer(*args, **kwargs)

    monkeypatch.setattr("falkordb.lite.server.EmbeddedServer", fake_server)
    monkeypatch.setattr("falkordb.falkordb.Is_Sentinel", lambda _conn: False)
    monkeypatch.setattr("falkordb.falkordb.Is_Cluster", lambda _conn: False)

    db = FalkorDB(
        embedded=True,
        db_path="/tmp/demo.db",
        embedded_config={"maxmemory": "1gb"},
        startup_timeout=12.5,
        max_connections=32,
    )

    assert observed["kwargs"]["db_path"] == "/tmp/demo.db"
    assert observed["kwargs"]["config"] == {"maxmemory": "1gb"}
    assert observed["kwargs"]["startup_timeout"] == 12.5
    assert db.connection.connection_pool.max_connections == 32
    db.close()


def test_context_manager_closes_embedded_server(monkeypatch):
    dummy_server = DummyServer()
    monkeypatch.setattr("falkordb.lite.server.EmbeddedServer", lambda *args, **kwargs: dummy_server)
    monkeypatch.setattr("falkordb.falkordb.Is_Sentinel", lambda _conn: False)
    monkeypatch.setattr("falkordb.falkordb.Is_Cluster", lambda _conn: False)

    with FalkorDB(embedded=True):
        pass

    assert dummy_server.stopped is True


def test_generate_config_with_persistence(tmp_path):
    db_file = tmp_path / "embedded.rdb"
    conf = generate_config(
        falkordb_module_path=tmp_path / "falkordb.so",
        db_path=str(db_file),
        unix_socket_path="/tmp/test.sock",
        user_config={"maxmemory": "1gb"},
    )

    assert f"dir {tmp_path}" in conf
    assert "dbfilename embedded.rdb" in conf
    assert "save 900 1" in conf
    assert "save 300 10" in conf
    assert "save 60 10000" in conf
    assert "appendonly yes" in conf
    assert "unixsocket /tmp/test.sock" in conf
    assert "maxmemory 1gb" in conf


def test_generate_config_rejects_loadmodule_override(tmp_path):
    with pytest.raises(ValueError):
        generate_config(
            falkordb_module_path=tmp_path / "falkordb.so",
            user_config={"loadmodule": "/tmp/other.so"},
        )


def test_missing_lite_dependency_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "falkordb_bin", None)

    with pytest.raises(BinaryNotFoundError):
        get_redis_server_path()
