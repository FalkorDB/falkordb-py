import pytest

from falkordb.asyncio import FalkorDB


class DummyServer:
    def __init__(self, db_path=None, config=None, startup_timeout=10.0):
        self.db_path = db_path
        self.config = config
        self.startup_timeout = startup_timeout
        self.unix_socket_path = "/tmp/falkordb-async-test.sock"
        self.stopped = False

    def stop(self):
        self.stopped = True


@pytest.mark.asyncio
async def test_async_embedded_defaults_to_pool_of_16(monkeypatch):
    dummy_server = DummyServer()
    monkeypatch.setattr(
        "falkordb.lite.server.EmbeddedServer", lambda *args, **kwargs: dummy_server
    )
    monkeypatch.setattr("falkordb.asyncio.falkordb.Is_Cluster", lambda _conn: False)

    db = FalkorDB(embedded=True)
    assert db.connection.connection_pool.max_connections == 16
    assert db.connection.connection_pool.timeout == 5.0
    assert db.connection.connection_pool.connection_kwargs["path"] == dummy_server.unix_socket_path

    await db.close()
    assert dummy_server.stopped is True


@pytest.mark.asyncio
async def test_async_embedded_uses_custom_acquire_timeout(monkeypatch):
    dummy_server = DummyServer()
    monkeypatch.setattr(
        "falkordb.lite.server.EmbeddedServer", lambda *args, **kwargs: dummy_server
    )
    monkeypatch.setattr("falkordb.asyncio.falkordb.Is_Cluster", lambda _conn: False)

    db = FalkorDB(embedded=True, connection_acquire_timeout=1.25)
    assert db.connection.connection_pool.timeout == 1.25
    await db.close()


@pytest.mark.asyncio
async def test_async_context_manager_closes_server(monkeypatch):
    dummy_server = DummyServer()
    monkeypatch.setattr(
        "falkordb.lite.server.EmbeddedServer", lambda *args, **kwargs: dummy_server
    )
    monkeypatch.setattr("falkordb.asyncio.falkordb.Is_Cluster", lambda _conn: False)

    async with FalkorDB(embedded=True):
        pass

    assert dummy_server.stopped is True
