from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from redis.exceptions import RedisError

import falkordb.asyncio.cluster as async_cluster_module
import falkordb.falkordb as sync_falkordb_module
from falkordb.asyncio.falkordb import FalkorDB as AsyncFalkorDB
from falkordb.asyncio.graph import AsyncGraph
from falkordb.exceptions import SchemaVersionMismatchException
from falkordb.falkordb import FalkorDB as SyncFalkorDB


def test_sync_cluster_conn_does_not_mutate_connection_kwargs(monkeypatch):
    original_kwargs = {
        "host": "localhost",
        "port": 6379,
        "username": "user",
        "password": "pass",
        "socket_timeout": 1,
    }
    conn = SimpleNamespace(
        connection_pool=SimpleNamespace(connection_kwargs=original_kwargs.copy())
    )

    import falkordb.cluster as sync_cluster_module

    monkeypatch.setattr(sync_cluster_module, "RedisCluster", lambda **kwargs: kwargs)
    result = sync_cluster_module.Cluster_Conn(conn, ssl=False)

    assert conn.connection_pool.connection_kwargs == original_kwargs
    assert result["host"] == "localhost"
    assert result["port"] == 6379


def test_async_cluster_conn_does_not_mutate_connection_kwargs(monkeypatch):
    original_kwargs = {
        "host": "localhost",
        "port": 6379,
        "username": "user",
        "password": "pass",
        "socket_timeout": 1,
        "unknown_option": "ignore_me",
    }
    conn = SimpleNamespace(
        connection_pool=SimpleNamespace(connection_kwargs=original_kwargs.copy())
    )

    monkeypatch.setattr(async_cluster_module, "RedisCluster", lambda **kwargs: kwargs)
    result = async_cluster_module.Cluster_Conn(conn, ssl=False)

    assert conn.connection_pool.connection_kwargs == original_kwargs
    assert result["host"] == "localhost"
    assert result["port"] == 6379
    assert result["socket_timeout"] == 1
    assert "unknown_option" not in result


def test_sync_udf_cluster_fanout_targets_primaries():
    db = object.__new__(SyncFalkorDB)
    execute_command = Mock(return_value={"primary-1": "OK", "primary-2": "OK"})
    db.connection = SimpleNamespace(
        execute_command=execute_command,
        PRIMARIES="primaries",
    )
    db._topology_mode = "cluster"

    response = db.udf_flush()

    assert response == "OK"
    execute_command.assert_called_once_with(
        "GRAPH.UDF",
        "FLUSH",
        target_nodes="primaries",
    )


def test_sync_udf_cluster_fanout_raises_on_inconsistent_responses():
    db = object.__new__(SyncFalkorDB)
    execute_command = Mock(return_value={"primary-1": "OK", "primary-2": "ERR"})
    db.connection = SimpleNamespace(
        execute_command=execute_command,
        PRIMARIES="primaries",
    )
    db._topology_mode = "cluster"

    with pytest.raises(RedisError):
        db.udf_flush()


@pytest.mark.asyncio
async def test_async_udf_cluster_fanout_targets_primaries():
    db = object.__new__(AsyncFalkorDB)
    execute_command = AsyncMock(return_value={"primary-1": "OK", "primary-2": "OK"})
    db.connection = SimpleNamespace(
        execute_command=execute_command,
        PRIMARIES="primaries",
    )
    db._topology_mode = "cluster"

    response = await db.udf_delete("lib")

    assert response == "OK"
    execute_command.assert_awaited_once_with(
        "GRAPH.UDF",
        "DELETE",
        "lib",
        target_nodes="primaries",
    )


@pytest.mark.asyncio
async def test_async_query_awaits_schema_refresh_on_version_mismatch():
    client = SimpleNamespace(
        execute_command=AsyncMock(side_effect=SchemaVersionMismatchException(7))
    )
    graph = AsyncGraph(client, "async_graph")
    graph.schema = SimpleNamespace(refresh=AsyncMock())

    with pytest.raises(SchemaVersionMismatchException):
        await graph.query("RETURN 1")

    graph.schema.refresh.assert_awaited_once_with(7)


def test_topology_mode_standalone_skips_auto_probe(monkeypatch):
    fake_conn = SimpleNamespace(
        connection_pool=SimpleNamespace(connection_kwargs={}),
        flushdb=Mock(),
        execute_command=Mock(),
        close=Mock(),
    )
    monkeypatch.setattr(sync_falkordb_module.redis, "Redis", lambda **kwargs: fake_conn)
    monkeypatch.setattr(
        sync_falkordb_module,
        "Is_Sentinel",
        lambda conn: (_ for _ in ()).throw(AssertionError("should not probe sentinel")),
    )
    monkeypatch.setattr(
        sync_falkordb_module,
        "Is_Cluster",
        lambda conn: (_ for _ in ()).throw(AssertionError("should not probe cluster")),
    )

    db = SyncFalkorDB(topology_mode="standalone")

    assert db._topology_mode == "standalone"
