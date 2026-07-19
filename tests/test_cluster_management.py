import sys
import types
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from redis.exceptions import RedisError

if "redis.driver_info" not in sys.modules:
    redis_driver_info = types.ModuleType("redis.driver_info")

    class _DriverInfo:
        def __init__(self, lib_name, lib_version):
            self.lib_name = lib_name
            self.lib_version = lib_version

    redis_driver_info.DriverInfo = _DriverInfo
    sys.modules["redis.driver_info"] = redis_driver_info

import falkordb.asyncio.cluster as async_cluster_module
import falkordb.asyncio.falkordb as async_falkordb_module
import falkordb.asyncio.sentinel as async_sentinel_module
import falkordb.cluster as sync_cluster_module
import falkordb.falkordb as sync_falkordb_module
import falkordb.sentinel as sync_sentinel_module
from falkordb.asyncio.falkordb import FalkorDB as AsyncFalkorDB
from falkordb.asyncio.graph import AsyncGraph
from falkordb.exceptions import SchemaVersionMismatchException
from falkordb.falkordb import FalkorDB as SyncFalkorDB


def _sync_conn(connection_kwargs=None):
    return SimpleNamespace(
        connection_pool=SimpleNamespace(connection_kwargs=connection_kwargs or {}),
        flushdb=Mock(),
        execute_command=Mock(),
        close=Mock(),
    )


def _async_conn(connection_kwargs=None):
    return SimpleNamespace(
        connection_pool=SimpleNamespace(connection_kwargs=connection_kwargs or {}),
        flushdb=AsyncMock(),
        execute_command=AsyncMock(),
        aclose=AsyncMock(),
    )


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
    result = async_cluster_module.Cluster_Conn(
        conn, ssl=False, dynamic_startup_nodes=False, url="redis://cluster"
    )

    assert conn.connection_pool.connection_kwargs == original_kwargs
    assert result["host"] == "localhost"
    assert result["port"] == 6379
    assert result["socket_timeout"] == 1
    assert result["dynamic_startup_nodes"] is False
    assert "unknown_option" not in result


def test_async_cluster_probe_kwargs_filters_supported_fields():
    kwargs = {
        "host": "localhost",
        "port": 6379,
        "password": "pw",
        "ignored": "value",
    }
    filtered = async_cluster_module._probe_kwargs(kwargs)
    assert filtered["host"] == "localhost"
    assert filtered["port"] == 6379
    assert filtered["password"] == "pw"
    assert filtered["decode_responses"] is True
    assert "ignored" not in filtered


def test_async_sentinel_probe_kwargs_filters_supported_fields():
    kwargs = {
        "host": "localhost",
        "port": 6379,
        "password": "pw",
        "ignored": "value",
    }
    filtered = async_sentinel_module._probe_kwargs(kwargs)
    assert filtered["host"] == "localhost"
    assert filtered["port"] == 6379
    assert filtered["password"] == "pw"
    assert filtered["decode_responses"] is True
    assert "ignored" not in filtered


def test_async_ssl_connection_helper_handles_non_type():
    pool = SimpleNamespace(connection_class=object())
    assert async_cluster_module._is_ssl_connection(pool) is False
    assert async_sentinel_module._is_ssl_connection(pool) is False


def test_async_is_cluster_uses_sync_probe_and_closes(monkeypatch):
    probe = SimpleNamespace(
        info=Mock(return_value={"redis_mode": "cluster"}),
        close=Mock(),
    )
    probe_ctor = Mock(return_value=probe)
    monkeypatch.setattr(async_cluster_module.sync_redis, "Redis", probe_ctor)

    conn = SimpleNamespace(
        connection_pool=SimpleNamespace(
            connection_kwargs={"host": "127.0.0.1", "port": 6379, "ignored": "x"},
            connection_class=async_cluster_module.redis.SSLConnection,
        )
    )

    assert async_cluster_module.Is_Cluster(conn) is True
    probe.info.assert_called_once_with(section="server")
    probe.close.assert_called_once()
    probe_kwargs = probe_ctor.call_args.kwargs
    assert probe_kwargs["host"] == "127.0.0.1"
    assert probe_kwargs["port"] == 6379
    assert probe_kwargs["ssl"] is True
    assert probe_kwargs["decode_responses"] is True
    assert "ignored" not in probe_kwargs


def test_async_is_sentinel_uses_sync_probe_and_closes(monkeypatch):
    probe = SimpleNamespace(
        info=Mock(return_value={"redis_mode": "sentinel"}),
        close=Mock(),
    )
    probe_ctor = Mock(return_value=probe)
    monkeypatch.setattr(async_sentinel_module.sync_redis, "Redis", probe_ctor)

    conn = SimpleNamespace(
        connection_pool=SimpleNamespace(
            connection_kwargs={"host": "127.0.0.1", "port": 26379, "ignored": "x"},
            connection_class=async_sentinel_module.redis.SSLConnection,
        )
    )

    assert async_sentinel_module.Is_Sentinel(conn) is True
    probe.info.assert_called_once_with(section="server")
    probe.close.assert_called_once()
    probe_kwargs = probe_ctor.call_args.kwargs
    assert probe_kwargs["host"] == "127.0.0.1"
    assert probe_kwargs["port"] == 26379
    assert probe_kwargs["ssl"] is True
    assert probe_kwargs["decode_responses"] is True
    assert "ignored" not in probe_kwargs


def test_sync_sentinel_conn_uses_supplied_service_and_nodes(monkeypatch):
    conn_kwargs = {
        "host": "127.0.0.1",
        "port": 26379,
        "username": "user",
        "password": "pass",
        "socket_timeout": 1,
    }
    conn = SimpleNamespace(
        connection_pool=SimpleNamespace(connection_kwargs=conn_kwargs.copy())
    )
    sentinel_ctor = Mock(return_value="sentinel-conn")
    monkeypatch.setattr(sync_sentinel_module, "Sentinel", sentinel_ctor)

    sentinel, service_name = sync_sentinel_module.Sentinel_Conn(
        conn,
        ssl=True,
        service_name="mymaster",
        sentinel_nodes=[("s1", 26379)],
    )

    assert sentinel == "sentinel-conn"
    assert service_name == "mymaster"
    sentinel_ctor.assert_called_once()
    assert sentinel_ctor.call_args.args[0] == [("s1", 26379)]
    assert sentinel_ctor.call_args.kwargs["sentinel_kwargs"] == {
        "username": "user",
        "password": "pass",
        "ssl": True,
    }
    assert sentinel_ctor.call_args.kwargs["socket_timeout"] == 1
    assert conn.connection_pool.connection_kwargs == conn_kwargs


def test_sync_sentinel_conn_discovers_single_master(monkeypatch):
    conn = SimpleNamespace(
        sentinel_masters=Mock(return_value={"mymaster": {}}),
        connection_pool=SimpleNamespace(
            connection_kwargs={"host": "127.0.0.1", "port": 26379}
        ),
    )
    sentinel_ctor = Mock(return_value="sentinel-conn")
    monkeypatch.setattr(sync_sentinel_module, "Sentinel", sentinel_ctor)

    _, service_name = sync_sentinel_module.Sentinel_Conn(conn, ssl=False)
    assert service_name == "mymaster"
    conn.sentinel_masters.assert_called_once()
    assert sentinel_ctor.call_args.args[0] == [("127.0.0.1", 26379)]


def test_sync_sentinel_conn_raises_on_multiple_masters():
    conn = SimpleNamespace(
        sentinel_masters=Mock(return_value={"m1": {}, "m2": {}}),
        connection_pool=SimpleNamespace(
            connection_kwargs={"host": "127.0.0.1", "port": 26379}
        ),
    )
    with pytest.raises(Exception, match="Multiple masters"):
        sync_sentinel_module.Sentinel_Conn(conn, ssl=False)


def test_async_sentinel_conn_uses_supplied_service_without_probe(monkeypatch):
    conn_kwargs = {
        "host": "127.0.0.1",
        "port": 26379,
        "username": "user",
        "password": "pass",
        "socket_timeout": 1,
    }
    conn = SimpleNamespace(
        connection_pool=SimpleNamespace(
            connection_kwargs=conn_kwargs.copy(),
            connection_class=object(),
        )
    )
    sentinel_ctor = Mock(return_value="async-sentinel-conn")
    monkeypatch.setattr(async_sentinel_module, "Sentinel", sentinel_ctor)
    monkeypatch.setattr(
        async_sentinel_module.sync_redis,
        "Redis",
        Mock(side_effect=AssertionError("should not probe")),
    )

    sentinel, service_name = async_sentinel_module.Sentinel_Conn(
        conn,
        ssl=True,
        service_name="mymaster",
        sentinel_nodes=[("s1", 26379)],
    )

    assert sentinel == "async-sentinel-conn"
    assert service_name == "mymaster"
    sentinel_ctor.assert_called_once()
    assert sentinel_ctor.call_args.args[0] == [("s1", 26379)]
    assert sentinel_ctor.call_args.kwargs["sentinel_kwargs"] == {
        "username": "user",
        "password": "pass",
        "ssl": True,
    }
    assert sentinel_ctor.call_args.kwargs["socket_timeout"] == 1
    assert conn.connection_pool.connection_kwargs == conn_kwargs


def test_async_sentinel_conn_discovers_single_master(monkeypatch):
    probe = SimpleNamespace(
        sentinel_masters=Mock(return_value={"mymaster": {}}),
        close=Mock(),
    )
    probe_ctor = Mock(return_value=probe)
    monkeypatch.setattr(async_sentinel_module.sync_redis, "Redis", probe_ctor)
    sentinel_ctor = Mock(return_value="async-sentinel-conn")
    monkeypatch.setattr(async_sentinel_module, "Sentinel", sentinel_ctor)

    conn = SimpleNamespace(
        connection_pool=SimpleNamespace(
            connection_kwargs={"host": "127.0.0.1", "port": 26379},
            connection_class=async_sentinel_module.redis.SSLConnection,
        )
    )

    _, service_name = async_sentinel_module.Sentinel_Conn(conn, ssl=False)
    assert service_name == "mymaster"
    probe.sentinel_masters.assert_called_once()
    probe.close.assert_called_once()
    assert sentinel_ctor.call_args.args[0] == [("127.0.0.1", 26379)]
    probe_kwargs = probe_ctor.call_args.kwargs
    assert probe_kwargs["host"] == "127.0.0.1"
    assert probe_kwargs["port"] == 26379
    assert probe_kwargs["ssl"] is True
    assert probe_kwargs["decode_responses"] is True


def test_async_sentinel_conn_raises_on_multiple_masters(monkeypatch):
    probe = SimpleNamespace(
        sentinel_masters=Mock(return_value={"m1": {}, "m2": {}}),
        close=Mock(),
    )
    monkeypatch.setattr(
        async_sentinel_module.sync_redis, "Redis", Mock(return_value=probe)
    )
    conn = SimpleNamespace(
        connection_pool=SimpleNamespace(
            connection_kwargs={"host": "127.0.0.1", "port": 26379},
            connection_class=async_sentinel_module.redis.SSLConnection,
        )
    )
    with pytest.raises(Exception, match="Multiple masters"):
        async_sentinel_module.Sentinel_Conn(conn, ssl=False)
    probe.close.assert_called_once()


def test_sync_cluster_primaries_target_prefers_get_primaries():
    db = object.__new__(SyncFalkorDB)
    db.connection = SimpleNamespace(get_primaries=lambda: ("p1", "p2"), PRIMARIES="all")
    assert db._cluster_primaries_target() == ["p1", "p2"]


def test_sync_normalize_cluster_fanout_response_handles_edges():
    db = object.__new__(SyncFalkorDB)
    assert db._normalize_cluster_fanout_response("OK") == "OK"
    with pytest.raises(RedisError, match="No responses"):
        db._normalize_cluster_fanout_response({})
    with pytest.raises(RedisError, match="Inconsistent responses"):
        db._normalize_cluster_fanout_response({"n1": "OK", "n2": RedisError("boom")})


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


def test_sync_udf_cluster_load_and_list_use_node_targets():
    db = object.__new__(SyncFalkorDB)
    execute_command = Mock(return_value={"p1": "OK", "p2": "OK"})
    db.connection = SimpleNamespace(
        execute_command=execute_command,
        get_primaries=lambda: ("p1", "p2"),
    )
    db._topology_mode = "cluster"

    load_result = db.udf_load("lib", "code", replace=True)
    list_result = db.udf_list("lib", with_code=True)

    assert load_result == "OK"
    assert list_result == "OK"
    execute_command.assert_any_call(
        "GRAPH.UDF",
        "LOAD",
        "REPLACE",
        "lib",
        "code",
        target_nodes=["p1", "p2"],
    )
    execute_command.assert_any_call(
        "GRAPH.UDF",
        "LIST",
        "lib",
        "WITHCODE",
        target_nodes=["p1", "p2"],
    )


@pytest.mark.asyncio
async def test_async_cluster_primaries_target_prefers_get_primaries():
    db = object.__new__(AsyncFalkorDB)
    db.connection = SimpleNamespace(get_primaries=lambda: ("p1", "p2"), PRIMARIES="all")
    assert db._cluster_primaries_target() == ["p1", "p2"]


@pytest.mark.asyncio
async def test_async_normalize_cluster_fanout_response_handles_edges():
    db = object.__new__(AsyncFalkorDB)
    assert db._normalize_cluster_fanout_response("OK") == "OK"
    with pytest.raises(RedisError, match="No responses"):
        db._normalize_cluster_fanout_response({})
    with pytest.raises(RedisError, match="Inconsistent responses"):
        db._normalize_cluster_fanout_response({"n1": "OK", "n2": RedisError("boom")})


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
async def test_async_udf_cluster_load_and_list_use_node_targets():
    db = object.__new__(AsyncFalkorDB)
    execute_command = AsyncMock(return_value={"p1": "OK", "p2": "OK"})
    db.connection = SimpleNamespace(
        execute_command=execute_command,
        get_primaries=lambda: ("p1", "p2"),
    )
    db._topology_mode = "cluster"

    load_result = await db.udf_load("lib", "code", replace=True)
    list_result = await db.udf_list("lib", with_code=True)

    assert load_result == "OK"
    assert list_result == "OK"
    execute_command.assert_any_await(
        "GRAPH.UDF",
        "LOAD",
        "REPLACE",
        "lib",
        "code",
        target_nodes=["p1", "p2"],
    )
    execute_command.assert_any_await(
        "GRAPH.UDF",
        "LIST",
        "lib",
        "WITHCODE",
        target_nodes=["p1", "p2"],
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


def test_sync_topology_mode_invalid_raises():
    with pytest.raises(ValueError, match="Invalid topology_mode"):
        SyncFalkorDB(topology_mode="invalid")


@pytest.mark.asyncio
async def test_async_topology_mode_invalid_raises():
    with pytest.raises(ValueError, match="Invalid topology_mode"):
        AsyncFalkorDB(topology_mode="invalid")


def test_topology_mode_standalone_skips_auto_probe(monkeypatch):
    fake_conn = _sync_conn({})
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


def test_sync_auto_topology_resolves_standalone(monkeypatch):
    fake_conn = _sync_conn({})
    monkeypatch.setattr(sync_falkordb_module.redis, "Redis", lambda **kwargs: fake_conn)
    monkeypatch.setattr(sync_falkordb_module, "Is_Sentinel", lambda conn: False)
    monkeypatch.setattr(sync_falkordb_module, "Is_Cluster", lambda conn: False)

    db = SyncFalkorDB(topology_mode="auto")
    assert db._topology_mode == "standalone"


def test_sync_explicit_cluster_skips_auto_probes(monkeypatch):
    base_conn = _sync_conn({"host": "localhost", "port": 6379})
    cluster_conn = _sync_conn({"host": "localhost", "port": 6379})
    cluster_conn_builder = Mock(return_value=cluster_conn)

    monkeypatch.setattr(sync_falkordb_module.redis, "Redis", lambda **kwargs: base_conn)
    monkeypatch.setattr(sync_falkordb_module, "Cluster_Conn", cluster_conn_builder)
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

    db = SyncFalkorDB(topology_mode="cluster")
    assert db._topology_mode == "cluster"
    assert db.connection is cluster_conn
    cluster_conn_builder.assert_called_once()


def test_sync_explicit_sentinel_skips_cluster_probe(monkeypatch):
    base_conn = _sync_conn({"host": "localhost", "port": 26379})
    master_conn = _sync_conn({"host": "localhost", "port": 6379})
    sentinel_obj = SimpleNamespace(master_for=Mock(return_value=master_conn))
    sentinel_conn_builder = Mock(return_value=(sentinel_obj, "mymaster"))

    monkeypatch.setattr(sync_falkordb_module.redis, "Redis", lambda **kwargs: base_conn)
    monkeypatch.setattr(sync_falkordb_module, "Sentinel_Conn", sentinel_conn_builder)
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

    db = SyncFalkorDB(
        topology_mode="sentinel",
        sentinel_service_name="mymaster",
        sentinel_nodes=[("localhost", 26379)],
    )
    assert db._topology_mode == "sentinel"
    assert db.connection is master_conn
    sentinel_conn_builder.assert_called_once()
    sentinel_obj.master_for.assert_called_once_with("mymaster", ssl=False)


@pytest.mark.asyncio
async def test_async_auto_topology_resolves_standalone(monkeypatch):
    fake_conn = _async_conn({})
    monkeypatch.setattr(
        async_falkordb_module.redis, "Redis", lambda **kwargs: fake_conn
    )
    monkeypatch.setattr(async_falkordb_module, "Is_Sentinel", lambda conn: False)
    monkeypatch.setattr(async_falkordb_module, "Is_Cluster", lambda conn: False)

    db = AsyncFalkorDB(topology_mode="auto")
    assert db._topology_mode == "standalone"


@pytest.mark.asyncio
async def test_async_explicit_cluster_skips_auto_probes(monkeypatch):
    base_conn = _async_conn({"host": "localhost", "port": 6379})
    cluster_conn = _async_conn({"host": "localhost", "port": 6379})
    cluster_conn_builder = Mock(return_value=cluster_conn)

    monkeypatch.setattr(
        async_falkordb_module.redis, "Redis", lambda **kwargs: base_conn
    )
    monkeypatch.setattr(async_falkordb_module, "Cluster_Conn", cluster_conn_builder)
    monkeypatch.setattr(
        async_falkordb_module,
        "Is_Sentinel",
        lambda conn: (_ for _ in ()).throw(AssertionError("should not probe sentinel")),
    )
    monkeypatch.setattr(
        async_falkordb_module,
        "Is_Cluster",
        lambda conn: (_ for _ in ()).throw(AssertionError("should not probe cluster")),
    )

    db = AsyncFalkorDB(topology_mode="cluster")
    assert db._topology_mode == "cluster"
    assert db.connection is cluster_conn
    cluster_conn_builder.assert_called_once()


@pytest.mark.asyncio
async def test_async_explicit_sentinel_skips_cluster_probe(monkeypatch):
    base_conn = _async_conn({"host": "localhost", "port": 26379})
    master_conn = _async_conn({"host": "localhost", "port": 6379})
    sentinel_obj = SimpleNamespace(master_for=Mock(return_value=master_conn))
    sentinel_conn_builder = Mock(return_value=(sentinel_obj, "mymaster"))

    monkeypatch.setattr(
        async_falkordb_module.redis, "Redis", lambda **kwargs: base_conn
    )
    monkeypatch.setattr(async_falkordb_module, "Sentinel_Conn", sentinel_conn_builder)
    monkeypatch.setattr(
        async_falkordb_module,
        "Is_Sentinel",
        lambda conn: (_ for _ in ()).throw(AssertionError("should not probe sentinel")),
    )
    monkeypatch.setattr(
        async_falkordb_module,
        "Is_Cluster",
        lambda conn: (_ for _ in ()).throw(AssertionError("should not probe cluster")),
    )

    db = AsyncFalkorDB(
        topology_mode="sentinel",
        sentinel_service_name="mymaster",
        sentinel_nodes=[("localhost", 26379)],
    )
    assert db._topology_mode == "sentinel"
    assert db.connection is master_conn
    sentinel_conn_builder.assert_called_once()
    sentinel_obj.master_for.assert_called_once_with("mymaster", ssl=False)


def test_sync_from_url_forwards_client_kwargs(monkeypatch):
    redis_conn = SimpleNamespace(connection_pool="POOL")
    from_url = Mock(return_value=redis_conn)
    monkeypatch.setattr(sync_falkordb_module.redis, "from_url", from_url)
    captured = {}

    class DummySyncFalkorDB(SyncFalkorDB):
        def __init__(self, **kwargs):
            captured.update(kwargs)

    DummySyncFalkorDB.from_url(
        "falkor://localhost:6379",
        topology_mode="cluster",
        dynamic_startup_nodes=False,
        socket_timeout=5,
    )

    from_url.assert_called_once_with(
        "redis://localhost:6379",
        socket_timeout=5,
        decode_responses=True,
    )
    assert captured["connection_pool"] == "POOL"
    assert captured["topology_mode"] == "cluster"
    assert captured["dynamic_startup_nodes"] is False


def test_async_from_url_forwards_client_kwargs(monkeypatch):
    redis_conn = SimpleNamespace(connection_pool="POOL")
    from_url = Mock(return_value=redis_conn)
    monkeypatch.setattr(async_falkordb_module.redis, "from_url", from_url)
    captured = {}

    class DummyAsyncFalkorDB(AsyncFalkorDB):
        def __init__(self, **kwargs):
            captured.update(kwargs)

    DummyAsyncFalkorDB.from_url(
        "falkor://localhost:6379",
        topology_mode="cluster",
        dynamic_startup_nodes=False,
        socket_timeout=5,
    )

    from_url.assert_called_once_with(
        "redis://localhost:6379",
        socket_timeout=5,
        decode_responses=True,
    )
    assert captured["connection_pool"] == "POOL"
    assert captured["topology_mode"] == "cluster"
    assert captured["dynamic_startup_nodes"] is False
