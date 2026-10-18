from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.asyncio import ConnectionPool, SSLConnection
from redis.asyncio.sentinel import Sentinel

from falkordb.asyncio import FalkorDB
from falkordb.asyncio.sentinel import (
    FalkorDBSentinelManagedConnection,
    FalkorDBSentinelManagedSSLConnection,
    Is_Sentinel,
    Sentinel_Conn,
    Sentinel_Master,
)

OLD_MASTER = ("10.0.0.1", 6379)
NEW_MASTER = ("10.0.0.2", 6379)


@pytest.fixture
def sentinel():
    sentinel = Sentinel([("localhost", 26379)])
    sentinel.discover_master = AsyncMock(return_value=OLD_MASTER)
    return sentinel


@pytest.fixture
def connected_to():
    """Patch the socket layer and record the address of every connect."""
    addresses = []

    async def fake_connect(self):
        addresses.append((self.host, self.port))
        self._reader = MagicMock()
        self._writer = MagicMock(drain=AsyncMock(), wait_closed=AsyncMock())

    with (
        patch("redis.asyncio.connection.Connection._connect", fake_connect),
        patch(
            "redis.asyncio.connection.AbstractConnection.on_connect_check_health",
            AsyncMock(),
        ),
    ):
        yield addresses


def get_connection(sentinel, ssl=False):
    # connections hold a weak reference to their pool: callers keep the pool
    pool = Sentinel_Master(sentinel, "mymaster", ssl).connection_pool
    return pool, pool.make_connection()


@pytest.mark.parametrize(
    ("ssl", "expected"),
    [
        (False, FalkorDBSentinelManagedConnection),
        (True, FalkorDBSentinelManagedSSLConnection),
    ],
)
def test_master_uses_rediscovering_connection(sentinel, ssl, expected):
    client = Sentinel_Master(sentinel, "mymaster", ssl)
    assert client.connection_pool.connection_class is expected


async def test_connect_discovers_master_once(sentinel, connected_to):
    _pool, conn = get_connection(sentinel)
    await conn.connect()

    assert connected_to == [OLD_MASTER]
    sentinel.discover_master.assert_awaited_once_with("mymaster")


@pytest.mark.parametrize("ssl", [False, True])
async def test_reconnect_rediscovers_master(sentinel, connected_to, ssl):
    _pool, conn = get_connection(sentinel, ssl)
    await conn.connect()

    # failover: sentinel now reports a new master, connection dropped
    sentinel.discover_master.return_value = NEW_MASTER
    await conn.disconnect()

    # the path redis-py takes when retrying a command on a dropped connection
    await conn.connect_check_health(check_health=False)

    assert connected_to == [OLD_MASTER, NEW_MASTER]
    assert (conn.host, conn.port) == NEW_MASTER


@pytest.mark.parametrize("ssl", [False, True])
async def test_send_command_after_failover_reaches_new_master(
    sentinel, connected_to, ssl
):
    _pool, conn = get_connection(sentinel, ssl)
    await conn.connect()

    sentinel.discover_master.return_value = NEW_MASTER
    await conn.disconnect()
    await conn.send_command("PING")

    assert connected_to[-1] == NEW_MASTER


async def test_connected_socket_is_kept(sentinel, connected_to):
    _pool, conn = get_connection(sentinel)
    await conn.connect()

    sentinel.discover_master.reset_mock()
    await conn.connect_check_health(check_health=False)

    assert connected_to == [OLD_MASTER]
    sentinel.discover_master.assert_not_awaited()


@pytest.mark.parametrize(
    ("mode", "expected"), [("sentinel", True), ("standalone", False)]
)
def test_is_sentinel(mode, expected):
    conn = SimpleNamespace(connection_pool=ConnectionPool(host="localhost"))

    with patch(
        "falkordb.asyncio.cluster.sync_redis.Redis.info",
        return_value={"redis_mode": mode},
    ):
        assert Is_Sentinel(conn) is expected


@pytest.mark.parametrize("ssl", [False, True])
def test_sentinel_conn(ssl):
    pool = ConnectionPool(
        host="sentinel-host", port=26379, username="user", password="pass"
    )
    conn = SimpleNamespace(connection_pool=pool)

    with patch(
        "falkordb.asyncio.cluster.sync_redis.Redis.sentinel_masters",
        return_value={"mymaster": {}},
    ):
        sentinel, service_name = Sentinel_Conn(conn, ssl)

    assert isinstance(sentinel, Sentinel)
    assert service_name == "mymaster"
    sentinel_pool = sentinel.sentinels[0].connection_pool
    assert sentinel_pool.connection_kwargs["host"] == "sentinel-host"
    assert sentinel_pool.connection_kwargs["port"] == 26379
    assert sentinel_pool.connection_kwargs["username"] == "user"
    assert sentinel_pool.connection_kwargs["password"] == "pass"
    assert issubclass(sentinel_pool.connection_class, SSLConnection) is ssl


def test_sentinel_conn_multiple_masters():
    conn = SimpleNamespace(connection_pool=ConnectionPool(host="localhost"))

    with patch(
        "falkordb.asyncio.cluster.sync_redis.Redis.sentinel_masters",
        return_value={"a": {}, "b": {}},
    ):
        with pytest.raises(Exception, match="Multiple masters"):
            Sentinel_Conn(conn, False)


async def test_falkordb_uses_rediscovering_connection(sentinel):
    with (
        patch("falkordb.asyncio.falkordb.Is_Sentinel", return_value=True),
        patch("falkordb.asyncio.falkordb.Is_Cluster") as is_cluster,
        patch(
            "falkordb.asyncio.falkordb.Sentinel_Conn",
            return_value=(sentinel, "mymaster"),
        ),
    ):
        db = FalkorDB(port=26379)

    # sentinel and cluster are exclusive: the master is not probed for cluster
    is_cluster.assert_not_called()
    pool = db.connection.connection_pool
    assert pool.connection_class is FalkorDBSentinelManagedConnection
    assert pool.service_name == "mymaster"
    await db.aclose()


async def test_falkordb_from_tls_url_uses_ssl_connection(sentinel):
    with (
        patch("falkordb.asyncio.falkordb.Is_Sentinel", return_value=True),
        patch("falkordb.asyncio.falkordb.Is_Cluster", return_value=False),
        patch(
            "falkordb.asyncio.falkordb.Sentinel_Conn",
            return_value=(sentinel, "mymaster"),
        ) as sentinel_conn,
    ):
        db = FalkorDB.from_url("falkors://localhost:26379")

    assert sentinel_conn.call_args.args[1] is True
    pool = db.connection.connection_pool
    assert pool.connection_class is FalkorDBSentinelManagedSSLConnection
    await db.aclose()


async def test_falkordb_udf_on_sentinel_runs_on_master(sentinel):
    with (
        patch("falkordb.asyncio.falkordb.Is_Sentinel", return_value=True),
        patch(
            "falkordb.asyncio.falkordb.Sentinel_Conn",
            return_value=(sentinel, "mymaster"),
        ),
    ):
        db = FalkorDB(port=26379)

    db.connection.execute_command = AsyncMock(return_value="OK")
    with patch("falkordb.asyncio.falkordb.Is_Cluster") as is_cluster:
        assert await db.udf_flush() == "OK"

    is_cluster.assert_not_called()
    db.connection.execute_command.assert_awaited_once_with("GRAPH.UDF", "FLUSH")
    await db.aclose()
