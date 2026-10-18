from unittest.mock import MagicMock, patch

import pytest
from redis.sentinel import Sentinel

from falkordb import FalkorDB
from falkordb.sentinel import (
    FalkorDBSentinelManagedConnection,
    FalkorDBSentinelManagedSSLConnection,
    Sentinel_Master,
)

OLD_MASTER = ("10.0.0.1", 6379)
NEW_MASTER = ("10.0.0.2", 6379)


@pytest.fixture
def sentinel():
    sentinel = Sentinel([("localhost", 26379)])
    sentinel.discover_master = MagicMock(return_value=OLD_MASTER)
    return sentinel


@pytest.fixture
def connected_to():
    """Patch the socket layer and record the address of every connect."""
    addresses = []

    def fake_connect(self):
        addresses.append((self.host, self.port))
        return MagicMock()

    with (
        patch("redis.connection.Connection._connect", fake_connect),
        patch("redis.connection.SSLConnection._connect", fake_connect),
        patch("redis.connection.AbstractConnection.on_connect_check_health"),
    ):
        yield addresses


def get_connection(sentinel, ssl=False):
    client = Sentinel_Master(sentinel, "mymaster", ssl)
    return client.connection_pool.make_connection()


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


def test_connect_discovers_master_once(sentinel, connected_to):
    conn = get_connection(sentinel)
    conn.connect()

    assert connected_to == [OLD_MASTER]
    sentinel.discover_master.assert_called_once_with("mymaster")


@pytest.mark.parametrize("ssl", [False, True])
def test_reconnect_rediscovers_master(sentinel, connected_to, ssl):
    conn = get_connection(sentinel, ssl)
    conn.connect()

    # failover: sentinel now reports a new master, connection dropped
    sentinel.discover_master.return_value = NEW_MASTER
    conn.disconnect()

    # the path redis-py takes when retrying a command on a dropped connection
    conn.connect_check_health(check_health=False)

    assert connected_to == [OLD_MASTER, NEW_MASTER]
    assert (conn.host, conn.port) == NEW_MASTER


@pytest.mark.parametrize("ssl", [False, True])
def test_send_command_after_failover_reaches_new_master(sentinel, connected_to, ssl):
    conn = get_connection(sentinel, ssl)
    conn.connect()

    sentinel.discover_master.return_value = NEW_MASTER
    conn.disconnect()
    conn.send_command("PING")

    assert connected_to[-1] == NEW_MASTER


def test_connected_socket_is_kept(sentinel, connected_to):
    conn = get_connection(sentinel)
    conn.connect()

    sentinel.discover_master.reset_mock()
    conn.connect_check_health(check_health=False)

    assert connected_to == [OLD_MASTER]
    sentinel.discover_master.assert_not_called()


def test_falkordb_uses_rediscovering_connection(sentinel):
    with (
        patch("falkordb.falkordb.Is_Sentinel", return_value=True),
        patch("falkordb.falkordb.Is_Cluster", return_value=False),
        patch("falkordb.falkordb.Sentinel_Conn", return_value=(sentinel, "mymaster")),
    ):
        db = FalkorDB(port=26379)

    pool = db.connection.connection_pool
    assert pool.connection_class is FalkorDBSentinelManagedConnection
    assert pool.service_name == "mymaster"


def test_falkordb_from_tls_url_uses_ssl_connection(sentinel):
    with (
        patch("falkordb.falkordb.Is_Sentinel", return_value=True),
        patch("falkordb.falkordb.Is_Cluster", return_value=False),
        patch(
            "falkordb.falkordb.Sentinel_Conn", return_value=(sentinel, "mymaster")
        ) as sentinel_conn,
    ):
        db = FalkorDB.from_url("falkors://localhost:26379")

    assert sentinel_conn.call_args.args[1] is True
    pool = db.connection.connection_pool
    assert pool.connection_class is FalkorDBSentinelManagedSSLConnection
