from unittest.mock import MagicMock, patch

import pytest
from redis.backoff import NoBackoff
from redis.exceptions import ConnectionError, ReadOnlyError, TimeoutError
from redis.retry import Retry
from redis.sentinel import Sentinel

from falkordb import FalkorDB
from falkordb.sentinel import (
    NOT_EXECUTED,
    FalkorDBSentinelManagedConnection,
    FalkorDBSentinelManagedSSLConnection,
    Is_Write,
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


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (("GRAPH.QUERY", "g", "CREATE ()"), True),
        (("GRAPH.RO_QUERY", "g", "MATCH (n) RETURN n"), False),
        (("graph.ro_query", "g", "MATCH (n) RETURN n"), False),
        (("GRAPH.DELETE", "g"), True),
        (("GRAPH.CONFIG", "GET", "TIMEOUT"), False),
        (("GRAPH.CONFIG", "SET", "TIMEOUT", 10), True),
        (("GRAPH.UDF", "LIST"), False),
        (("GRAPH.UDF", "LOAD", "lib", "code"), True),
        (("SET", "k", "v"), True),
        (("GET", "k"), False),
        (("HGETALL", "k"), False),
        (("GRAPH.SLOWLOG", "g"), False),
        (("GRAPH.SLOWLOG", "g", "RESET"), True),
        (("UNKNOWN.COMMAND",), True),
        ((b"GET", b"k"), False),
        ((b"GRAPH.CONFIG", b"GET", b"TIMEOUT"), False),
        ((b"GRAPH.SLOWLOG", b"g", b"RESET"), True),
    ],
)
def test_is_write(args, expected):
    assert Is_Write(args) is expected


@pytest.fixture
def server_errors():
    """Make the server side fail with the queued errors, then answer OK."""
    errors = []
    calls = []

    def fake_send(self, conn, command_name, *args, **options):
        calls.append(args)
        if errors:
            raise errors.pop(0)
        return "OK"

    with patch("redis.client.Redis._send_command_parse_response", fake_send):
        yield errors, calls


def get_client(sentinel, retry_writes=False):
    client = Sentinel_Master(sentinel, "mymaster", False, retry_writes)
    client.connection_pool.connection_kwargs["retry"] = Retry(NoBackoff(), 3)
    return client


def test_write_not_resent_after_ambiguous_error(sentinel, connected_to, server_errors):
    errors, calls = server_errors
    errors.append(ConnectionError("Connection closed by server."))

    with pytest.raises(ConnectionError, match="Connection closed"):
        get_client(sentinel).execute_command("GRAPH.QUERY", "g", "CREATE ()")

    assert len(calls) == 1


def test_write_timeout_not_resent(sentinel, connected_to, server_errors):
    errors, calls = server_errors
    errors.append(TimeoutError("Timeout reading from socket"))

    with pytest.raises(TimeoutError):
        get_client(sentinel).execute_command("GRAPH.QUERY", "g", "CREATE ()")

    assert len(calls) == 1


def test_write_resent_when_opted_in(sentinel, connected_to, server_errors):
    errors, calls = server_errors
    errors.append(ConnectionError("Connection closed by server."))

    client = get_client(sentinel, retry_writes=True)
    assert client.execute_command("GRAPH.QUERY", "g", "CREATE ()") == "OK"
    assert len(calls) == 2


def test_read_resent_after_ambiguous_error(sentinel, connected_to, server_errors):
    errors, calls = server_errors
    errors.append(ConnectionError("Connection closed by server."))

    client = get_client(sentinel)
    assert client.execute_command("GRAPH.RO_QUERY", "g", "RETURN 1") == "OK"
    assert len(calls) == 2


def test_write_resent_when_it_never_ran(sentinel, connected_to, server_errors):
    errors, calls = server_errors
    error = ConnectionError("The previous master is now a slave")
    setattr(error, NOT_EXECUTED, True)
    errors.append(error)

    client = get_client(sentinel)
    assert client.execute_command("GRAPH.QUERY", "g", "CREATE ()") == "OK"
    assert len(calls) == 2


def test_readonly_marks_error_not_executed(sentinel, connected_to):
    conn = get_connection(sentinel)
    conn.connect()

    with patch(
        "redis.connection.Connection.read_response",
        side_effect=ReadOnlyError("You can't write against a read only replica."),
    ):
        with pytest.raises(ConnectionError) as info:
            conn.read_response()

    assert getattr(info.value, NOT_EXECUTED, False)


def test_connect_failure_marks_error_not_executed(sentinel):
    conn = get_connection(sentinel)

    with patch(
        "redis.connection.Connection._connect",
        side_effect=ConnectionError("Connection refused"),
    ):
        with pytest.raises(ConnectionError) as info:
            conn.connect_check_health(check_health=False, retry_socket_connect=False)

    assert getattr(info.value, NOT_EXECUTED, False)


def test_falkordb_from_url_passes_retry_writes(sentinel):
    with (
        patch("falkordb.falkordb.Is_Sentinel", return_value=True),
        patch("falkordb.falkordb.Is_Cluster", return_value=False),
        patch("falkordb.falkordb.Sentinel_Conn", return_value=(sentinel, "mymaster")),
    ):
        db = FalkorDB.from_url("falkor://localhost:26379", retry_writes=True)

    assert db.connection.retry_writes is True
