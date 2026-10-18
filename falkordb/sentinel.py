from redis import Redis
from redis.commands import READ_COMMANDS  # type: ignore[attr-defined]
from redis.connection import SSLConnection
from redis.exceptions import ConnectionError, ReadOnlyError, TimeoutError
from redis.sentinel import (  # type: ignore[import-not-found]
    Sentinel,
    SentinelManagedConnection,
)

# commands that never modify data, safe to resend after a connection drop
READ_ONLY_COMMANDS = READ_COMMANDS | {
    "GRAPH.RO_QUERY",
    "GRAPH.EXPLAIN",
    "GRAPH.LIST",
    "GRAPH.INFO",
    "GRAPH.MEMORY",
    "PING",
    "INFO",
}

# read-only subcommands of commands that can also write
READ_ONLY_SUBCOMMANDS = {
    "GRAPH.CONFIG": {"GET"},
    "GRAPH.UDF": {"LIST"},
    "GRAPH.CONSTRAINT": {"LIST"},
}

# marks an error raised before the command could run on the server
NOT_EXECUTED = "_falkordb_not_executed"


def _Name(arg):
    if isinstance(arg, (bytes, bytearray, memoryview)):
        arg = bytes(arg).decode("utf-8", errors="replace")
    return str(arg).upper()


def Is_Write(args):
    command = _Name(args[0])
    if command in READ_ONLY_COMMANDS:
        return False
    if command == "GRAPH.SLOWLOG":
        # GRAPH.SLOWLOG <graph> [RESET]
        return len(args) > 2 and _Name(args[2]) == "RESET"
    subcommands = READ_ONLY_SUBCOMMANDS.get(command)
    if subcommands is not None and len(args) > 1:
        return _Name(args[1]) not in subcommands
    # unknown commands are treated as writes
    return True


class FalkorDBSentinelManagedConnection(SentinelManagedConnection):
    """
    Sentinel managed connection that asks sentinel for the current master
    on every reconnect.

    redis-py only queries sentinel in ``connect()``. A command retried inside
    redis-py reconnects through ``connect_check_health()`` instead, which
    reuses the last known host: the old master, after a failover. This class
    re-discovers the master on that path as well.
    """

    def connect_to(self, address):
        self.host, self.port = address
        # skip our override: the address was just resolved by sentinel
        SentinelManagedConnection.connect_check_health(
            self,
            check_health=self.connection_pool.check_connection,
            retry_socket_connect=False,
        )

    def connect_check_health(self, *args, **kwargs):
        try:
            if not self._sock and self.connection_pool.is_master:
                self.host, self.port = self.connection_pool.get_master_address()
            return super().connect_check_health(*args, **kwargs)
        except (ConnectionError, TimeoutError) as e:
            # failed while connecting, before the command was sent
            setattr(e, NOT_EXECUTED, True)
            raise

    def read_response(self, *args, **kwargs):
        try:
            return super().read_response(*args, **kwargs)
        except ConnectionError as e:
            # the old master refused the command with READONLY
            if isinstance(e.__context__, ReadOnlyError):
                setattr(e, NOT_EXECUTED, True)
            raise


class FalkorDBSentinelManagedSSLConnection(
    FalkorDBSentinelManagedConnection, SSLConnection
):
    pass


class _WriteNotRetried(Exception):
    def __init__(self, error):
        self.error = error


class FalkorDBSentinelRedis(Redis):
    """
    Sentinel master client that does not resend a write after an ambiguous
    failure.

    A dropped connection or a timeout does not tell whether a write ran on
    the server: resending it may run it twice. Such writes are resent only
    when ``retry_writes`` is set. Reads, and failures that prove the command
    never ran (connect errors, READONLY from a demoted master), follow the
    client's retry policy as usual.
    """

    retry_writes = False

    def _execute_command(self, *args, **options):
        try:
            return super()._execute_command(*args, **options)
        except _WriteNotRetried as e:
            raise e.error from None

    def _send_command_parse_response(self, conn, command_name, *args, **options):
        try:
            return super()._send_command_parse_response(
                conn, command_name, *args, **options
            )
        except (ConnectionError, TimeoutError) as e:
            if (
                not self.retry_writes
                and not getattr(e, NOT_EXECUTED, False)
                and Is_Write(args)
            ):
                # escape redis-py's retry loop
                conn.disconnect()
                raise _WriteNotRetried(e)
            raise


# detect if a connection is a sentinel
def Is_Sentinel(conn):
    info = conn.info(section="server")
    return "redis_mode" in info and info["redis_mode"] == "sentinel"


# create a sentinel connection from a Redis connection
def Sentinel_Conn(conn, ssl):
    # collect masters
    masters = conn.sentinel_masters()

    # abort if multiple masters are detected
    if len(masters) != 1:
        raise Exception("Multiple masters, require service name")

    # monitored service name
    service_name = list(masters.keys())[0]

    # list of sentinels connection information
    sentinels_conns = []

    # current sentinel
    host = conn.connection_pool.connection_kwargs["host"]
    port = conn.connection_pool.connection_kwargs["port"]
    sentinels_conns.append((host, port))

    # additional sentinels
    # sentinels = conn.sentinel_sentinels(service_name)
    # for sentinel in sentinels:
    #    ip = sentinel['ip']
    #    port = sentinel['port']
    #    sentinels_conns.append((host, port))

    # use the same connection arguments e.g. username and password
    connection_kwargs = conn.connection_pool.connection_kwargs

    # construct sentinel arguments
    sentinel_kwargs = {}
    if "username" in connection_kwargs:
        sentinel_kwargs["username"] = connection_kwargs["username"]
    if "password" in connection_kwargs:
        sentinel_kwargs["password"] = connection_kwargs["password"]
    if ssl:
        sentinel_kwargs["ssl"] = True

    return (
        Sentinel(sentinels_conns, sentinel_kwargs=sentinel_kwargs, **connection_kwargs),
        service_name,
    )


# create a client for the monitored master
def Sentinel_Master(sentinel, service_name, ssl, retry_writes=False):
    connection_class = (
        FalkorDBSentinelManagedSSLConnection
        if ssl
        else FalkorDBSentinelManagedConnection
    )
    conn = sentinel.master_for(
        service_name,
        redis_class=FalkorDBSentinelRedis,
        connection_class=connection_class,
    )
    conn.retry_writes = retry_writes
    return conn
