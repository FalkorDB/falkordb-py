import inspect

from redis.asyncio import Redis  # type: ignore[import-not-found]
from redis.asyncio.connection import SSLConnection  # type: ignore[import-not-found]
from redis.asyncio.sentinel import (  # type: ignore[import-not-found]
    Sentinel,
    SentinelManagedConnection,
)
from redis.exceptions import ConnectionError, ReadOnlyError, TimeoutError

from .cluster import Sync_Conn

# commands that never modify data, safe to resend after a connection drop
READ_ONLY_COMMANDS = {
    "GRAPH.RO_QUERY",
    "GRAPH.EXPLAIN",
    "GRAPH.LIST",
    "GRAPH.INFO",
    "GRAPH.MEMORY",
    "GRAPH.SLOWLOG",
    "PING",
    "INFO",
    "EXISTS",
    "TYPE",
    "GET",
    "MGET",
    "KEYS",
    "SCAN",
    "DBSIZE",
}

# read-only subcommands of commands that can also write
READ_ONLY_SUBCOMMANDS = {
    "GRAPH.CONFIG": {"GET"},
    "GRAPH.UDF": {"LIST"},
    "GRAPH.CONSTRAINT": {"LIST"},
}

# marks an error raised before the command could run on the server
NOT_EXECUTED = "_falkordb_not_executed"


def Is_Write(args):
    command = str(args[0]).upper()
    if command in READ_ONLY_COMMANDS:
        return False
    subcommands = READ_ONLY_SUBCOMMANDS.get(command)
    if subcommands is not None and len(args) > 1:
        return str(args[1]).upper() not in subcommands
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

    async def connect_to(self, address):
        self.host, self.port = address
        # skip our override: the address was just resolved by sentinel
        await SentinelManagedConnection.connect_check_health(
            self,
            check_health=self.connection_pool.check_connection,
            retry_socket_connect=False,
        )

    async def connect_check_health(self, *args, **kwargs):
        try:
            if not self.is_connected and self.connection_pool.is_master:
                self.host, self.port = await self.connection_pool.get_master_address()
            return await super().connect_check_health(*args, **kwargs)
        except (ConnectionError, TimeoutError) as e:
            # failed while connecting, before the command was sent
            setattr(e, NOT_EXECUTED, True)
            raise

    async def read_response(self, *args, **kwargs):
        try:
            return await super().read_response(*args, **kwargs)
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

    async def execute_command(self, *args, **options):
        try:
            return await super().execute_command(*args, **options)
        except _WriteNotRetried as e:
            raise e.error from None

    async def _send_command_parse_response(self, conn, command_name, *args, **options):
        try:
            return await super()._send_command_parse_response(
                conn, command_name, *args, **options
            )
        except (ConnectionError, TimeoutError) as e:
            if (
                not self.retry_writes
                and not getattr(e, NOT_EXECUTED, False)
                and Is_Write(args)
            ):
                # escape redis-py's retry loop
                await conn.disconnect()
                raise _WriteNotRetried(e)
            raise


# detect if a connection is a sentinel
def Is_Sentinel(conn):
    # probe synchronously, to keep the async constructor synchronous
    with Sync_Conn(conn) as probe:
        info = probe.info(section="server")

    return "redis_mode" in info and info["redis_mode"] == "sentinel"


# create a sentinel connection from a Redis connection
def Sentinel_Conn(conn, ssl):
    # collect masters
    with Sync_Conn(conn) as probe:
        masters = probe.sentinel_masters()

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
        # verify sentinels with the same certificates as the master
        accepted = inspect.signature(Redis.__init__).parameters
        sentinel_kwargs.update(
            (k, v)
            for k, v in connection_kwargs.items()
            if k.startswith("ssl_") and k in accepted
        )

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
