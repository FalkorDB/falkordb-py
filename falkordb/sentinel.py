from redis.connection import SSLConnection
from redis.sentinel import (  # type: ignore[import-not-found]
    Sentinel,
    SentinelManagedConnection,
)


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
        super(SentinelManagedConnection, self).connect_check_health(
            check_health=self.connection_pool.check_connection,
            retry_socket_connect=False,
        )

    def connect_check_health(self, *args, **kwargs):
        if not self._sock and self.connection_pool.is_master:
            self.host, self.port = self.connection_pool.get_master_address()
        return super().connect_check_health(*args, **kwargs)


class FalkorDBSentinelManagedSSLConnection(
    FalkorDBSentinelManagedConnection, SSLConnection
):
    pass


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
def Sentinel_Master(sentinel, service_name, ssl):
    connection_class = (
        FalkorDBSentinelManagedSSLConnection
        if ssl
        else FalkorDBSentinelManagedConnection
    )
    return sentinel.master_for(service_name, connection_class=connection_class)
