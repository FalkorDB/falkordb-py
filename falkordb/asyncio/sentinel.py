from typing import cast

import redis as sync_redis  # type: ignore[import-not-found]
import redis.asyncio as redis  # type: ignore[import-not-found]
from redis.asyncio.sentinel import Sentinel  # type: ignore[import-not-found]

ASYNC_PROBE_KWARGS = {
    "client_name",
    "credential_provider",
    "db",
    "host",
    "password",
    "path",
    "port",
    "protocol",
    "socket_connect_timeout",
    "socket_keepalive",
    "socket_keepalive_options",
    "socket_timeout",
    "ssl",
    "ssl_ca_certs",
    "ssl_ca_data",
    "ssl_cert_reqs",
    "ssl_check_hostname",
    "ssl_certfile",
    "ssl_keyfile",
    "username",
}


def _is_ssl_connection(pool) -> bool:
    try:
        return issubclass(pool.connection_class, redis.SSLConnection)
    except TypeError:
        return pool.connection_class is redis.SSLConnection


def _probe_kwargs(kwargs):
    probe_kwargs = {
        key: value for key, value in kwargs.items() if key in ASYNC_PROBE_KWARGS
    }
    probe_kwargs["decode_responses"] = True
    return probe_kwargs


# detect if a connection is a sentinel
def Is_Sentinel(conn: redis.Redis):
    pool = conn.connection_pool
    kwargs = pool.connection_kwargs.copy()
    kwargs["ssl"] = _is_ssl_connection(pool)

    probe = sync_redis.Redis(**_probe_kwargs(kwargs))
    try:
        info = cast(dict, probe.info(section="server"))
    finally:
        probe.close()
    return info.get("redis_mode") == "sentinel"


# create a sentinel connection from a Redis connection
def Sentinel_Conn(conn, ssl, service_name=None, sentinel_nodes=None):
    connection_kwargs = conn.connection_pool.connection_kwargs.copy()

    sentinels_conns = list(sentinel_nodes or [])
    if len(sentinels_conns) == 0:
        host = connection_kwargs.get("host")
        port = connection_kwargs.get("port")
        sentinels_conns.append((host, port))

    if service_name is None:
        pool = conn.connection_pool
        probe_kwargs = connection_kwargs.copy()
        probe_kwargs["ssl"] = _is_ssl_connection(pool)
        probe = sync_redis.Redis(**_probe_kwargs(probe_kwargs))
        try:
            masters = cast(dict, probe.sentinel_masters())
        finally:
            probe.close()
        if len(masters) != 1:
            raise Exception("Multiple masters, require service name")
        service_name = list(masters.keys())[0]

    sentinel_kwargs = {}
    if "username" in connection_kwargs:
        sentinel_kwargs["username"] = connection_kwargs["username"]
    if "password" in connection_kwargs:
        sentinel_kwargs["password"] = connection_kwargs["password"]
    if ssl:
        sentinel_kwargs["ssl"] = True

    connection_kwargs.pop("host", None)
    connection_kwargs.pop("port", None)
    connection_kwargs.pop("connection_pool", None)

    return (
        Sentinel(sentinels_conns, sentinel_kwargs=sentinel_kwargs, **connection_kwargs),
        service_name,
    )
