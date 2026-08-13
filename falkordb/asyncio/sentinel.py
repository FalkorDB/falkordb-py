import inspect

import redis as sync_redis  # type: ignore[import-not-found]
import redis.asyncio as redis  # type: ignore[import-not-found]
from redis.asyncio.sentinel import Sentinel  # type: ignore[import-not-found]


# detect if a connection is a sentinel
def Is_Sentinel(conn: redis.Redis) -> bool:
    pool = conn.connection_pool
    kwargs = pool.connection_kwargs.copy()

    kwargs["ssl"] = pool.connection_class is redis.SSLConnection

    if pool.connection_class is redis.UnixDomainSocketConnection:
        kwargs["unix_socket_path"] = kwargs.pop("path")

    accepted = inspect.signature(sync_redis.Redis.__init__).parameters
    kwargs = {k: v for k, v in kwargs.items() if k in accepted}

    info = sync_redis.Redis(**kwargs).info(section="server")
    return "redis_mode" in info and info["redis_mode"] == "sentinel"


# create an async sentinel connection from a Redis connection
def Sentinel_Conn(conn: redis.Redis, ssl: bool):
    pool = conn.connection_pool
    kwargs = pool.connection_kwargs.copy()

    kwargs["ssl"] = pool.connection_class is redis.SSLConnection

    if pool.connection_class is redis.UnixDomainSocketConnection:
        kwargs["unix_socket_path"] = kwargs.pop("path")

    accepted = inspect.signature(sync_redis.Redis.__init__).parameters
    probe_kwargs = {k: v for k, v in kwargs.items() if k in accepted}

    sync_conn = sync_redis.Redis(**probe_kwargs)
    masters = sync_conn.sentinel_masters()

    if len(masters) != 1:
        raise Exception("Multiple masters, require service name")

    service_name = list(masters.keys())[0]

    host = kwargs.get("host", "localhost")
    port = kwargs.get("port", 6379)
    sentinels_conns = [(host, port)]

    sentinel_kwargs = {}
    if "username" in kwargs:
        sentinel_kwargs["username"] = kwargs["username"]
    if "password" in kwargs:
        sentinel_kwargs["password"] = kwargs["password"]
    if ssl:
        sentinel_kwargs["ssl"] = True

    return (
        Sentinel(sentinels_conns, sentinel_kwargs=sentinel_kwargs, **kwargs),
        service_name,
    )
