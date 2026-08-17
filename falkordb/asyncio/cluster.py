import inspect
import socket

import redis as sync_redis  # type: ignore[import-not-found]
import redis.asyncio as redis  # type: ignore[import-not-found]
import redis.exceptions as redis_exceptions  # type: ignore[import-not-found]
from redis.asyncio.cluster import RedisCluster  # type: ignore[import-not-found]


# detect if a connection is a cluster
def Is_Cluster(conn: redis.Redis):

    pool = conn.connection_pool
    kwargs = pool.connection_kwargs.copy()

    # Check if the connection is using SSL and add it
    # this propery is not kept in the connection_kwargs
    kwargs["ssl"] = pool.connection_class is redis.SSLConnection

    # The async Unix-domain-socket pool stores the socket path under "path",
    # but the synchronous redis.Redis constructor expects "unix_socket_path".
    # Translate the key so the sync probe can be built for unix:// connections.
    if pool.connection_class is redis.UnixDomainSocketConnection:
        kwargs["unix_socket_path"] = kwargs.pop("path")

    # redis.asyncio.retry.Retry and the connect hook are asyncio-specific: they
    # are valid parameter *names* on the sync client, so the signature filter
    # below keeps them, but the sync client would call them and get back an
    # un-awaited coroutine. credential_provider is deliberately NOT dropped --
    # redis-py has a single CredentialProvider class whose get_credentials() is
    # synchronous, and removing it would leave the probe unauthenticated
    # because username/password are None whenever a provider is in use.
    for async_only in ("retry", "redis_connect_func"):
        kwargs.pop(async_only, None)

    # Keep only the parameters the synchronous constructor actually accepts.
    # redis-py stores internal state in ``connection_kwargs`` that is not part
    # of the ``Redis.__init__`` signature — redis 8.1.0 added ``himport_registry``
    # there — and forwarding those raises TypeError before any I/O happens.
    accepted = inspect.signature(sync_redis.Redis.__init__).parameters
    kwargs = {k: v for k, v in kwargs.items() if k in accepted}

    # Create a synchronous Redis client with the same parameters
    # as the connection pool just to keep Is_Cluster synchronous
    probe = sync_redis.Redis(**kwargs)
    try:
        info = probe.info(section="server")
    finally:
        probe.close()

    return "redis_mode" in info and info["redis_mode"] == "cluster"


# create a cluster connection from a Redis connection
def Cluster_Conn(
    conn,
    ssl,
    cluster_error_retry_attempts=3,
    startup_nodes=None,
    require_full_coverage=False,
    reinitialize_steps=5,
    read_from_replicas=False,
    address_remap=None,
    load_balancing_strategy=None,
):
    # copy, popping from the live pool dict would strip host/port/credentials
    # from a pool the caller may still be using
    connection_kwargs = conn.connection_pool.connection_kwargs.copy()
    host = connection_kwargs.pop("host")
    port = connection_kwargs.pop("port")
    username = connection_kwargs.pop("username")
    password = connection_kwargs.pop("password")

    retry = connection_kwargs.pop("retry", None)
    retry_on_error = connection_kwargs.pop(
        "retry_on_error",
        [
            ConnectionRefusedError,
            ConnectionError,
            TimeoutError,
            socket.timeout,
            redis_exceptions.ConnectionError,
        ],
    )

    # redis-py deprecated these and warns for every one it receives, only
    # forward them when the caller actually diverged from the default
    optional: dict = {}
    if cluster_error_retry_attempts != 3:
        optional["cluster_error_retry_attempts"] = cluster_error_retry_attempts
    if read_from_replicas:
        optional["read_from_replicas"] = read_from_replicas
    if load_balancing_strategy is not None:
        optional["load_balancing_strategy"] = load_balancing_strategy

    return RedisCluster(
        host=host,
        port=port,
        username=username,
        password=password,
        decode_responses=True,
        ssl=ssl,
        retry=retry,
        retry_on_error=retry_on_error,
        require_full_coverage=require_full_coverage,
        reinitialize_steps=reinitialize_steps,
        address_remap=address_remap,
        startup_nodes=startup_nodes,
        **optional,
    )
