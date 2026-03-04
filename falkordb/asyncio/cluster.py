import socket
from typing import cast

import redis as sync_redis  # type: ignore[import-not-found]
import redis.asyncio as redis  # type: ignore[import-not-found]
import redis.exceptions as redis_exceptions  # type: ignore[import-not-found]
from redis.asyncio.cluster import RedisCluster  # type: ignore[import-not-found]

ASYNC_CLUSTER_SUPPORTED_KWARGS = {
    "cache",
    "cache_config",
    "client_name",
    "credential_provider",
    "encoding",
    "encoding_errors",
    "event_dispatcher",
    "health_check_interval",
    "lib_name",
    "lib_version",
    "load_balancing_strategy",
    "max_connections",
    "path",
    "policy_resolver",
    "protocol",
    "socket_connect_timeout",
    "socket_keepalive",
    "socket_keepalive_options",
    "socket_timeout",
    "ssl_ca_certs",
    "ssl_ca_data",
    "ssl_cert_reqs",
    "ssl_certfile",
    "ssl_check_hostname",
    "ssl_ciphers",
    "ssl_exclude_verify_flags",
    "ssl_include_verify_flags",
    "ssl_keyfile",
    "ssl_min_version",
}

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


# detect if a connection is a cluster
def Is_Cluster(conn: redis.Redis):
    pool = conn.connection_pool
    kwargs = pool.connection_kwargs.copy()

    # Check if the connection is using SSL and add it
    # this property is not kept in the connection_kwargs
    kwargs["ssl"] = _is_ssl_connection(pool)

    # Create a synchronous Redis client with the same parameters
    # as the connection pool just to keep Is_Cluster synchronous
    probe = sync_redis.Redis(**_probe_kwargs(kwargs))
    try:
        info = cast(dict, probe.info(section="server"))
    finally:
        probe.close()
    return info.get("redis_mode") == "cluster"


# create a cluster connection from a Redis connection
def Cluster_Conn(
    conn,
    ssl,
    cluster_error_retry_attempts=3,
    startup_nodes=None,
    require_full_coverage=False,
    reinitialize_steps=5,
    read_from_replicas=False,
    dynamic_startup_nodes=True,
    url=None,
    address_remap=None,
):
    connection_kwargs = conn.connection_pool.connection_kwargs.copy()
    host = connection_kwargs.pop("host", None)
    port = connection_kwargs.pop("port", None)
    username = connection_kwargs.pop("username", None)
    password = connection_kwargs.pop("password", None)

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
    connection_kwargs.pop("connection_pool", None)
    connection_kwargs.pop("db", None)
    connection_kwargs.pop("decode_responses", None)

    cluster_kwargs = {
        key: value
        for key, value in connection_kwargs.items()
        if key in ASYNC_CLUSTER_SUPPORTED_KWARGS
    }
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
        read_from_replicas=read_from_replicas,
        dynamic_startup_nodes=dynamic_startup_nodes,
        address_remap=address_remap,
        startup_nodes=startup_nodes,
        cluster_error_retry_attempts=cluster_error_retry_attempts,
        **cluster_kwargs,
    )
