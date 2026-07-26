import socket

import redis.exceptions as redis_exceptions  # type: ignore[import-not-found]
from redis.cluster import RedisCluster  # type: ignore[import-not-found]


# detect if a connection is a Cluster
def Is_Cluster(conn):
    info = conn.info(section="server")
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
    retry_on_timeout = connection_kwargs.pop("retry_on_timeout", None)
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
    return RedisCluster(
        host=host,
        port=port,
        username=username,
        password=password,
        decode_responses=True,
        ssl=ssl,
        retry=retry,
        retry_on_timeout=retry_on_timeout,
        retry_on_error=retry_on_error,
        require_full_coverage=require_full_coverage,
        reinitialize_steps=reinitialize_steps,
        read_from_replicas=read_from_replicas,
        dynamic_startup_nodes=dynamic_startup_nodes,
        url=url,
        address_remap=address_remap,
        startup_nodes=startup_nodes,
        cluster_error_retry_attempts=cluster_error_retry_attempts,
        **connection_kwargs,
    )
