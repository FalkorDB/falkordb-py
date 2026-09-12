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

    # redis-py deprecated these and warns for every one it receives, only
    # forward them when the caller actually diverged from the default
    optional: dict = {}
    if cluster_error_retry_attempts != 3:
        optional["cluster_error_retry_attempts"] = cluster_error_retry_attempts
    if read_from_replicas:
        optional["read_from_replicas"] = read_from_replicas
    if retry_on_timeout is not None:
        optional["retry_on_timeout"] = retry_on_timeout
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
        dynamic_startup_nodes=dynamic_startup_nodes,
        url=url,
        address_remap=address_remap,
        startup_nodes=startup_nodes,
        **optional,
    )
