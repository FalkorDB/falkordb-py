from redis.asyncio.cluster import RedisCluster
import redis.exceptions as redis_exceptions
import redis.asyncio as redis
import redis as sync_redis
import socket


# detect if a connection is a cluster
def Is_Cluster(conn: redis.Redis):

    pool = conn.connection_pool
    kwargs = pool.connection_kwargs.copy()

    # Check if the connection is using SSL and add it
    # this propery is not kept in the connection_kwargs
    kwargs["ssl"] = pool.connection_class is redis.SSLConnection

    # Create a synchronous Redis client with the same parameters
    # as the connection pool just to keep Is_Cluster synchronous
    info = sync_redis.Redis(**kwargs).info(section="server")

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
):
    connection_kwargs = conn.connection_pool.connection_kwargs
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
        address_remap=address_remap,
        startup_nodes=startup_nodes,
        cluster_error_retry_attempts=cluster_error_retry_attempts,
    )
