import inspect
import socket

import redis as sync_redis  # type: ignore[import-not-found]
import redis.asyncio as redis  # type: ignore[import-not-found]
import redis.exceptions as redis_exceptions  # type: ignore[import-not-found]
from redis.asyncio.cluster import RedisCluster  # type: ignore[import-not-found]


# detect if a connection is a cluster
def Is_Cluster(conn: redis.Redis):
    try:
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

        # Keep only the parameters the synchronous constructor actually accepts.
        # redis-py stores internal state in ``connection_kwargs`` that is not part
        # of the ``Redis.__init__`` signature — redis 8.1.0 added ``himport_registry``
        # there — and forwarding those raises TypeError before any I/O happens.
        accepted = inspect.signature(sync_redis.Redis.__init__).parameters
        kwargs = {k: v for k, v in kwargs.items() if k in accepted}

        # Create a synchronous Redis client with the same parameters
        # as the connection pool just to keep Is_Cluster synchronous
        info = sync_redis.Redis(**kwargs).info(section="server")

        return "redis_mode" in info and info["redis_mode"] == "cluster"
    except Exception:
        return False


def _str_val(v):
    if isinstance(v, bytes):
        return v.decode("utf-8")
    return str(v)


def parse_cluster_slots(raw_slots):
    """
    Parses CLUSTER SLOTS output into a list of primary and replica shard mappings.
    """
    shards_map = {}

    for item in raw_slots:
        if not item or len(item) < 3:
            continue
        start_slot = int(item[0])
        end_slot = int(item[1])

        p_info = item[2]
        p_host = _str_val(p_info[0])
        p_port = int(p_info[1])
        p_id = _str_val(p_info[2]) if len(p_info) > 2 else f"{p_host}:{p_port}"

        primary_key = (p_host, p_port)
        if primary_key not in shards_map:
            replicas = []
            for r_info in item[3:]:
                if not r_info or len(r_info) < 2:
                    continue
                r_host = _str_val(r_info[0])
                r_port = int(r_info[1])
                r_id = _str_val(r_info[2]) if len(r_info) > 2 else f"{r_host}:{r_port}"
                replicas.append({
                    "id": r_id,
                    "host": r_host,
                    "port": r_port,
                    "endpoint": f"{r_host}:{r_port}",
                })

            shards_map[primary_key] = {
                "primary": {
                    "id": p_id,
                    "host": p_host,
                    "port": p_port,
                    "endpoint": f"{p_host}:{p_port}",
                },
                "replicas": replicas,
                "slots": [(start_slot, end_slot)],
            }
        else:
            shards_map[primary_key]["slots"].append((start_slot, end_slot))

    return list(shards_map.values())


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
