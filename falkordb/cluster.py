from redis.cluster import RedisCluster

# detect if a connection is a sentinel
def Is_Cluster(conn):
    info = conn.info(section="server")
    return "redis_mode" in info and info["redis_mode"] == "cluster"

# create a cluster connection from a Redis connection
def Cluster_Conn(conn, ssl):
    connection_kwargs = conn.connection_pool.connection_kwargs
    host = connection_kwargs.pop("host")
    port = connection_kwargs.pop("port") 
    username = connection_kwargs.pop("username")
    password = connection_kwargs.pop("password")

    startup_nodes = connection_kwargs.pop("startup_nodes", None) 
    cluster_error_retry_attempts = connection_kwargs.pop("cluster_error_retry_attempts", 3) 
    retry = connection_kwargs.pop("retry", None)
    retry_on_timeout = connection_kwargs.pop("retry_on_timeout", None)
    retry_on_error = connection_kwargs.pop("retry_on_error", [
                        ConnectionError,
                        TimeoutError,
                        ConnectionRefusedError,
    ])
    require_full_coverage = connection_kwargs.pop("require_full_coverage", None)
    reinitialize_steps = connection_kwargs.pop("reinitialize_steps", None)
    read_from_replicas = connection_kwargs.pop("read_from_replicas", None) 
    dynamic_startup_nodes = connection_kwargs.pop("dynamic_startup_nodes", None) 
    url = connection_kwargs.pop("url", None) 
    address_remap = connection_kwargs.pop("address_remap", None)

    return RedisCluster(
        host=host, 
        port=port, 
        username=username, 
        password=password, 
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
    )
