from redis.cluster import RedisCluster

# detect if a connection is a sentinel
def Is_Cluster(conn):
    info = conn.info(section="server")
    return "redis_mode" in info and info["redis_mode"] == "cluster"

# create a cluster connection from a Redis connection
def Cluster_Conn(conn, ssl):
    # current sentinel
    connection_kwargs = conn.connection_pool.connection_kwargs
    host = connection_kwargs['host']
    port = connection_kwargs['port']
    username = connection_kwargs['username']
    password = connection_kwargs['password']

    return RedisCluster(host=host, port=port, username=username, password=password, ssl=ssl)

