from redis.cluster import RedisCluster, ClusterNode
from redis.retry import Retry
from redis.backoff import default_backoff

# detect if a connection is a sentinel
def Is_Cluster(conn):
    info = conn.info(section="server")
    return "redis_mode" in info and info["redis_mode"] == "cluster"

# create a cluster connection from a Redis connection
def Cluster_Conn(conn, ssl):
    info = conn.execute_command("CLUSTER NODES")
    nodes = [ ClusterNode(v['hostname'],k.split(':')[1]) for k,v in info.items()]
    connection_kwargs = conn.connection_pool.connection_kwargs
    username = connection_kwargs['username']
    password = connection_kwargs['password']
    return RedisCluster(Retry(default_backoff(),20),cluster_error_retry_attempts=6,startup_nodes=nodes,username=username,password=password,ssl=ssl)

