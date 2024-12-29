import time
import pytest
import subprocess
from falkordb import FalkorDB

CLUSTER_PORT    = 5000
STANDALONE_PORT = 6379
SENTINEL_PORT   = 26379

# check=True is used to raise an exception if the command fails
def delete_container(container_name):
    subprocess.run(f"docker rm {container_name} -f", shell=True, check=True)

# shell=True is used to run the command in a shell
# encoding='utf-8' is used to get the output as a string
def reapply_compose(path):
    subprocess.run(f"docker-compose -f {path} down && docker-compose -f {path} up -d", check=True, shell=True)

def cluster_client():
    return FalkorDB(host='localhost', port=CLUSTER_PORT)

def standalone_client():
    return FalkorDB(host='localhost', port=STANDALONE_PORT)

def sentinel_client():
    return FalkorDB(host='localhost', port=SENTINEL_PORT)

def delete_replicas(client):
    result = client.get_replica_connections()
    for i in result:
        name = i[0]
        delete_container(name)

def test_get_replica_connections_cluster():
    c = cluster_client()
    result = c.get_replica_connections()
    assert ('node3', 8000) in result
    assert ('node4', 9000) in result
    assert ('node5', 10000) in result

def test_get_replica_connections_standalone():
    c = standalone_client()
    with pytest.raises(ValueError, match="Unsupported Redis mode"):
        result = c.get_replica_connections()

def test_get_replica_connections_sentinel():
    c = sentinel_client()
    result = c.get_replica_connections()
    assert result == [('redis-server-2', 6381)]

def test_get_replica_connections_cluster_no_replicas():
    # assume this cluster has no replicas configured
    delete_replicas(cluster_client())
    time.sleep(15)
    c = cluster_client()
    with pytest.raises(ConnectionError, match="Unable to get cluster nodes"):
        c.get_replica_connections()
    reapply_compose('/home/runner/work/falkordb-py/falkordb-py/docker/cluster-compose.yml')


def test_get_replica_connections_sentinel_no_replicas():
    # Assume this Sentinel setup has no replicas
    delete_replicas(sentinel_client())
    time.sleep(40)
    c = sentinel_client()
    with pytest.raises(ConnectionError, match="Unable to get replica hostname."):
        c.get_replica_connections()
        
    reapply_compose('/home/runner/work/falkordb-py/falkordb-py/docker/sentinel-compose.yml')

