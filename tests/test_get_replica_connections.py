import pytest
import subprocess
from falkordb import FalkorDB
import time

def delete_container(container_name):
    subprocess.run(["docker","rm",container_name,'-f'],check=True,encoding='utf-8')

def reapply_compose(path):
    subprocess.run(f"docker-compose -f {path} down && docker-compose -f {path} up -d",check=True,shell=True,encoding='utf-8')

def cluster_client():
    client = FalkorDB(
        host='localhost',
        port=5000
    )
    return client


def standalone_client():
    client = FalkorDB(
        host='localhost',
        port=6379
    )
    return client

def sentinel_client():
    client = FalkorDB(
        host='localhost',
        port=26379
    )
    return client

def delete_replicas(client,mode):
    try:
        result = client.get_replica_connections()
    except Exception as e:
        return
    if mode == 'cluster':
        for i in result:
            name = i[0]
            try:
                delete_container(name)
            except Exception as e:
                print(e)
                return
    elif mode == 'sentinel':
        name = result[0][0]
        try:
            delete_container(name)
        except Exception as e:
            print(e)
            return
        

def test_get_replica_connections_cluster():
    c = cluster_client()
    result = c.get_replica_connections()
    assert ('node3',8000) in result
    assert ('node4',9000) in result
    assert ('node5',10000) in result
    


#
def test_get_replica_connections_standalone():
    c = standalone_client()
    with pytest.raises(ValueError, match="Unsupported Redis mode"):
        result = c.get_replica_connections()
    

#
def test_get_replica_connections_sentinel():
    c = sentinel_client()
    result = c.get_replica_connections()
    assert result == [('redis-sentinel2', 6381)]
    



def test_get_replica_connections_cluster_no_replicas():
    # Assume this cluster has no replicas configured
    delete_replicas(cluster_client(),'cluster')
    time.sleep(15)
    c = cluster_client()
    with pytest.raises(ConnectionError, match="Unable to get cluster nodes"):
        c.get_replica_connections()
    reapply_compose('/home/runner/work/falkordb-py/falkordb-py/cluster-compose')


def test_get_replica_connections_sentinel_no_replicas():
    # Assume this Sentinel setup has no replicas
    delete_replicas(sentinel_client(),'sentinel')
    time.sleep(40)
    c = sentinel_client()
    with pytest.raises(ConnectionError, match="Unable to get replica hostname."):
        c.get_replica_connections()
        
    reapply_compose('/home/runner/work/falkordb-py/falkordb-py/sentinel-compose')
