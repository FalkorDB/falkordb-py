import pytest
from unittest.mock import MagicMock, patch
from redis.sentinel import Sentinel
from falkordb.falkordb import Sentinel_Conn
from falkordb.falkordb import Cluster_Conn
from redis.cluster import RedisCluster




def test_sentinel_conn_with_single_master():
    # Mock the Redis connection
    mock_redis_connection = MagicMock()
    mock_redis_connection.sentinel_masters.return_value = {"mymaster": {"ip": "127.0.0.1", "port": 26379}}
    mock_redis_connection.connection_pool.connection_kwargs = {
        "host": "127.0.0.1",
        "port": 26379,
        "username": "user",
        "password": "password",
    }

    ssl = True
    sentinel, service_name = Sentinel_Conn(mock_redis_connection, ssl)

    # Verify the service name
    assert service_name == "mymaster"

    # Verify that the returned Sentinel object has correct arguments
    assert isinstance(sentinel, Sentinel)
    assert sentinel.sentinels == [("127.0.0.1", 26379)]

    # Verify SSL and authentication
    sentinel_kwargs = sentinel.sentinel_kwargs
    assert sentinel_kwargs["username"] == "user"
    assert sentinel_kwargs["password"] == "password"
    assert sentinel_kwargs["ssl"] is True

    mock_redis_connection.sentinel_masters.assert_called_once()

def test_sentinel_conn_with_multiple_masters():
    mock_redis_connection = MagicMock()
    mock_redis_connection.sentinel_masters.return_value = {
        "master1": {},
        "master2": {},
    }
    with pytest.raises(Exception, match="Multiple masters, require service name"):
        Sentinel_Conn(mock_redis_connection, ssl=False)

from your_module import Cluster_Conn
from redis.cluster import RedisCluster

def test_cluster_conn():
    # Mock the Redis connection
    mock_redis_connection = MagicMock()
    mock_redis_connection.connection_pool.connection_kwargs = {
        "host": "127.0.0.1",
        "port": 6379,
        "username": "user",
        "password": "password",
    }

    ssl = False
    cluster_error_retry_attempts = 3
    startup_nodes = [{"host": "127.0.0.1", "port": 6379}]
    require_full_coverage = True
    reinitialize_steps = 10
    read_from_replicas = True
    dynamic_startup_nodes = False
    url = None
    address_remap = None

    cluster_conn = Cluster_Conn(
        mock_redis_connection,
        ssl,
        cluster_error_retry_attempts,
        startup_nodes,
        require_full_coverage,
        reinitialize_steps,
        read_from_replicas,
        dynamic_startup_nodes,
        url,
        address_remap,
    )

    # Verify the returned object
    assert isinstance(cluster_conn, RedisCluster)

    # Verify the connection parameters
    assert cluster_conn.connection_kwargs["host"] == "127.0.0.1"
    assert cluster_conn.connection_kwargs["port"] == 6379
    assert cluster_conn.connection_kwargs["username"] == "user"
    assert cluster_conn.connection_kwargs["password"] == "password"
    assert cluster_conn.connection_kwargs["require_full_coverage"] is True
    assert cluster_conn.connection_kwargs["read_from_replicas"] is True

def test_sentinel_and_cluster_combination():
    # Mock Redis connection for Sentinel
    mock_sentinel_conn = MagicMock()
    mock_sentinel_conn.sentinel_masters.return_value = {"mymaster": {"ip": "127.0.0.1", "port": 26379}}
    mock_sentinel_conn.connection_pool.connection_kwargs = {"host": "127.0.0.1", "port": 26379}

    # Create Sentinel connection
    sentinel, service_name = Sentinel_Conn(mock_sentinel_conn, ssl=True)
    master_conn = sentinel.master_for(service_name, ssl=True)

    # Verify Sentinel connection
    assert service_name == "mymaster"
    assert isinstance(master_conn, MagicMock)  # Replace with your expected object type

    # Mock Redis connection for Cluster
    mock_cluster_conn = MagicMock()
    mock_cluster_conn.connection_pool.connection_kwargs = {"host": "127.0.0.1", "port": 6379}

    # Create Cluster connection
    cluster_conn = Cluster_Conn(mock_cluster_conn, ssl=False, startup_nodes=[{"host": "127.0.0.1", "port": 6379}])

    # Verify Cluster connection
    assert isinstance(cluster_conn, RedisCluster)
