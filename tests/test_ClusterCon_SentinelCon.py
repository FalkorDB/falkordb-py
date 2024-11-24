import socket
import pytest
from unittest.mock import MagicMock, patch
from falkordb.sentinel import Sentinel_Conn
from falkordb.cluster import Cluster_Conn
import redis

def test_sentinel_conn():
    # Mock a Redis connection
    mock_conn = MagicMock()
    mock_conn.info.return_value = {"redis_mode": "sentinel"}
    mock_conn.sentinel_masters.return_value = {"mymaster": {}}
    mock_conn.connection_pool.connection_kwargs = {
        'host': 'localhost',
        'port': 26379,
        'username': 'user',
        'password': 'pass'
    }

    # Test Sentinel_Conn
    with patch('falkordb.sentinel.Sentinel') as MockSentinel:
        mock_sentinel_instance = MockSentinel.return_value
        mock_sentinel_instance.sentinels = [('localhost', 26379)]
        
        sentinel, service_name = Sentinel_Conn(mock_conn, ssl=True)
        assert service_name == "mymaster"
        assert sentinel.sentinels == [('localhost', 26379)]

def test_cluster_conn():
    # Mock a Redis connection
    mock_conn = MagicMock()
    mock_conn.info.return_value = {"redis_mode": "cluster"}
    mock_conn.connection_pool.connection_kwargs = {
        'host': 'localhost',
        'port': 6379,
        'username': 'user',
        'password': 'pass'
    }

    # Test Cluster_Conn
    with patch('falkordb.cluster.RedisCluster') as MockRedisCluster:
        cluster_conn = Cluster_Conn(mock_conn, ssl=True)
        MockRedisCluster.assert_called_once_with(
            host='localhost',
            port=6379,
            username='user',
            password='pass',
            ssl=True,
            retry=None,
            retry_on_timeout=None,
            retry_on_error=[
                ConnectionRefusedError,
                ConnectionError,
                TimeoutError,
                socket.timeout,
                redis.exceptions.ConnectionError,
            ],
            require_full_coverage=False,
            reinitialize_steps=5,
            read_from_replicas=False,
            dynamic_startup_nodes=True,
            url=None,
            address_remap=None,
            startup_nodes=None,
            cluster_error_retry_attempts=3,
        )
