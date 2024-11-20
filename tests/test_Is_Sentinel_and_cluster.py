import pytest
from unittest.mock import MagicMock
from falkordb.sentinel import Is_Sentinel
from falkordb.cluster import Is_Cluster
import redis

@pytest.fixture
def mock_redis_connection():
    """
    Fixture to provide a mock Redis connection.
    """
    return MagicMock(spec=redis.Redis)

def test_is_sentinel_with_sentinel_connection(mock_redis_connection):
    """
    Test `Is_Sentinel` function with a Sentinel connection.
    """
    # Mock the response for Sentinel command
    mock_redis_connection.execute_command.return_value = [
        {"name": "mymaster", "ip": "127.0.0.1", "port": 6379, "flags": "master"}
    ]
    
    assert Is_Sentinel(mock_redis_connection) is True
    mock_redis_connection.execute_command.assert_called_once_with("SENTINEL", "masters")

def test_is_sentinel_with_non_sentinel_connection(mock_redis_connection):
    """
    Test `Is_Sentinel` function with a non-Sentinel connection.
    """
    # Simulate an error or empty response
    mock_redis_connection.execute_command.side_effect = redis.exceptions.ResponseError("ERR unknown command 'SENTINEL'")
    
    assert Is_Sentinel(mock_redis_connection) is False
    mock_redis_connection.execute_command.assert_called_once_with("SENTINEL", "masters")

def test_is_cluster_with_cluster_connection(mock_redis_connection):
    """
    Test `Is_Cluster` function with a cluster connection.
    """
    # Mock the response for cluster info
    mock_redis_connection.execute_command.return_value = "cluster_state:ok\n"

    assert Is_Cluster(mock_redis_connection) is True
    mock_redis_connection.execute_command.assert_called_once_with("CLUSTER", "INFO")

def test_is_cluster_with_non_cluster_connection(mock_redis_connection):
    """
    Test `Is_Cluster` function with a non-cluster connection.
    """
    # Mock the response for non-cluster setup
    mock_redis_connection.execute_command.side_effect = redis.exceptions.ResponseError("ERR unknown command 'CLUSTER'")

    assert Is_Cluster(mock_redis_connection) is False
    mock_redis_connection.execute_command.assert_called_once_with("CLUSTER", "INFO")
