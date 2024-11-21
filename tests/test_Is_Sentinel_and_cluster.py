import pytest
from unittest.mock import MagicMock
from falkordb.falkordb import Is_Sentinel
from falkordb.falkordb import Is_Cluster

def test_is_sentinel_with_sentinel_connection():
    # Mock the Redis connection
    mock_redis_connection = MagicMock()
    mock_redis_connection.info.return_value = {"redis_mode": "sentinel"}
    
    # Assert that Is_Sentinel detects a Sentinel connection
    assert Is_Sentinel(mock_redis_connection) is True
    mock_redis_connection.info.assert_called_once_with(section="server")

def test_is_sentinel_with_non_sentinel_connection():
    # Mock the Redis connection
    mock_redis_connection = MagicMock()
    mock_redis_connection.info.return_value = {"redis_mode": "standalone"}
    
    # Assert that Is_Sentinel does not detect a Sentinel connection
    assert Is_Sentinel(mock_redis_connection) is False
    mock_redis_connection.info.assert_called_once_with(section="server")

def test_is_cluster_with_cluster_connection():
    # Mock the Redis connection
    mock_redis_connection = MagicMock()
    mock_redis_connection.info.return_value = {"redis_mode": "cluster"}
    
    # Assert that Is_Cluster detects a Cluster connection
    assert Is_Cluster(mock_redis_connection) is True
    mock_redis_connection.info.assert_called_once_with(section="server")

def test_is_cluster_with_non_cluster_connection():
    # Mock the Redis connection
    mock_redis_connection = MagicMock()
    mock_redis_connection.info.return_value = {"redis_mode": "standalone"}
    
    # Assert that Is_Cluster does not detect a Cluster connection
    assert Is_Cluster(mock_redis_connection) is False
    mock_redis_connection.info.assert_called_once_with(section="server")
