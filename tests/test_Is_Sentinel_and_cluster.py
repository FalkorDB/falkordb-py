import pytest
import redis
from falkordb.falkordb import FalkorDB, Graph
from unittest.mock import patch, MagicMock, create_autospec
from falkordb.cluster import Is_Cluster
from falkordb.sentinel import Is_Sentinel

@pytest.fixture
def mock_redis():
    with patch('redis.Redis') as mock_redis:
        yield mock_redis

@pytest.fixture
def falkor_db(mock_redis):
    return FalkorDB()

def test_init(mock_redis):
    db = FalkorDB()
    assert db.connection is not None

def test_from_url(mock_redis):
    url = "falkor://localhost:6379"
    db = FalkorDB.from_url(url)
    assert db.connection is not None

def test_select_graph(falkor_db):
    # Test with valid graph_id
    graph_id = "social"
    graph = falkor_db.select_graph(graph_id)
    assert isinstance(graph, Graph)
    assert graph.name == graph_id

    # Test with invalid graph_id
    with pytest.raises(TypeError, match="Expected a string parameter"):
        falkor_db.select_graph(123)

    with pytest.raises(TypeError, match="Expected a string parameter"):
        falkor_db.select_graph("")

def test_list_graphs(falkor_db):
    falkor_db.connection.execute_command = MagicMock(return_value=['graph1', 'graph2'])
    graphs = falkor_db.list_graphs()
    assert graphs == ['graph1', 'graph2']

def test_flushdb(falkor_db):
    falkor_db.flushdb = MagicMock(return_value=True)
    result = falkor_db.flushdb()
    assert result is True
    falkor_db.flushdb.assert_called_once()

def test_execute_command(falkor_db):
    falkor_db.execute_command = MagicMock(return_value='OK')
    result = falkor_db.execute_command('PING')
    assert result == 'OK'
    falkor_db.execute_command.assert_called_with('PING')

def test_config_get(falkor_db):
    falkor_db.connection.execute_command = MagicMock(return_value=[None, 'value'])
    value = falkor_db.config_get('some_config')
    assert value == 'value'

    # Test error scenario
    falkor_db.connection.execute_command = MagicMock(side_effect=redis.RedisError("Config get failed"))
    with pytest.raises(redis.RedisError, match="Config get failed"):
        falkor_db.config_get('invalid_config')

def test_config_set(falkor_db):
    falkor_db.connection.execute_command = MagicMock(return_value='OK')
    result = falkor_db.config_set('some_config', 'new_value')
    assert result == 'OK'
    falkor_db.connection.execute_command.assert_called_with('GRAPH.CONFIG', 'SET', 'some_config', 'new_value')

    # Test error scenario
    falkor_db.connection.execute_command = MagicMock(side_effect=redis.RedisError("Config set failed"))
    with pytest.raises(redis.RedisError, match="Config set failed"):
        falkor_db.config_set('invalid_config', 'value')

def test_get_replica_connections_sentinel(falkor_db):
    falkor_db.sentinel = MagicMock()
    falkor_db.sentinel.discover_slaves = MagicMock(return_value=[('host1', '6379')])
    falkor_db.service_name = 'service'
    replicas = falkor_db.get_replica_connections()
    assert replicas == [('host1', 6379)]

    # Test sentinel with no replicas
    falkor_db.sentinel.discover_slaves = MagicMock(return_value=[])
    with pytest.raises(ConnectionError, match="Unable to get replica hostname"):
        falkor_db.get_replica_connections()

def test_get_replica_connections_cluster(falkor_db):
    falkor_db.connection.execute_command = MagicMock(return_value={'redis_mode': 'cluster'})
    falkor_db.connection.info = MagicMock(return_value={'redis_mode': 'cluster'})
    falkor_db.connection.cluster_nodes = MagicMock(return_value={
        '127.0.0.1:7001': {'flags': ['slave'], 'hostname': 'host1'},
        '127.0.0.1:7002': {'flags': ['master'], 'hostname': 'host2'}
    })
    replicas = falkor_db.get_replica_connections()
    assert replicas == [('host1', 7001)]

    # Test cluster with no nodes
    falkor_db.connection.cluster_nodes = MagicMock(return_value={})
    with pytest.raises(ConnectionError, match="Unable to get cluster nodes"):
        falkor_db.get_replica_connections()

def test_is_cluster():
    mock_redis = create_autospec(redis.Redis)
    mock_redis.info.return_value = {'redis_mode': 'cluster'}
    assert Is_Cluster(mock_redis) is True

    mock_redis.info.return_value = {'redis_mode': 'standalone'}
    assert Is_Cluster(mock_redis) is False

def test_is_sentinel():
    mock_redis = create_autospec(redis.Redis)
    mock_redis.info.return_value = {'redis_mode': 'sentinel'}
    assert Is_Sentinel(mock_redis) is True

    mock_redis.info.return_value = {'redis_mode': 'standalone'}
    assert Is_Sentinel(mock_redis) is False
