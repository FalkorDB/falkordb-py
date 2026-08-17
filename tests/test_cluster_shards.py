from unittest.mock import MagicMock, patch

import pytest

from falkordb.asyncio.falkordb import FalkorDB as AsyncFalkorDB
from falkordb.cluster import parse_cluster_slots
from falkordb.falkordb import FalkorDB as SyncFalkorDB


def test_parse_cluster_slots():
    raw_slots = [
        [
            0,
            5460,
            [b"127.0.0.1", 6379, b"node_primary_1"],
            [b"127.0.0.1", 6380, b"node_replica_1"],
        ],
        [
            5461,
            10922,
            [b"127.0.0.1", 6381, b"node_primary_2"],
            [b"127.0.0.1", 6382, b"node_replica_2"],
        ],
    ]
    shards = parse_cluster_slots(raw_slots)
    assert len(shards) == 2

    shard1 = shards[0]
    assert shard1["primary"]["id"] == "node_primary_1"
    assert shard1["primary"]["host"] == "127.0.0.1"
    assert shard1["primary"]["port"] == 6379
    assert len(shard1["replicas"]) == 1
    assert shard1["replicas"][0]["id"] == "node_replica_1"
    assert shard1["replicas"][0]["port"] == 6380
    assert shard1["slots"] == [(0, 5460)]

    shard2 = shards[1]
    assert shard2["primary"]["id"] == "node_primary_2"
    assert shard2["primary"]["port"] == 6381
    assert shard2["replicas"][0]["port"] == 6382


def test_sync_get_cluster_shards():
    db = object.__new__(SyncFalkorDB)
    db.sentinel = None
    db.service_name = None
    mock_conn = MagicMock()
    raw_slots = [
        [0, 16383, [b"10.0.0.1", 6379, b"p1"], [b"10.0.0.2", 6379, b"r1"]],
    ]
    mock_conn.execute_command.return_value = raw_slots
    db.connection = mock_conn

    with patch("falkordb.falkordb.Is_Cluster", return_value=True):
        shards = db.get_cluster_shards()
        assert len(shards) == 1
        assert shards[0]["primary"]["host"] == "10.0.0.1"
        assert shards[0]["replicas"][0]["host"] == "10.0.0.2"


def test_sync_get_cluster_shards_standalone():
    db = object.__new__(SyncFalkorDB)
    db.sentinel = None
    db.service_name = None
    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {"host": "127.0.0.1", "port": 6379}
    db.connection = mock_conn

    with patch("falkordb.falkordb.Is_Cluster", return_value=False):
        shards = db.get_cluster_shards()
        assert len(shards) == 1
        assert shards[0]["primary"]["host"] == "127.0.0.1"
        assert shards[0]["primary"]["port"] == 6379


@pytest.mark.asyncio
async def test_async_get_cluster_shards():
    db = object.__new__(AsyncFalkorDB)
    db.sentinel = None
    db.service_name = None
    mock_conn = MagicMock()
    raw_slots = [
        [0, 16383, [b"10.0.0.1", 6379, b"p1"], [b"10.0.0.2", 6379, b"r1"]],
    ]

    async def async_exec(*args, **kwargs):
        return raw_slots

    mock_conn.execute_command = MagicMock(side_effect=async_exec)
    db.connection = mock_conn

    with patch("falkordb.asyncio.falkordb.Is_Cluster", return_value=True):
        shards = await db.get_cluster_shards()
        assert len(shards) == 1
        assert shards[0]["primary"]["host"] == "10.0.0.1"
        assert shards[0]["replicas"][0]["host"] == "10.0.0.2"


def test_sync_is_cluster_true():
    from falkordb.cluster import Is_Cluster

    mock_conn = MagicMock()
    mock_conn.info.return_value = {"redis_mode": "cluster"}
    assert Is_Cluster(mock_conn) is True


def test_sync_is_cluster_false():
    from falkordb.cluster import Is_Cluster

    mock_conn = MagicMock()
    mock_conn.info.return_value = {"redis_mode": "standalone"}
    assert Is_Cluster(mock_conn) is False


def test_sync_is_cluster_generic_exception():
    from falkordb.cluster import Is_Cluster

    mock_conn = MagicMock()
    mock_conn.info.side_effect = ValueError("unexpected error")
    assert Is_Cluster(mock_conn) is False


def test_sync_is_cluster_connection_error_raises():
    from falkordb.cluster import Is_Cluster

    mock_conn = MagicMock()
    mock_conn.info.side_effect = ConnectionError("failed connect")
    with pytest.raises(ConnectionError):
        Is_Cluster(mock_conn)


def test_async_is_cluster_true():
    from falkordb.asyncio.cluster import Is_Cluster

    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {"host": "localhost", "port": 6379}
    with patch("falkordb.asyncio.cluster.sync_redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value.info.return_value = {"redis_mode": "cluster"}
        assert Is_Cluster(mock_conn) is True


def test_async_is_cluster_false():
    from falkordb.asyncio.cluster import Is_Cluster

    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {"host": "localhost", "port": 6379}
    with patch("falkordb.asyncio.cluster.sync_redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value.info.return_value = {"redis_mode": "standalone"}
        assert Is_Cluster(mock_conn) is False


def test_async_is_cluster_generic_exception():
    from falkordb.asyncio.cluster import Is_Cluster

    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {"host": "localhost", "port": 6379}
    with patch("falkordb.asyncio.cluster.sync_redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value.info.side_effect = ValueError("error")
        assert Is_Cluster(mock_conn) is False


def test_sync_cluster_conn():
    from falkordb.cluster import Cluster_Conn

    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {
        "host": "localhost",
        "port": 6379,
        "username": "user",
        "password": "pass",
    }
    with patch("falkordb.cluster.RedisCluster") as mock_cluster_cls:
        Cluster_Conn(mock_conn, ssl=False, read_from_replicas=True)
        mock_cluster_cls.assert_called_once()


def test_async_cluster_conn():
    from falkordb.asyncio.cluster import Cluster_Conn

    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {
        "host": "localhost",
        "port": 6379,
        "username": "user",
        "password": "pass",
    }
    with patch("falkordb.asyncio.cluster.RedisCluster") as mock_cluster_cls:
        Cluster_Conn(mock_conn, ssl=False, read_from_replicas=True)
        mock_cluster_cls.assert_called_once()


def test_async_is_cluster_unix_socket():
    from redis.asyncio.connection import UnixDomainSocketConnection

    from falkordb.asyncio.cluster import Is_Cluster

    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = UnixDomainSocketConnection
    mock_conn.connection_pool.connection_kwargs = {"path": "/tmp/falkordb.sock"}
    with patch("falkordb.asyncio.cluster.sync_redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value.info.return_value = {"redis_mode": "cluster"}
        assert Is_Cluster(mock_conn) is True


def test_async_is_cluster_connection_error_raises():
    from falkordb.asyncio.cluster import Is_Cluster

    mock_conn = MagicMock()
    mock_conn.connection_pool.connection_class = MagicMock()
    mock_conn.connection_pool.connection_kwargs = {"host": "localhost", "port": 6379}
    with patch("falkordb.asyncio.cluster.sync_redis.Redis") as mock_redis_cls:
        mock_redis_cls.return_value.info.side_effect = ConnectionError("failed")
        with pytest.raises(ConnectionError):
            Is_Cluster(mock_conn)


def test_parse_cluster_slots_edge_cases():
    raw_slots = [
        [],  # Empty item
        [0, 5460],  # Item too short
        [
            0,
            5460,
            ["127.0.0.1", "6379", "node_primary_1"],  # String types
            [],  # Empty replica info
            ["127.0.0.1", 6380],  # Replica without ID
        ],
    ]
    shards = parse_cluster_slots(raw_slots)
    assert len(shards) == 1
    assert shards[0]["primary"]["id"] == "node_primary_1"
    assert shards[0]["replicas"][0]["id"] == "127.0.0.1:6380"
