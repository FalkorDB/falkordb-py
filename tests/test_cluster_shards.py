from unittest.mock import MagicMock, patch
import pytest

from falkordb.falkordb import FalkorDB as SyncFalkorDB
from falkordb.asyncio.falkordb import FalkorDB as AsyncFalkorDB
from falkordb.cluster import parse_cluster_slots


def test_parse_cluster_slots():
    raw_slots = [
        [0, 5460, [b"127.0.0.1", 6379, b"node_primary_1"], [b"127.0.0.1", 6380, b"node_replica_1"]],
        [5461, 10922, [b"127.0.0.1", 6381, b"node_primary_2"], [b"127.0.0.1", 6382, b"node_replica_2"]],
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
