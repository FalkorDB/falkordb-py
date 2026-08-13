from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from falkordb.asyncio.falkordb import FalkorDB as AsyncFalkorDB
from falkordb.falkordb import FalkorDB as SyncFalkorDB


def test_sync_standalone_get_replica_connection():
    db = object.__new__(SyncFalkorDB)
    db.sentinel = None
    db.service_name = None
    mock_conn = MagicMock()
    db.connection = mock_conn

    with patch("falkordb.falkordb.Is_Cluster", return_value=False):
        replica_conn = db.get_replica_connection()
        assert replica_conn is mock_conn


def test_sync_sentinel_get_replica_connection():
    db = object.__new__(SyncFalkorDB)
    mock_conn = MagicMock()
    mock_sentinel = MagicMock()
    mock_replica_conn = MagicMock()
    mock_sentinel.replica_for.return_value = mock_replica_conn
    mock_sentinel.slave_for.return_value = mock_replica_conn

    db.connection = mock_conn
    db.sentinel = mock_sentinel
    db.service_name = "mymaster"

    replica_conn = db.get_replica_connection()
    assert replica_conn is mock_replica_conn


def test_sync_sentinel_read_from_replicas_init():
    mock_redis = MagicMock()
    mock_sentinel = MagicMock()
    mock_slave = MagicMock()
    mock_sentinel.slave_for.return_value = mock_slave

    with (
        patch("falkordb.falkordb.redis.Redis", return_value=mock_redis),
        patch("falkordb.falkordb.Is_Sentinel", return_value=True),
        patch(
            "falkordb.falkordb.Sentinel_Conn", return_value=(mock_sentinel, "mymaster")
        ),
        patch("falkordb.falkordb.Is_Cluster", return_value=False),
    ):
        db = SyncFalkorDB(read_from_replicas=True)
        assert db.connection is mock_slave
        mock_sentinel.slave_for.assert_called_once_with("mymaster", ssl=False)


def test_sync_cluster_get_replica_connection():
    db = object.__new__(SyncFalkorDB)
    db.sentinel = None
    db.service_name = None
    mock_conn = SimpleNamespace(
        connection_pool=SimpleNamespace(
            connection_kwargs={
                "host": "127.0.0.1",
                "port": 6379,
                "username": None,
                "password": None,
            }
        )
    )
    db.connection = mock_conn

    with (
        patch("falkordb.falkordb.Is_Cluster", return_value=True),
        patch("falkordb.falkordb.Cluster_Conn") as mock_cluster_conn,
    ):
        db.get_replica_connection()
        mock_cluster_conn.assert_called_once_with(
            mock_conn, ssl=False, read_from_replicas=True
        )


def test_async_standalone_get_replica_connection():
    db = object.__new__(AsyncFalkorDB)
    db.sentinel = None
    db.service_name = None
    mock_conn = MagicMock()
    db.connection = mock_conn

    with patch("falkordb.asyncio.falkordb.Is_Cluster", return_value=False):
        replica_conn = db.get_replica_connection()
        assert replica_conn is mock_conn


def test_async_sentinel_get_replica_connection():
    db = object.__new__(AsyncFalkorDB)
    mock_conn = MagicMock()
    mock_sentinel = MagicMock()
    mock_replica_conn = MagicMock()
    mock_sentinel.replica_for.return_value = mock_replica_conn
    mock_sentinel.slave_for.return_value = mock_replica_conn

    db.connection = mock_conn
    db.sentinel = mock_sentinel
    db.service_name = "mymaster"

    replica_conn = db.get_replica_connection()
    assert replica_conn is mock_replica_conn


def test_async_sentinel_read_from_replicas_init():
    mock_redis = MagicMock()
    mock_sentinel = MagicMock()
    mock_slave = MagicMock()
    mock_sentinel.slave_for.return_value = mock_slave

    with (
        patch("falkordb.asyncio.falkordb.redis.Redis", return_value=mock_redis),
        patch("falkordb.asyncio.falkordb.Is_Sentinel", return_value=True),
        patch(
            "falkordb.asyncio.falkordb.Sentinel_Conn",
            return_value=(mock_sentinel, "mymaster"),
        ),
        patch("falkordb.asyncio.falkordb.Is_Cluster", return_value=False),
    ):
        db = AsyncFalkorDB(read_from_replicas=True)
        assert db.connection is mock_slave
        mock_sentinel.slave_for.assert_called_once_with("mymaster", ssl=False)


def test_async_cluster_get_replica_connection():
    db = object.__new__(AsyncFalkorDB)
    db.sentinel = None
    db.service_name = None
    mock_conn = SimpleNamespace(
        connection_pool=SimpleNamespace(
            connection_kwargs={
                "host": "127.0.0.1",
                "port": 6379,
                "username": None,
                "password": None,
            }
        )
    )
    db.connection = mock_conn

    with (
        patch("falkordb.asyncio.falkordb.Is_Cluster", return_value=True),
        patch("falkordb.asyncio.falkordb.Cluster_Conn") as mock_cluster_conn,
    ):
        db.get_replica_connection()
        mock_cluster_conn.assert_called_once_with(
            mock_conn, ssl=False, read_from_replicas=True
        )


def test_sync_existing_redis_cluster_get_replica_connection():
    db = object.__new__(SyncFalkorDB)
    db.sentinel = None
    db.service_name = None
    db._raw_conn = MagicMock()
    db._ssl = False
    db._replica_connection = None

    cluster_primary = MagicMock(spec=["read_from_replicas"])
    cluster_primary.read_from_replicas = False

    cluster_replica = MagicMock()

    db.connection = cluster_primary

    with (
        patch("falkordb.falkordb.Is_Cluster", return_value=True),
        patch("falkordb.falkordb.isinstance", side_effect=lambda obj, cls: True),
        patch(
            "falkordb.falkordb.Cluster_Conn", return_value=cluster_replica
        ) as mock_cluster_conn,
    ):
        replica_conn = db.get_replica_connection()
        assert replica_conn is cluster_replica
        mock_cluster_conn.assert_called_once_with(
            db._raw_conn, ssl=False, read_from_replicas=True
        )


def test_async_existing_redis_cluster_get_replica_connection():
    db = object.__new__(AsyncFalkorDB)
    db.sentinel = None
    db.service_name = None
    db._raw_conn = MagicMock()
    db._ssl = False
    db._replica_connection = None

    cluster_primary = MagicMock(spec=["read_from_replicas"])
    cluster_primary.read_from_replicas = False

    cluster_replica = MagicMock()

    db.connection = cluster_primary

    with (
        patch("falkordb.asyncio.falkordb.Is_Cluster", return_value=True),
        patch(
            "falkordb.asyncio.falkordb.isinstance", side_effect=lambda obj, cls: True
        ),
        patch(
            "falkordb.asyncio.falkordb.Cluster_Conn", return_value=cluster_replica
        ) as mock_cluster_conn,
    ):
        replica_conn = db.get_replica_connection()
        assert replica_conn is cluster_replica
        mock_cluster_conn.assert_called_once_with(
            db._raw_conn, ssl=False, read_from_replicas=True
        )
