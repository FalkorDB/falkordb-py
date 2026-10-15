"""Connection-construction tests that do not need a live server."""

import inspect
import warnings
from typing import ClassVar

import pytest
import redis

import falkordb.asyncio.cluster as async_cluster
import falkordb.cluster as sync_cluster


class _RecordingCluster:
    """Stands in for redis.RedisCluster and records how it was constructed."""

    last_kwargs: ClassVar[dict] = {}

    def __init__(self, **kwargs):
        type(self).last_kwargs = kwargs


class _StubPool:
    def __init__(self, **connection_kwargs):
        self.connection_kwargs = connection_kwargs
        self.connection_class = redis.Connection


class _StubConn:
    def __init__(self, **connection_kwargs):
        connection_kwargs.setdefault("host", "localhost")
        connection_kwargs.setdefault("port", 6379)
        connection_kwargs.setdefault("username", None)
        connection_kwargs.setdefault("password", None)
        self.connection_pool = _StubPool(**connection_kwargs)


def test_cluster_conn_does_not_mutate_caller_pool(monkeypatch):
    monkeypatch.setattr(sync_cluster, "RedisCluster", _RecordingCluster)
    conn = _StubConn(username="user", password="secret")

    sync_cluster.Cluster_Conn(conn, ssl=False)

    # the caller's pool must still be able to authenticate
    assert conn.connection_pool.connection_kwargs["username"] == "user"
    assert conn.connection_pool.connection_kwargs["password"] == "secret"
    assert conn.connection_pool.connection_kwargs["host"] == "localhost"


def test_cluster_conn_omits_deprecated_defaults(monkeypatch):
    monkeypatch.setattr(sync_cluster, "RedisCluster", _RecordingCluster)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        sync_cluster.Cluster_Conn(_StubConn(), ssl=False)

    kwargs = _RecordingCluster.last_kwargs
    assert "cluster_error_retry_attempts" not in kwargs
    assert "read_from_replicas" not in kwargs
    assert "retry_on_timeout" not in kwargs


def test_cluster_conn_forwards_non_default_values(monkeypatch):
    monkeypatch.setattr(sync_cluster, "RedisCluster", _RecordingCluster)

    sync_cluster.Cluster_Conn(
        _StubConn(retry_on_timeout=True),
        ssl=False,
        cluster_error_retry_attempts=7,
        read_from_replicas=True,
        load_balancing_strategy="round_robin",
    )

    kwargs = _RecordingCluster.last_kwargs
    assert kwargs["cluster_error_retry_attempts"] == 7
    assert kwargs["read_from_replicas"] is True
    assert kwargs["retry_on_timeout"] is True
    assert kwargs["load_balancing_strategy"] == "round_robin"


def test_cluster_conn_passes_ssl_through(monkeypatch):
    monkeypatch.setattr(sync_cluster, "RedisCluster", _RecordingCluster)

    sync_cluster.Cluster_Conn(_StubConn(), ssl=True)

    assert _RecordingCluster.last_kwargs["ssl"] is True


def test_async_cluster_conn_does_not_mutate_caller_pool(monkeypatch):
    monkeypatch.setattr(async_cluster, "RedisCluster", _RecordingCluster)
    conn = _StubConn(username="user", password="secret")

    async_cluster.Cluster_Conn(conn, ssl=False)

    assert conn.connection_pool.connection_kwargs["username"] == "user"
    assert conn.connection_pool.connection_kwargs["password"] == "secret"


def test_async_cluster_conn_omits_deprecated_defaults(monkeypatch):
    monkeypatch.setattr(async_cluster, "RedisCluster", _RecordingCluster)

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        async_cluster.Cluster_Conn(_StubConn(), ssl=False)

    kwargs = _RecordingCluster.last_kwargs
    assert "cluster_error_retry_attempts" not in kwargs
    assert "read_from_replicas" not in kwargs


class _ClosingProbe:
    """Records the kwargs it was built with and whether it was closed."""

    instances: ClassVar[list] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False
        type(self).instances.append(self)

    def info(self, section=None):
        raise RuntimeError("probe failed")

    def close(self):
        self.closed = True


# Is_Cluster filters the pool kwargs against the signature of whatever it
# finds at sync_redis.Redis. A bare **kwargs stub accepts no named parameter,
# so every kwarg would be filtered out and the assertions below would hold no
# matter what Is_Cluster did. Borrow the real signature instead.
_ClosingProbe.__init__.__signature__ = inspect.signature(redis.Redis.__init__)


def test_async_is_cluster_closes_probe_on_failure(monkeypatch):
    _ClosingProbe.instances = []
    monkeypatch.setattr(async_cluster.sync_redis, "Redis", _ClosingProbe)

    conn = _StubConn(
        retry="retry-object",
        credential_provider="creds",
        redis_connect_func="connect",
    )

    with pytest.raises(RuntimeError):
        async_cluster.Is_Cluster(conn)

    probe = _ClosingProbe.instances[-1]
    assert probe.closed, "probe client leaked a connection"

    # the probe is synchronous and cannot drive asyncio-specific machinery
    assert "retry" not in probe.kwargs
    assert "redis_connect_func" not in probe.kwargs

    # it does still have to reach the server, so the connection details and
    # the credentials must survive. username/password are None whenever a
    # credential provider is in use, dropping it would leave the probe
    # unauthenticated and Is_Cluster would fail for those callers
    assert probe.kwargs["credential_provider"] == "creds"
    assert probe.kwargs["host"] == "localhost"
    assert probe.kwargs["port"] == 6379


def test_async_is_cluster_detects_cluster_mode(monkeypatch):
    class _Probe(_ClosingProbe):
        def info(self, section=None):
            return {"redis_mode": "cluster"}

    _Probe.instances = []
    monkeypatch.setattr(async_cluster.sync_redis, "Redis", _Probe)

    assert async_cluster.Is_Cluster(_StubConn()) is True
    assert _Probe.instances[-1].closed
