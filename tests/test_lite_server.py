import subprocess

import pytest
import redis

from falkordb.lite.server import EmbeddedServer, EmbeddedServerError


class DummyProcess:
    def __init__(self, poll_result=None, wait_raises=False):
        self._poll_result = poll_result
        self.returncode = 7 if poll_result is not None else None
        self.wait_raises = wait_raises
        self._wait_raise_count = 0
        self.terminated = False
        self.killed = False
        self.wait_called = False

    def poll(self):
        return self._poll_result

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        self.wait_called = True
        if self.wait_raises and self._wait_raise_count == 0:
            self._wait_raise_count += 1
            raise subprocess.TimeoutExpired("redis-server", timeout)

    def kill(self):
        self.killed = True


class DummyConn:
    def __init__(self, shutdown_exc=None):
        self.shutdown_exc = shutdown_exc
        self.closed = False

    def shutdown(self, nosave=False):
        if self.shutdown_exc is not None:
            raise self.shutdown_exc

    def close(self):
        self.closed = True


def _make_server(tmp_path):
    server = EmbeddedServer.__new__(EmbeddedServer)
    server._process = None
    server._stderr_file = None
    server._db_path = None
    server._startup_timeout = 0.01
    server._tmpdir = str(tmp_path)
    server._socket_path = str(tmp_path / "falkordb.sock")
    server._config_path = str(tmp_path / "redis.conf")
    server._stderr_path = str(tmp_path / "redis.stderr.log")
    return server


def test_start_raises_with_stderr_content(monkeypatch, tmp_path):
    server = _make_server(tmp_path)

    monkeypatch.setattr("falkordb.lite.server.subprocess.Popen", lambda *args, **kwargs: DummyProcess(poll_result=1))
    monkeypatch.setattr(server, "_read_stderr", lambda: "boom")

    with pytest.raises(EmbeddedServerError, match="boom"):
        server._start(tmp_path / "redis-server")


def test_stop_ignores_connection_error_on_shutdown(monkeypatch, tmp_path):
    server = _make_server(tmp_path)
    server._process = DummyProcess(poll_result=None)
    conn = DummyConn(shutdown_exc=redis.exceptions.ConnectionError("expected"))

    monkeypatch.setattr("falkordb.lite.server.redis.Redis", lambda *args, **kwargs: conn)

    server.stop()
    assert conn.closed is True
    assert server._process is None


def test_stop_terminates_on_unexpected_shutdown_error(monkeypatch, tmp_path):
    server = _make_server(tmp_path)
    process = DummyProcess(poll_result=None)
    server._process = process
    conn = DummyConn(shutdown_exc=RuntimeError("unexpected"))

    monkeypatch.setattr("falkordb.lite.server.redis.Redis", lambda *args, **kwargs: conn)

    server.stop()
    assert process.terminated is True
    assert conn.closed is True
    assert server._process is None


def test_stop_kills_process_on_wait_timeout(monkeypatch, tmp_path):
    server = _make_server(tmp_path)
    process = DummyProcess(poll_result=None, wait_raises=True)
    server._process = process
    conn = DummyConn()

    monkeypatch.setattr("falkordb.lite.server.redis.Redis", lambda *args, **kwargs: conn)

    server.stop()
    assert process.killed is True
    assert process.wait_called is True


def test_read_stderr_flushes_open_file(tmp_path):
    server = _make_server(tmp_path)
    stderr_file = open(tmp_path / "redis.stderr.log", "w", encoding="utf-8")
    stderr_file.write("stderr-text")
    server._stderr_file = stderr_file

    output = server._read_stderr()
    assert output == "stderr-text"

    stderr_file.close()
