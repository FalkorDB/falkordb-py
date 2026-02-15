"""Embedded redis-server lifecycle management for FalkorDB."""

import atexit
import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional

import redis

from .binaries import get_falkordb_module_path, get_redis_server_path
from .config import generate_config


class EmbeddedServerError(RuntimeError):
    """Raised when the embedded server cannot be started or managed."""


class EmbeddedServer:
    """Manage an embedded redis-server process configured with FalkorDB module."""

    def __init__(
        self,
        db_path: Optional[str] = None,
        config: Optional[dict] = None,
        startup_timeout: float = 10.0,
    ):
        self._process = None
        self._stderr_file = None
        self._db_path = db_path
        self._startup_timeout = startup_timeout
        self._tmpdir = tempfile.mkdtemp(prefix="falkordb_")
        self._socket_path = os.path.join(self._tmpdir, "falkordb.sock")
        self._config_path = os.path.join(self._tmpdir, "redis.conf")
        self._stderr_path = os.path.join(self._tmpdir, "redis.stderr.log")

        redis_server = get_redis_server_path()
        falkordb_module = get_falkordb_module_path()

        config_content = generate_config(
            falkordb_module_path=falkordb_module,
            db_path=db_path,
            unix_socket_path=self._socket_path,
            user_config=config,
        )
        Path(self._config_path).write_text(config_content, encoding="utf-8")

        self._start(redis_server)
        atexit.register(self.stop)

    def _start(self, redis_server: Path) -> None:
        self._stderr_file = open(self._stderr_path, "w", encoding="utf-8")
        self._process = subprocess.Popen(  # noqa: S603
            [str(redis_server), self._config_path],
            stdout=subprocess.DEVNULL,
            stderr=self._stderr_file,
        )

        deadline = time.monotonic() + self._startup_timeout
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                stderr = self._read_stderr()
                self._close_stderr_file()
                raise EmbeddedServerError(
                    f"redis-server exited with code {self._process.returncode}: {stderr}"
                )

            if os.path.exists(self._socket_path):
                try:
                    conn = redis.Redis(unix_socket_path=self._socket_path, decode_responses=True)
                    conn.ping()
                    conn.close()
                    self._close_stderr_file()
                    return
                except redis.ConnectionError:
                    pass
            time.sleep(0.05)

        self.stop()
        raise EmbeddedServerError(
            f"redis-server did not start within {self._startup_timeout} seconds"
        )

    @property
    def unix_socket_path(self) -> str:
        return self._socket_path

    def stop(self) -> None:
        """Stop embedded redis-server and cleanup temporary files."""
        if self._process is not None and self._process.poll() is None:
            conn = None
            try:
                conn = redis.Redis(unix_socket_path=self._socket_path, decode_responses=True)
                try:
                    conn.shutdown(nosave=not bool(self._db_path))
                except redis.exceptions.ConnectionError:
                    # redis-server can drop the connection as part of normal shutdown
                    pass
            except Exception:
                self._process.terminate()
            finally:
                if conn is not None:
                    conn.close()

            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)

        self._process = None
        self._close_stderr_file()
        if os.path.isdir(self._tmpdir):
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _read_stderr(self) -> str:
        if self._stderr_file is not None and not self._stderr_file.closed:
            self._stderr_file.flush()
        if not os.path.exists(self._stderr_path):
            return ""
        return Path(self._stderr_path).read_text(encoding="utf-8", errors="replace")

    def _close_stderr_file(self) -> None:
        if self._stderr_file is not None and not self._stderr_file.closed:
            self._stderr_file.close()

    def __del__(self):
        try:
            self.stop()
        except Exception:
            pass
