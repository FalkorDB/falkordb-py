"""Generate Redis configuration used by embedded FalkorDB."""

import os
from pathlib import Path
from typing import Optional

DEFAULT_CONFIG = {
    "bind": "127.0.0.1",
    "port": "0",
    "save": "",
    "appendonly": "no",
    "protected-mode": "yes",
    "loglevel": "warning",
    "databases": "16",
}

PERSISTENT_OVERRIDES = {
    "save": [("900", "1"), ("300", "10"), ("60", "10000")],
    "appendonly": "yes",
    "appendfsync": "everysec",
}


def generate_config(
    falkordb_module_path: Path,
    db_path: Optional[str] = None,
    unix_socket_path: Optional[str] = None,
    user_config: Optional[dict] = None,
) -> str:
    """Return redis.conf content for embedded mode."""
    config = dict(DEFAULT_CONFIG)

    if db_path:
        abs_db_path = os.path.abspath(db_path)
        db_dir = os.path.dirname(abs_db_path)
        db_file = os.path.basename(abs_db_path)
        os.makedirs(db_dir, exist_ok=True)
        config["dir"] = db_dir
        config["dbfilename"] = db_file
        config.update(PERSISTENT_OVERRIDES)

    if unix_socket_path:
        config["unixsocket"] = unix_socket_path
        config["unixsocketperm"] = "700"
        config["port"] = "0"

    if user_config:
        if "loadmodule" in user_config:
            raise ValueError("'loadmodule' is reserved and cannot be overridden")
        config.update(user_config)

    config["loadmodule"] = str(falkordb_module_path)

    lines = []
    for key, value in config.items():
        if key == "save" and isinstance(value, list):
            for rule in value:
                if isinstance(rule, (tuple, list)) and len(rule) == 2:
                    lines.append(f"save {rule[0]} {rule[1]}")
                else:
                    lines.append(f"save {rule}")
            continue
        lines.append(f"{key} {value}")

    return "\n".join(lines) + "\n"
