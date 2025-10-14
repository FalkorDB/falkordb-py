# Copyright (c) 2024, FalkorDB
# Licensed under the MIT License
"""
This module provides embedded FalkorDB functionality.
It manages a local Redis+FalkorDB process that runs automatically.
"""

__all__ = ['EmbeddedFalkorDB']

from .client import EmbeddedFalkorDB  # NOQA
