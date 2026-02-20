# Project Guidelines

## Overview
falkordb-py is the official Python client for [FalkorDB](https://github.com/FalkorDB/FalkorDB), a graph database that uses the openCypher query language. It wraps Redis connections (via `redis-py`) and adds graph-specific commands (`GRAPH.QUERY`, `GRAPH.DELETE`, etc.).

## Build & Install
```bash
uv sync                # install runtime dependencies
uv sync --extra test   # also install test dependencies (pytest, pytest-asyncio, pytest-cov)
uv sync --group dev    # also install dev tools (ruff, mypy, types-redis)
```

## Testing
Tests require a running FalkorDB instance on `localhost:6379`:
```bash
docker run -p 6379:6379 -d falkordb/falkordb:edge
```
Run all tests:
```bash
uv run pytest
```
Run a single test file or test:
```bash
uv run pytest tests/test_graph.py
uv run pytest tests/test_graph.py::test_query
```
With coverage:
```bash
uv run pytest --cov --cov-report=xml
```
Async tests use `pytest-asyncio` and are prefixed `test_async_*.py`. They mirror the sync tests 1:1.

## Pre-commit Lint Checks
Always run these three checks before every commit:
```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy falkordb/
```
If formatting fails, fix with `uv run ruff format .` before committing.

## Code Style
- **Formatter/linter**: Ruff (line length 88, target Python 3.10)
- **Lint rules**: `F` (Pyflakes), `E`/`W` (pycodestyle), `I` (isort)
- **Type checking**: mypy with `ignore_missing_imports = true`
- **Python**: requires >= 3.10; CI tests 3.10 through 3.14

## Project Structure
```
falkordb/
  falkordb.py        # FalkorDB client — connection setup, config, graph listing
  graph.py           # Graph — query, delete, copy, explain, profile, constraints, indices
  query_result.py    # QueryResult — parses GRAPH.QUERY responses into nodes/edges/scalars
  graph_schema.py    # GraphSchema — caches labels, relationship types, property keys
  node.py            # Node model
  edge.py            # Edge model
  path.py            # Path model
  execution_plan.py  # ExecutionPlan — parses GRAPH.EXPLAIN / GRAPH.PROFILE output
  helpers.py         # Parameter serialization helpers
  exceptions.py      # Custom exceptions
  cluster.py         # Redis Cluster support
  sentinel.py        # Redis Sentinel support
  _version.py        # Package version via importlib.metadata
  asyncio/           # Async mirror (see below)
  lite/              # Lightweight variant
tests/
  test_*.py          # Sync tests
  test_async_*.py    # Async tests (mirror sync tests)
```

## Architecture Patterns

### Sync / Async Parity
Every sync class in `falkordb/` has an async counterpart in `falkordb/asyncio/`:
| Sync | Async |
|------|-------|
| `falkordb.py` → `FalkorDB` | `asyncio/falkordb.py` → `AsyncFalkorDB` |
| `graph.py` → `Graph` | `asyncio/graph.py` → `AsyncGraph` |
| `query_result.py` → `QueryResult` | `asyncio/query_result.py` → `AsyncQueryResult` |

When modifying sync code, always check if the async counterpart needs the same change.

### Redis Integration
- `FalkorDB` wraps a `redis.Redis` (or `redis.asyncio.Redis`) connection
- `Graph` receives the client and delegates commands via `client.execute_command()`
- Graph commands are plain Redis commands: `GRAPH.QUERY`, `GRAPH.RO_QUERY`, `GRAPH.DELETE`, `GRAPH.EXPLAIN`, `GRAPH.PROFILE`, `GRAPH.COPY`, `GRAPH.LIST`, `GRAPH.CONFIG`

### Graph Model
- `Node`, `Edge`, `Path` are lightweight data classes returned inside `QueryResult`
- `GraphSchema` caches label/relationship-type/property-key mappings and refreshes on schema version mismatch (`SchemaVersionMismatchException`)

## CI/CD
- **`lint.yml`**: Runs ruff format, ruff check, and mypy on Python 3.14
- **`test.yml`**: Runs pytest against a `falkordb/falkordb:edge` Docker service on Python 3.10–3.14; uploads coverage to Codecov

