[![license](https://img.shields.io/github/license/falkordb/falkordb-py.svg)](https://github.com/falkordb/falkordb-py)
[![Release](https://img.shields.io/github/release/falkordb/falkordb-py.svg)](https://github.com/falkordb/falkordb-py/releases/latest)
[![PyPI version](https://badge.fury.io/py/falkordb.svg)](https://badge.fury.io/py/falkordb)
[![Codecov](https://codecov.io/gh/falkordb/falkordb-py/branch/main/graph/badge.svg)](https://codecov.io/gh/falkordb/falkordb-py)
[![Forum](https://img.shields.io/badge/Forum-falkordb-blue)](https://github.com/orgs/FalkorDB/discussions)
[![Discord](https://img.shields.io/discord/1146782921294884966?style=flat-square)](https://discord.gg/ErBEqN9E)

# falkordb-py

[![Try Free](https://img.shields.io/badge/Try%20Free-FalkorDB%20Cloud-FF8101?labelColor=FDE900&style=for-the-badge&link=https://app.falkordb.cloud)](https://app.falkordb.cloud)

FalkorDB Python client

see [docs](http://falkordb-py.readthedocs.io/)

## Installation
```sh
pip install FalkorDB
```

To install with embedded FalkorDB support:
```sh
pip install FalkorDB[embedded]
```

**Note**: For embedded mode, you also need:
- Redis server installed (`redis-server` in PATH)
- FalkorDB module (download from [FalkorDB releases](https://github.com/FalkorDB/FalkorDB/releases))

## Usage

### Run FalkorDB instance
Docker:
```sh
docker run --rm -p 6379:6379 falkordb/falkordb
```
Or use [FalkorDB Cloud](https://app.falkordb.cloud)

Or use embedded FalkorDB (no external server needed):
```python
from falkordb import FalkorDB

# Create an embedded FalkorDB instance
db = FalkorDB(embedded=True)

# Use it just like a remote connection
g = db.select_graph('social')
result = g.query("CREATE (n:Person {name: 'Alice'}) RETURN n")
```

### Synchronous Example

```python
from falkordb import FalkorDB

# Connect to FalkorDB
db = FalkorDB(host='localhost', port=6379)

# Select the social graph
g = db.select_graph('social')

# Create 100 nodes and return a handful
nodes = g.query('UNWIND range(0, 100) AS i CREATE (n {v:1}) RETURN n LIMIT 10').result_set
for n in nodes:
    print(n)

# Read-only query the graph for the first 10 nodes
nodes = g.ro_query('MATCH (n) RETURN n LIMIT 10').result_set

# Copy the Graph
copy_graph = g.copy('social_copy')

# Delete the Graph
g.delete()
```

### Asynchronous Example

```python
import asyncio
from falkordb.asyncio import FalkorDB
from redis.asyncio import BlockingConnectionPool

async def main():

    # Connect to FalkorDB
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    
    # Select the social graph
    g = db.select_graph('social')
    
    # Execute query asynchronously
    result = await g.query('UNWIND range(0, 100) AS i CREATE (n {v:1}) RETURN n LIMIT 10')
    
    # Process results
    for n in result.result_set:
        print(n)
    
    # Run multiple queries concurrently
    tasks = [
        g.query('MATCH (n) WHERE n.v = 1 RETURN count(n) AS count'),
        g.query('CREATE (p:Person {name: "Alice"}) RETURN p'),
        g.query('CREATE (p:Person {name: "Bob"}) RETURN p')
    ]
    
    results = await asyncio.gather(*tasks)
    
    # Process concurrent results
    print(f"Node count: {results[0].result_set[0][0]}")
    print(f"Created Alice: {results[1].result_set[0][0]}")
    print(f"Created Bob: {results[2].result_set[0][0]}")
    
    # Close the connection when done
    await pool.aclose()

# Run the async example
if __name__ == "__main__":
    asyncio.run(main())
```

### Embedded FalkorDB

FalkorDB supports an embedded mode that runs Redis + FalkorDB in a local process, eliminating the need for a separate server. This is useful for development, testing, or applications that need a self-contained graph database.

**Installation:**
```sh
pip install FalkorDB[embedded]
```

**Prerequisites:**
- Redis server installed (`redis-server` must be in PATH)
- FalkorDB module downloaded (`.so` file from [FalkorDB releases](https://github.com/FalkorDB/FalkorDB/releases))

Place the FalkorDB module in one of these locations:
- `/usr/local/lib/falkordb.so`
- `/usr/lib/falkordb.so`
- In the `falkordb/bin/` directory of your Python package

**Usage:**
```python
from falkordb import FalkorDB

# Create an embedded instance (data stored in memory)
db = FalkorDB(embedded=True)

# Or specify a database file for persistence
db = FalkorDB(embedded=True, dbfilename='/path/to/database.db')

# Use it exactly like a remote FalkorDB instance
graph = db.select_graph('my_graph')
result = graph.query('CREATE (n:Person {name: "John", age: 30}) RETURN n')

# Data persists across connections when using dbfilename
del db

# Reconnect to the same database
db = FalkorDB(embedded=True, dbfilename='/path/to/database.db')
graph = db.select_graph('my_graph')
result = graph.query('MATCH (n:Person) RETURN n.name, n.age')
```

**Notes:**
- Embedded mode uses Unix sockets for communication (no network overhead)
- The embedded server automatically starts and stops with your application
- Embedded mode is not available with asyncio (use the synchronous API)
- For production deployments, use a standalone FalkorDB server or FalkorDB Cloud
```
