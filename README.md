# FalkorDB

FalkorDB python client

see [docs](http://falkordb-py.readthedocs.io/)

## Installation
```sh
pip install FalkorDB
```

## Usage

### Run FalkorDB instance
Docker:
```sh
docker run --rm -p 6379:6379 falkordb/falkordb
```
Or use our [sandbox](https://cloud.falkordb.com/sandbox)

### Example 
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
```
