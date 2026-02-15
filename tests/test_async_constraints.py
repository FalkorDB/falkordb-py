import pytest
from falkordb.asyncio import FalkorDB
from redis.asyncio import BlockingConnectionPool

@pytest.mark.asyncio
async def test_constraints():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_constraints")

    # create node constraints
    await g.create_node_unique_constraint("Person", "name")
    await g.create_node_mandatory_constraint("Person", "name")
    await g.create_node_unique_constraint("Person", "v1", "v2")

    # create edge constraints
    await g.create_edge_unique_constraint("KNOWS", "since")
    await g.create_edge_mandatory_constraint("KNOWS", "since")
    await g.create_edge_unique_constraint("KNOWS", "v1", "v2")

    constraints = await g.list_constraints()
    assert(len(constraints) == 6)

    # drop constraints
    await g.drop_node_unique_constraint("Person", "name")
    await g.drop_node_mandatory_constraint("Person", "name")
    await g.drop_node_unique_constraint("Person", "v1", "v2")

    await g.drop_edge_unique_constraint("KNOWS", "since")
    await g.drop_edge_mandatory_constraint("KNOWS", "since")
    await g.drop_edge_unique_constraint("KNOWS", "v1", "v2")

    constraints = await g.list_constraints()
    assert(len(constraints) == 0)

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_create_existing_constraint():
    # trying to create an existing constraint
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_constraints")

    # create node constraints
    await g.create_node_unique_constraint("Person", "name")
    try:
        await g.create_node_unique_constraint("Person", "name")
        assert(False)
    except Exception as e:
        assert("Constraint already exists" == str(e))

    # close the connection pool
    await pool.aclose()
