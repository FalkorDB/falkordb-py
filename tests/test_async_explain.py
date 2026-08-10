import pytest
from redis.asyncio import BlockingConnectionPool

from falkordb.asyncio import FalkorDB

from .plan_helpers import assert_parsed_plan


@pytest.mark.asyncio
async def test_explain():
    pool = BlockingConnectionPool(
        max_connections=16, timeout=None, decode_responses=True
    )
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_explain")

    # run a single query to create the graph
    await g.query("RETURN 1")

    plan = await g.explain("UNWIND range(0, 3) AS x RETURN x")

    # which operations the query compiles into is up to the engine, the client
    # is responsible for parsing whatever plan comes back
    assert_parsed_plan(plan, min_operations=2)

    # close the connection pool
    await pool.aclose()


@pytest.mark.asyncio
async def test_cartesian_product_explain():
    pool = BlockingConnectionPool(
        max_connections=16, timeout=None, decode_responses=True
    )
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_explain")
    plan = await g.explain("MATCH (a), (b) RETURN *")

    assert_parsed_plan(plan, min_operations=4, expect_args=True)

    # close the connection pool
    await pool.aclose()


@pytest.mark.asyncio
async def test_merge():
    pool = BlockingConnectionPool(
        max_connections=16, timeout=None, decode_responses=True
    )
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_explain")

    try:
        await g.create_node_range_index("person", "age")
    except Exception:
        pass
    plan = await g.explain("MERGE (p1:person {age: 40}) MERGE (p2:person {age: 41})")

    assert_parsed_plan(plan, min_operations=4, expect_args=True)

    # close the connection pool
    await pool.aclose()
