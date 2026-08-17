import pytest
from redis import ResponseError
from redis.asyncio import BlockingConnectionPool

from falkordb.asyncio import FalkorDB

from .plan_helpers import assert_parsed_plan, assert_plan_shape


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

    assert_plan_shape(
        plan,
        """
        Project
            Unwind
        """,
    )

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

    assert_plan_shape(
        plan,
        """
        Project
            Cartesian Product
                All Node Scan | (a)
                All Node Scan | (b)
        """,
    )

    # close the connection pool
    await pool.aclose()


@pytest.mark.asyncio
async def test_cartesian_product_explain_three_way():
    pool = BlockingConnectionPool(
        max_connections=16, timeout=None, decode_responses=True
    )
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_explain")
    plan = await g.explain("MATCH (a), (b), (c) RETURN *")

    # three operations share a nesting level here, which is what the parser
    # used to get wrong: it attached the third scan to the second instead of
    # to the cartesian product. The engines scan the three in whichever order
    # they like, so the arguments are left out.
    assert_plan_shape(
        plan,
        """
        Project
            Cartesian Product
                All Node Scan
                All Node Scan
                All Node Scan
        """,
    )

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
    except ResponseError as e:
        # an earlier run of this test already created the index, any other
        # failure is a real one and must not be swallowed
        assert "already indexed" in str(e), e
    plan = await g.explain("MERGE (p1:person {age: 40}) MERGE (p2:person {age: 41})")

    # the two engines compile MERGE differently — Rust matches through an
    # "Include Pending" operation, C emits a "MergeCreate" — so there is no
    # single tree to assert. Check the client parsed whatever came back.
    assert_parsed_plan(plan, min_operations=4, expect_args=True)

    # close the connection pool
    await pool.aclose()
