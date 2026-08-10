import pytest
from redis.asyncio import BlockingConnectionPool

from falkordb.asyncio import FalkorDB

from .plan_helpers import assert_parsed_profile


@pytest.mark.asyncio
async def test_profile():
    pool = BlockingConnectionPool(
        max_connections=16, timeout=None, decode_responses=True
    )
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_profile")

    plan = await g.profile("UNWIND range(0, 3) AS x RETURN x")

    # which operations the query compiles into is up to the engine, the client
    # is responsible for parsing the plan and its statistics
    assert_parsed_profile(plan, min_operations=2, records_produced=4)

    # close the connection pool
    await pool.aclose()


@pytest.mark.asyncio
async def test_cartesian_product_profile():
    pool = BlockingConnectionPool(
        max_connections=16, timeout=None, decode_responses=True
    )
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_profile")

    plan = await g.profile("MATCH (a), (b) RETURN *")

    assert_parsed_profile(plan, min_operations=4, expect_args=True, records_produced=0)

    # close the connection pool
    await pool.aclose()
