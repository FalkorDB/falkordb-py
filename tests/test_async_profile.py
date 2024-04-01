import pytest
from falkordb.asyncio import FalkorDB
from redis.asyncio import BlockingConnectionPool


@pytest.mark.asyncio
async def test_profile():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_profile")

    plan = await g.profile("UNWIND range(0, 3) AS x RETURN x")

    results_op = plan.structured_plan
    assert(results_op.name == 'Results')
    assert(len(results_op.children) == 1)
    assert(results_op.profile_stats.records_produced == 4)

    project_op = results_op.children[0]
    assert(project_op.name == 'Project')
    assert(len(project_op.children) == 1)
    assert(project_op.profile_stats.records_produced == 4)

    unwind_op = project_op.children[0]
    assert(unwind_op.name == 'Unwind')
    assert(len(unwind_op.children) == 0)
    assert(unwind_op.profile_stats.records_produced == 4)

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_cartesian_product_profile():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_profile")

    plan = await g.profile("MATCH (a), (b) RETURN *")

    results_op = plan.structured_plan
    assert(results_op.name == 'Results')
    assert(len(results_op.children) == 1)
    assert(results_op.profile_stats.records_produced == 0)

    project_op = results_op.children[0]
    assert(project_op.name == 'Project')
    assert(len(project_op.children) == 1)
    assert(project_op.profile_stats.records_produced == 0)

    cp_op = project_op.children[0]
    assert(cp_op.name == 'Cartesian Product')
    assert(len(cp_op.children) == 2)
    assert(cp_op.profile_stats.records_produced == 0)

    scan_a_op = cp_op.children[0]
    scan_b_op = cp_op.children[1]

    assert(scan_a_op.name == 'All Node Scan')
    assert(len(scan_a_op.children) == 0)
    assert(scan_a_op.profile_stats.records_produced == 0)

    assert(scan_b_op.name == 'All Node Scan')
    assert(len(scan_b_op.children) == 0)
    assert(scan_b_op.profile_stats.records_produced == 0)

    # close the connection pool
    await pool.aclose()
