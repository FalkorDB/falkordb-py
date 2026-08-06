import pytest
from redis.asyncio import BlockingConnectionPool

from falkordb.asyncio import FalkorDB


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

    project_op = plan.structured_plan
    assert project_op.name == "Project"
    assert len(project_op.children) == 1

    unwind_op = project_op.children[0]
    assert unwind_op.name == "Unwind"
    assert len(unwind_op.children) == 0

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

    project_op = plan.structured_plan
    assert project_op.name == "Project"
    assert len(project_op.children) == 1

    cp_op = project_op.children[0]
    assert cp_op.name == "Cartesian Product"
    assert len(cp_op.children) == 2

    scan_a_op = cp_op.children[0]
    scan_b_op = cp_op.children[1]

    assert scan_a_op.name == "All Node Scan"
    assert len(scan_a_op.children) == 0

    assert scan_b_op.name == "All Node Scan"
    assert len(scan_b_op.children) == 0

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

    root = plan.structured_plan
    assert root.name == "Commit"
    assert len(root.children) == 1

    # assert on the operations the plan is made of rather than on the exact
    # shape of the operation tree, which varies between server versions
    merge_ops = plan.collect_operations("Merge")
    assert len(merge_ops) == 2

    index_scan_ops = plan.collect_operations("Node By Index Scan")
    assert len(index_scan_ops) == 2

    for index_scan_op in index_scan_ops:
        assert len(index_scan_op.children) == 1
        assert index_scan_op.children[0].name == "Argument"
        assert len(index_scan_op.children[0].children) == 0

    # close the connection pool
    await pool.aclose()
