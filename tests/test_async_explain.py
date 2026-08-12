import contextlib

import pytest
from redis.asyncio import BlockingConnectionPool

from falkordb.asyncio import FalkorDB

from .plan_utils import plan_root


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

    project_op = plan_root(plan)
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

    project_op = plan_root(plan)
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

    with contextlib.suppress(Exception):
        await g.create_node_range_index("person", "age")
    plan = await g.explain("MERGE (p1:person {age: 40}) MERGE (p2:person {age: 41})")

    root = plan.structured_plan
    # the exact shape of a MERGE plan is a server implementation detail that
    # has changed between releases, assert the parser produced a well-formed
    # tree containing the operations this query must involve
    assert len(plan.collect_operations("Merge")) == 2
    assert len(plan.collect_operations("Node By Index Scan")) == 2
    assert len(plan.collect_operations("Argument")) == 2

    seen = []

    def walk(op):
        seen.append(op)
        for child in op.children:
            walk(child)

    walk(root)
    indexed = sum(len(ops) for ops in plan.operations.values())
    assert len(seen) == indexed

    # close the connection pool
    await pool.aclose()
