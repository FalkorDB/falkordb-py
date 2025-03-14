import pytest
from falkordb.asyncio import FalkorDB
from redis.asyncio import BlockingConnectionPool

@pytest.mark.asyncio
async def test_graph_copy():
    # create a simple graph and clone it
    # make sure graphs are the same

    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    src = db.select_graph("async_src")

    # create entities
    await src.query("CREATE (:A {v:1})-[:R {v:2}]->(:B {v:3})")

    # create index
    await src.create_edge_range_index("A", "v")
    await src.create_edge_range_index("R", "v")
    await src.create_node_fulltext_index("B", "v")

    # create constrain
    await src.create_node_unique_constraint("A", "v")
    await src.create_edge_unique_constraint("R", "v")

    # clone graph
    dest = await src.copy("async_dest")

    # validate src and dest are the same
    # validate entities
    q = "MATCH (a) RETURN a ORDER BY ID(a)"
    src_res  = (await src.query(q)).result_set
    dest_res = (await dest.query(q)).result_set
    assert(src_res == dest_res)

    q = "MATCH ()-[e]->() RETURN e ORDER BY ID(e)"
    src_res  = (await src.query(q)).result_set
    dest_res = (await dest.query(q)).result_set
    assert(src_res == dest_res)

    # validate schema
    src_res  = (await src.call_procedure("DB.LABELS")).result_set
    dest_res = (await dest.call_procedure("DB.LABELS")).result_set
    assert(src_res == dest_res)

    src_res  = (await src.call_procedure("DB.PROPERTYKEYS")).result_set
    dest_res = (await dest.call_procedure("DB.PROPERTYKEYS")).result_set
    assert(src_res == dest_res)

    src_res  = (await src.call_procedure("DB.RELATIONSHIPTYPES")).result_set
    dest_res = (await dest.call_procedure("DB.RELATIONSHIPTYPES")).result_set
    assert(src_res == dest_res)

    # validate indices
    q = """CALL DB.INDEXES()
           YIELD label, properties, types, language, stopwords, entitytype, status
           RETURN *
           ORDER BY label, properties, types, language, stopwords, entitytype, status"""
    src_res = (await src.query(q)).result_set
    dest_res = (await dest.query(q)).result_set

    assert(src_res == dest_res)

    # validate constraints
    src_res = await src.list_constraints()
    dest_res = await dest.list_constraints()
    assert(src_res == dest_res)

    # close the connection pool
    await pool.aclose()
