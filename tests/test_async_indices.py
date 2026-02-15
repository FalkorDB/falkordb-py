import pytest
from redis import ResponseError
from falkordb.asyncio import FalkorDB
from redis.asyncio import BlockingConnectionPool

class Index():
    def __init__(self, raw_response):
        self.label       = raw_response[0]
        self.properties  = raw_response[1]
        self.types       = raw_response[2]
        self.entity_type = raw_response[6]

@pytest.mark.asyncio
async def test_node_index_creation():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    graph = db.select_graph("async_indices")

    lbl = "N"

    # create node indices

    # create node range index
    res = await graph.create_node_range_index(lbl, 'x')
    assert(res.indices_created == 1)

    index = Index((await graph.list_indices()).result_set[0])
    assert(index.label       == lbl)
    assert(index.properties  == ['x'])
    assert(index.types['x']  == ['RANGE'])
    assert(index.entity_type == 'NODE')

    # create node range index over multiple properties
    res = await graph.create_node_range_index(lbl, 'y', 'z')
    assert(res.indices_created == 2)

    index = Index((await graph.list_indices()).result_set[0])
    assert(index.label       == lbl)
    assert(index.properties  == ['x', 'y', 'z'])
    assert(index.types['x']  == ['RANGE'])
    assert(index.types['y']  == ['RANGE'])
    assert(index.types['z']  == ['RANGE'])
    assert(index.entity_type == 'NODE')

    # try to create an existing index
    with pytest.raises(ResponseError):
        res = await graph.create_node_range_index(lbl, 'z', 'x')

    # create node full-text index
    res = await graph.create_node_fulltext_index(lbl, 'name')
    assert(res.indices_created == 1)

    index = Index((await graph.list_indices()).result_set[0])
    assert(index.label         == lbl)
    assert(index.properties    == ['x', 'y', 'z', 'name'])
    assert(index.types['x']    == ['RANGE'])
    assert(index.types['y']    == ['RANGE'])
    assert(index.types['z']    == ['RANGE'])
    assert(index.types['name'] == ['FULLTEXT'])
    assert(index.entity_type   == 'NODE')

    # create node vector index
    res = await graph.create_node_vector_index(lbl, 'desc', dim=32, similarity_function="euclidean")
    assert(res.indices_created == 1)

    index = Index((await graph.list_indices()).result_set[0])
    assert(index.label         == lbl)
    assert(index.properties    == ['x', 'y', 'z', 'name', 'desc'])
    assert(index.types['x']    == ['RANGE'])
    assert(index.types['y']    == ['RANGE'])
    assert(index.types['z']    == ['RANGE'])
    assert(index.types['name'] == ['FULLTEXT'])
    assert(index.types['desc'] == ['VECTOR'])
    assert(index.entity_type   == 'NODE')

    # create a multi-type property
    res = await graph.create_node_fulltext_index(lbl, 'x')
    assert(res.indices_created == 1)

    index = Index((await graph.list_indices()).result_set[0])
    assert(index.label         == lbl)
    assert(index.properties    == ['x', 'y', 'z', 'name', 'desc'])
    assert(index.types['x']    == ['RANGE', 'FULLTEXT'])
    assert(index.types['y']    == ['RANGE'])
    assert(index.types['z']    == ['RANGE'])
    assert(index.types['name'] == ['FULLTEXT'])
    assert(index.types['desc'] == ['VECTOR'])
    assert(index.entity_type   == 'NODE')

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_edge_index_creation():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    graph = db.select_graph("async_indices")
    await graph.delete()

    rel = "R"

    # create edge indices

    # create edge range index
    res = await graph.create_edge_range_index(rel, 'x')
    assert(res.indices_created == 1)

    index = Index((await graph.list_indices()).result_set[0])
    assert(index.label       ==rel)
    assert(index.properties  == ['x'])
    assert(index.types['x']  == ['RANGE'])
    assert(index.entity_type == 'RELATIONSHIP')

    # create edge range index over multiple properties
    res = await graph.create_edge_range_index(rel, 'y', 'z')
    assert(res.indices_created == 2)

    index = Index((await graph.list_indices()).result_set[0])
    assert(index.label       ==rel)
    assert(index.properties  == ['x', 'y', 'z'])
    assert(index.types['x']  == ['RANGE'])
    assert(index.types['y']  == ['RANGE'])
    assert(index.types['z']  == ['RANGE'])
    assert(index.entity_type == 'RELATIONSHIP')

    # try to create an existing index
    with pytest.raises(ResponseError):
        res = await graph.create_edge_range_index(rel, 'z', 'x')

    # create edge full-text index
    res = await graph.create_edge_fulltext_index(rel, 'name')
    assert(res.indices_created == 1)

    index = Index((await graph.list_indices()).result_set[0])
    assert(index.label         ==rel)
    assert(index.properties    == ['x', 'y', 'z', 'name'])
    assert(index.types['x']    == ['RANGE'])
    assert(index.types['y']    == ['RANGE'])
    assert(index.types['z']    == ['RANGE'])
    assert(index.types['name'] == ['FULLTEXT'])
    assert(index.entity_type   == 'RELATIONSHIP')

    # create edge vector index
    res = await graph.create_edge_vector_index(rel, 'desc', dim=32, similarity_function="euclidean")
    assert(res.indices_created == 1)

    index = Index((await graph.list_indices()).result_set[0])
    assert(index.label         ==rel)
    assert(index.properties    == ['x', 'y', 'z', 'name', 'desc'])
    assert(index.types['x']    == ['RANGE'])
    assert(index.types['y']    == ['RANGE'])
    assert(index.types['z']    == ['RANGE'])
    assert(index.types['name'] == ['FULLTEXT'])
    assert(index.types['desc'] == ['VECTOR'])
    assert(index.entity_type   == 'RELATIONSHIP')

    # create a multi-type property
    res = await graph.create_edge_fulltext_index(rel, 'x')
    assert(res.indices_created == 1)

    index = Index((await graph.list_indices()).result_set[0])
    assert(index.label         ==rel)
    assert(index.properties    == ['x', 'y', 'z', 'name', 'desc'])
    assert(index.types['x']    == ['RANGE', 'FULLTEXT'])
    assert(index.types['y']    == ['RANGE'])
    assert(index.types['z']    == ['RANGE'])
    assert(index.types['name'] == ['FULLTEXT'])
    assert(index.types['desc'] == ['VECTOR'])
    assert(index.entity_type   == 'RELATIONSHIP')

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_node_index_drop():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    graph = db.select_graph("async_indices")
    await graph.delete()

    # create an index and delete it
    lbl = 'N'
    attr = 'x'

    # create node range index
    res = await graph.create_node_range_index(lbl, attr)
    assert(res.indices_created == 1)

    # list indices
    res = await graph.list_indices()
    assert(len(res.result_set) == 1)
    
    # drop range index
    res = await graph.drop_node_range_index(lbl, attr)
    assert(res.indices_deleted == 1)

    # list indices
    res = await graph.list_indices()
    assert(len(res.result_set) == 0)

    #---------------------------------------------------------------------------

    # create node fulltext index
    res = await graph.create_node_fulltext_index(lbl, attr)
    assert(res.indices_created == 1)

    # list indices
    res = await graph.list_indices()
    assert(len(res.result_set) == 1)

    # drop fulltext index
    res = await graph.drop_node_fulltext_index(lbl, attr)
    assert(res.indices_deleted == 1)

    # list indices
    res = await graph.list_indices()
    assert(len(res.result_set) == 0)

    #---------------------------------------------------------------------------

    # create node vector index
    res = await graph.create_node_vector_index(lbl, attr)
    assert(res.indices_created == 1)

    # list indices
    res = await graph.list_indices()
    assert(len(res.result_set) == 1)

    # drop vector index
    res = await graph.drop_node_vector_index(lbl, attr)
    assert(res.indices_deleted == 1)

    # list indices
    res = await graph.list_indices()
    assert(len(res.result_set) == 0)

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_edge_index_drop():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    graph = db.select_graph("async_indices")
    await graph.delete()

    # create an index and delete it
    rel = 'R'
    attr = 'x'

    # create edge range index
    res = await graph.create_edge_range_index(rel, attr)
    assert(res.indices_created == 1)

    # list indices
    res = await graph.list_indices()
    assert(len(res.result_set) == 1)
    
    # drop range index
    res = await graph.drop_edge_range_index(rel, attr)
    assert(res.indices_deleted == 1)

    # list indices
    res = await graph.list_indices()
    assert(len(res.result_set) == 0)

    #---------------------------------------------------------------------------

    # create edge fulltext index
    res = await graph.create_edge_fulltext_index(rel, attr)
    assert(res.indices_created == 1)

    # list indices
    res = await graph.list_indices()
    assert(len(res.result_set) == 1)

    # drop fulltext index
    res = await graph.drop_edge_fulltext_index(rel, attr)
    assert(res.indices_deleted == 1)

    # list indices
    res = await graph.list_indices()
    assert(len(res.result_set) == 0)

    #---------------------------------------------------------------------------

    # create edge vector index
    res = await graph.create_edge_vector_index(rel, attr)
    assert(res.indices_created == 1)

    # list indices
    res = await graph.list_indices()
    assert(len(res.result_set) == 1)

    # drop vector index
    res = await graph.drop_edge_vector_index(rel, attr)
    assert(res.indices_deleted == 1)

    # list indices
    res = await graph.list_indices()
    assert(len(res.result_set) == 0)

    # close the connection pool
    await pool.aclose()
