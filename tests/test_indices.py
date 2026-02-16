import pytest
from redis import ResponseError

from falkordb import FalkorDB


class Index:
    def __init__(self, raw_response):
        self.label = raw_response[0]
        self.properties = raw_response[1]
        self.types = raw_response[2]
        self.entity_type = raw_response[6]


@pytest.fixture
def client(request):
    db = FalkorDB(host="localhost", port=6379)
    db.flushdb()
    return db.select_graph("indices")


def test_node_index_creation(client):
    graph = client
    lbl = "N"

    # create node indices

    # create node range index
    res = graph.create_node_range_index(lbl, "x")
    assert res.indices_created == 1

    index = Index(graph.list_indices().result_set[0])
    assert index.label == lbl
    assert index.properties == ["x"]
    assert index.types["x"] == ["RANGE"]
    assert index.entity_type == "NODE"

    # create node range index over multiple properties
    res = graph.create_node_range_index(lbl, "y", "z")
    assert res.indices_created == 2

    index = Index(graph.list_indices().result_set[0])
    assert index.label == lbl
    assert index.properties == ["x", "y", "z"]
    assert index.types["x"] == ["RANGE"]
    assert index.types["y"] == ["RANGE"]
    assert index.types["z"] == ["RANGE"]
    assert index.entity_type == "NODE"

    # try to create an existing index
    with pytest.raises(ResponseError):
        res = graph.create_node_range_index(lbl, "z", "x")

    # create node full-text index
    res = graph.create_node_fulltext_index(lbl, "name")
    assert res.indices_created == 1

    index = Index(graph.list_indices().result_set[0])
    assert index.label == lbl
    assert index.properties == ["x", "y", "z", "name"]
    assert index.types["x"] == ["RANGE"]
    assert index.types["y"] == ["RANGE"]
    assert index.types["z"] == ["RANGE"]
    assert index.types["name"] == ["FULLTEXT"]
    assert index.entity_type == "NODE"

    # create node vector index
    res = graph.create_node_vector_index(
        lbl, "desc", dim=32, similarity_function="euclidean"
    )
    assert res.indices_created == 1

    index = Index(graph.list_indices().result_set[0])
    assert index.label == lbl
    assert index.properties == ["x", "y", "z", "name", "desc"]
    assert index.types["x"] == ["RANGE"]
    assert index.types["y"] == ["RANGE"]
    assert index.types["z"] == ["RANGE"]
    assert index.types["name"] == ["FULLTEXT"]
    assert index.types["desc"] == ["VECTOR"]
    assert index.entity_type == "NODE"

    # create a multi-type property
    res = graph.create_node_fulltext_index(lbl, "x")
    assert res.indices_created == 1

    index = Index(graph.list_indices().result_set[0])
    assert index.label == lbl
    assert index.properties == ["x", "y", "z", "name", "desc"]
    assert index.types["x"] == ["RANGE", "FULLTEXT"]
    assert index.types["y"] == ["RANGE"]
    assert index.types["z"] == ["RANGE"]
    assert index.types["name"] == ["FULLTEXT"]
    assert index.types["desc"] == ["VECTOR"]
    assert index.entity_type == "NODE"


def test_edge_index_creation(client):
    graph = client
    rel = "R"

    # create edge indices

    # create edge range index
    res = graph.create_edge_range_index(rel, "x")
    assert res.indices_created == 1

    index = Index(graph.list_indices().result_set[0])
    assert index.label == rel
    assert index.properties == ["x"]
    assert index.types["x"] == ["RANGE"]
    assert index.entity_type == "RELATIONSHIP"

    # create edge range index over multiple properties
    res = graph.create_edge_range_index(rel, "y", "z")
    assert res.indices_created == 2

    index = Index(graph.list_indices().result_set[0])
    assert index.label == rel
    assert index.properties == ["x", "y", "z"]
    assert index.types["x"] == ["RANGE"]
    assert index.types["y"] == ["RANGE"]
    assert index.types["z"] == ["RANGE"]
    assert index.entity_type == "RELATIONSHIP"

    # try to create an existing index
    with pytest.raises(ResponseError):
        res = graph.create_edge_range_index(rel, "z", "x")

    # create edge full-text index
    res = graph.create_edge_fulltext_index(rel, "name")
    assert res.indices_created == 1

    index = Index(graph.list_indices().result_set[0])
    assert index.label == rel
    assert index.properties == ["x", "y", "z", "name"]
    assert index.types["x"] == ["RANGE"]
    assert index.types["y"] == ["RANGE"]
    assert index.types["z"] == ["RANGE"]
    assert index.types["name"] == ["FULLTEXT"]
    assert index.entity_type == "RELATIONSHIP"

    # create edge vector index
    res = graph.create_edge_vector_index(
        rel, "desc", dim=32, similarity_function="euclidean"
    )
    assert res.indices_created == 1

    index = Index(graph.list_indices().result_set[0])
    assert index.label == rel
    assert index.properties == ["x", "y", "z", "name", "desc"]
    assert index.types["x"] == ["RANGE"]
    assert index.types["y"] == ["RANGE"]
    assert index.types["z"] == ["RANGE"]
    assert index.types["name"] == ["FULLTEXT"]
    assert index.types["desc"] == ["VECTOR"]
    assert index.entity_type == "RELATIONSHIP"

    # create a multi-type property
    res = graph.create_edge_fulltext_index(rel, "x")
    assert res.indices_created == 1

    index = Index(graph.list_indices().result_set[0])
    assert index.label == rel
    assert index.properties == ["x", "y", "z", "name", "desc"]
    assert index.types["x"] == ["RANGE", "FULLTEXT"]
    assert index.types["y"] == ["RANGE"]
    assert index.types["z"] == ["RANGE"]
    assert index.types["name"] == ["FULLTEXT"]
    assert index.types["desc"] == ["VECTOR"]
    assert index.entity_type == "RELATIONSHIP"


def test_node_index_drop(client):
    graph = client

    # create an index and delete it
    lbl = "N"
    attr = "x"

    # create node range index
    res = graph.create_node_range_index(lbl, attr)
    assert res.indices_created == 1

    # list indices
    res = graph.list_indices()
    assert len(res.result_set) == 1

    # drop range index
    res = graph.drop_node_range_index(lbl, attr)
    assert res.indices_deleted == 1

    # list indices
    res = graph.list_indices()
    assert len(res.result_set) == 0

    # ---------------------------------------------------------------------------

    # create node fulltext index
    res = graph.create_node_fulltext_index(lbl, attr)
    assert res.indices_created == 1

    # list indices
    res = graph.list_indices()
    assert len(res.result_set) == 1

    # drop fulltext index
    res = graph.drop_node_fulltext_index(lbl, attr)
    assert res.indices_deleted == 1

    # list indices
    res = graph.list_indices()
    assert len(res.result_set) == 0

    # ---------------------------------------------------------------------------

    # create node vector index
    res = graph.create_node_vector_index(lbl, attr)
    assert res.indices_created == 1

    # list indices
    res = graph.list_indices()
    assert len(res.result_set) == 1

    # drop vector index
    res = graph.drop_node_vector_index(lbl, attr)
    assert res.indices_deleted == 1

    # list indices
    res = graph.list_indices()
    assert len(res.result_set) == 0


def test_edge_index_drop(client):
    graph = client

    # create an index and delete it
    rel = "R"
    attr = "x"

    # create edge range index
    res = graph.create_edge_range_index(rel, attr)
    assert res.indices_created == 1

    # list indices
    res = graph.list_indices()
    assert len(res.result_set) == 1

    # drop range index
    res = graph.drop_edge_range_index(rel, attr)
    assert res.indices_deleted == 1

    # list indices
    res = graph.list_indices()
    assert len(res.result_set) == 0

    # ---------------------------------------------------------------------------

    # create edge fulltext index
    res = graph.create_edge_fulltext_index(rel, attr)
    assert res.indices_created == 1

    # list indices
    res = graph.list_indices()
    assert len(res.result_set) == 1

    # drop fulltext index
    res = graph.drop_edge_fulltext_index(rel, attr)
    assert res.indices_deleted == 1

    # list indices
    res = graph.list_indices()
    assert len(res.result_set) == 0

    # ---------------------------------------------------------------------------

    # create edge vector index
    res = graph.create_edge_vector_index(rel, attr)
    assert res.indices_created == 1

    # list indices
    res = graph.list_indices()
    assert len(res.result_set) == 1

    # drop vector index
    res = graph.drop_edge_vector_index(rel, attr)
    assert res.indices_deleted == 1

    # list indices
    res = graph.list_indices()
    assert len(res.result_set) == 0
