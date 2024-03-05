import pytest
from redis import ResponseError
from falkordb import FalkorDB, Edge, Node, Path, Operation


@pytest.fixture
def client(request):
    db = FalkorDB(host='localhost', port=6379)
    db.flushdb()
    return db.select_graph("graph")

def test_graph_creation(client):
    graph = client

    john = Node(
        alias="p",
        labels="person",
        properties={
            "name": "John Doe",
            "age": 33,
            "gender": "male",
            "status": "single",
        },
    )

    japan = Node(alias="c", labels="country", properties={"name": "Japan"})

    edge = Edge(john, "visited", japan, alias="v", properties={"purpose": "pleasure"})

    query = f"CREATE {john}, {japan}, {edge} RETURN p,v,c"
    result = graph.query(query)

    person  = result.result_set[0][0]
    visit   = result.result_set[0][1]
    country = result.result_set[0][2]

    assert person == john
    assert visit.properties == edge.properties
    assert country == japan

    query = """RETURN [1, 2.3, "4", true, false, null]"""
    result = graph.query(query)
    assert [1, 2.3, "4", True, False, None] == result.result_set[0][0]

    # all done, remove graph
    graph.delete()


def test_array_functions(client):
    graph = client
    query = """RETURN [0,1,2]"""
    result = graph.query(query)
    assert [0, 1, 2] == result.result_set[0][0]

    a = Node(
        node_id=0,
        labels="person",
        properties={"name": "a", "age": 32, "array": [0, 1, 2]}
    )

    graph.query(f"CREATE {a}")

    query = "MATCH(n) return collect(n)"
    result = graph.query(query)

    assert [a] == result.result_set[0][0]


def test_path(client):
    graph  = client
    node0  = Node(alias="node0", node_id=0, labels="L1")
    node1  = Node(alias="node1", node_id=1, labels="L1")
    edge01 = Edge(node0, "R1", node1, edge_id=0, properties={"value": 1})

    graph.query(f"CREATE {node0}, {node1}, {edge01}")

    path01 = Path([node0, node1], [edge01])
    expected_results = [[path01]]

    query = "MATCH p=(:L1)-[:R1]->(:L1) RETURN p"
    result = graph.query(query)
    assert expected_results == result.result_set


def test_param(client):
    graph = client
    params = [1, 2.3, "str", True, False, None, [0, 1, 2], r"\" RETURN 1337 //"]
    query = "RETURN $param"
    for param in params:
        result = graph.query(query, {"param": param})
        expected_results = [[param]]
        assert expected_results == result.result_set


def test_map(client):
    g = client

    query = "RETURN {a:1, b:'str', c:NULL, d:[1,2,3], e:True, f:{x:1, y:2}}"
    actual = g.query(query).result_set[0][0]
    expected = {
        "a": 1,
        "b": "str",
        "c": None,
        "d": [1, 2, 3],
        "e": True,
        "f": {"x": 1, "y": 2},
    }

    assert actual == expected

    src  = Node(alias="src", node_id=0, labels="L1", properties={"v": 0})
    dest = Node(alias="dest", node_id=1, labels="L2", properties={"v":2})
    e    = Edge(src, "R1", dest, edge_id=0, properties={"value": 1})
    g.query(f"CREATE {src}, {dest}, {e}")

    query = "MATCH (src)-[e]->(dest) RETURN {src:src, e:e, dest:dest}"
    actual = g.query(query).result_set[0][0]
    expected = { "src": src, "e": e, "dest": dest }
    assert actual == expected

def test_point(client):
    g = client
    query = "RETURN point({latitude: 32.070794860, longitude: 34.820751118})"
    expected_lat = 32.070794860
    expected_lon = 34.820751118
    actual = g.query(query).result_set[0][0]
    assert abs(actual["latitude"] - expected_lat) < 0.001
    assert abs(actual["longitude"] - expected_lon) < 0.001

    query = "RETURN point({latitude: 32, longitude: 34.0})"
    expected_lat = 32
    expected_lon = 34
    actual = g.query(query).result_set[0][0]
    assert abs(actual["latitude"] - expected_lat) < 0.001
    assert abs(actual["longitude"] - expected_lon) < 0.001


def test_index_response(client):
    g = client
    result_set = g.query("CREATE INDEX ON :person(age)")
    assert 1 == result_set.indices_created

    with pytest.raises(ResponseError):
        g.query("CREATE INDEX ON :person(age)")

    result_set = g.query("DROP INDEX ON :person(age)")
    assert 1 == result_set.indices_deleted

    with pytest.raises(ResponseError):
        g.query("DROP INDEX ON :person(age)")


def test_stringify_query_result(client):
    g = client

    john = Node(alias="a", labels="person",
                properties={ "name": "John Doe", "age": 33, "gender": "male",
                            "status": "single", })
    japan = Node(alias="b", labels="country", properties={"name": "Japan"})

    e = Edge(john, "visited", japan, properties={"purpose": "pleasure"})

    assert (
        str(john)
        == """(a:person{age:33,gender:"male",name:"John Doe",status:"single"})"""
    )
    assert str(e) == """(a)-[:visited{purpose:"pleasure"}]->(b)"""
    assert str(japan) == """(b:country{name:"Japan"})"""

    g.query(f"CREATE {john}, {japan}, {e}")

    query = """MATCH (p:person)-[v:visited {purpose:"pleasure"}]->(c:country)
               RETURN p, v, c"""

    result  = g.query(query)
    person  = result.result_set[0][0]
    visit   = result.result_set[0][1]
    country = result.result_set[0][2]

    assert (
        str(person)
        == """(:person{age:33,gender:"male",name:"John Doe",status:"single"})"""
    )
    assert str(visit) == """()-[:visited{purpose:"pleasure"}]->()"""
    assert str(country) == """(:country{name:"Japan"})"""

    g.delete()


def test_optional_match(client):
    # build a graph of form (a)-[R]->(b)
    src = Node(alias="src", node_id=0, labels="L1", properties={"value": "a"})
    dest = Node(alias="dest", node_id=1, labels="L1", properties={"value": "b"})

    e = Edge(src, "R", dest, edge_id=0)

    g = client
    g.query(f"CREATE {src}, {dest}, {e}")

    # issue a query that collects all outgoing edges from both nodes
    # (the second has none)
    query = """MATCH (a)
               OPTIONAL MATCH (a)-[e]->(b)
               RETURN a, e, b
               ORDER BY a.value"""
    expected_results = [[src, e, dest], [dest, None, None]]

    result = g.query(query)
    assert expected_results == result.result_set

    g.delete()


def test_cached_execution(client):
    g = client

    result = g.query("RETURN $param", {"param": 0})
    assert result.cached_execution is False

    result = g.query("RETURN $param", {"param": 0})
    assert result.cached_execution is True


def test_slowlog(client):
    g = client
    create_query = """CREATE (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
                             (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
                             (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    g.query(create_query)

    results = g.slowlog()
    assert len(results[0]) == 4
    assert results[0][1] == "GRAPH.QUERY"
    assert results[0][2] == create_query


@pytest.mark.xfail(strict=False)
def test_query_timeout(client):
    g = client
    # build a graph with 1000 nodes
    g.query("UNWIND range(0, 1000) as val CREATE ({v: val})")
    # issue a long-running query with a 1-millisecond timeout
    with pytest.raises(ResponseError):
        g.query("MATCH (a), (b), (c), (d) RETURN *", timeout=1)
        assert False is False

    with pytest.raises(Exception):
        g.query("RETURN 1", timeout="str")
        assert False is False


def test_read_only_query(client):
    g = client
    with pytest.raises(Exception):
        # issue a write query, specifying read-only true
        # call should fail
        g.query("CREATE ()", read_only=True)
        assert False is False


def _test_list_keys(client):
    g = client
    result = g.list_keys()
    assert result == []

    client.graph("G").query("RETURN 1")
    result = client.graph().list_keys()
    assert result == ["G"]

    client.graph("X").query("RETURN 1")
    result = client.graph().list_keys()
    assert result == ["G", "X"]

    client.delete("G")
    client.rename("X", "Z")
    result = client.graph().list_keys()
    assert result == ["Z"]

    client.delete("Z")
    result = client.graph().list_keys()
    assert result == []


def test_multi_label(client):
    g = client

    node = Node(labels=["l", "ll"])
    g.query(f"CREATE {node}")

    query = "MATCH (n) RETURN n"
    result = g.query(query)
    result_node = result.result_set[0][0]
    assert result_node == node

    try:
        Node(labels=1)
        assert False
    except AssertionError:
        assert True

    try:
        Node(labels=["l", 1])
        assert False
    except AssertionError:
        assert True


def test_cache_sync(client):
    pass
    return
    # This test verifies that client internal graph schema cache stays
    # in sync with the graph schema
    #
    # Client B will try to get Client A out of sync by:
    # 1. deleting the graph
    # 2. reconstructing the graph in a different order, this will casuse
    #    a differance in the current mapping between string IDs and the
    #    mapping Client A is aware of
    #
    # Client A should pick up on the changes by comparing graph versions
    # and resyncing its cache.

    A = client.graph("cache-sync")
    B = client.graph("cache-sync")

    # Build order:
    # 1. introduce label 'L' and 'K'
    # 2. introduce attribute 'x' and 'q'
    # 3. introduce relationship-type 'R' and 'S'

    A.query("CREATE (:L)")
    B.query("CREATE (:K)")
    A.query("MATCH (n) SET n.x = 1")
    B.query("MATCH (n) SET n.q = 1")
    A.query("MATCH (n) CREATE (n)-[:R]->()")
    B.query("MATCH (n) CREATE (n)-[:S]->()")

    # Cause client A to populate its cache
    A.query("MATCH (n)-[e]->() RETURN n, e")

    assert len(A._labels) == 2
    assert len(A._properties) == 2
    assert len(A._relationship_types) == 2
    assert A._labels[0] == "L"
    assert A._labels[1] == "K"
    assert A._properties[0] == "x"
    assert A._properties[1] == "q"
    assert A._relationship_types[0] == "R"
    assert A._relationship_types[1] == "S"

    # Have client B reconstruct the graph in a different order.
    B.delete()

    # Build order:
    # 1. introduce relationship-type 'R'
    # 2. introduce label 'L'
    # 3. introduce attribute 'x'
    B.query("CREATE ()-[:S]->()")
    B.query("CREATE ()-[:R]->()")
    B.query("CREATE (:K)")
    B.query("CREATE (:L)")
    B.query("MATCH (n) SET n.q = 1")
    B.query("MATCH (n) SET n.x = 1")

    # A's internal cached mapping is now out of sync
    # issue a query and make sure A's cache is synced.
    A.query("MATCH (n)-[e]->() RETURN n, e")

    assert len(A._labels) == 2
    assert len(A._properties) == 2
    assert len(A._relationship_types) == 2
    assert A._labels[0] == "K"
    assert A._labels[1] == "L"
    assert A._properties[0] == "q"
    assert A._properties[1] == "x"
    assert A._relationship_types[0] == "S"
    assert A._relationship_types[1] == "R"


def test_execution_plan(client):
    g = client
    create_query = """CREATE
                      (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
                      (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
                      (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    g.query(create_query)

    result = g.explain(
        """MATCH (r:Rider)-[:rides]->(t:Team)
           WHERE t.name = $name
           RETURN r.name, t.name, $params""", {"name": "Yehuda"}
    )

    expected = "Results\n    Project\n        Conditional Traverse | (t)->(r:Rider)\n            Filter\n                Node By Label Scan | (t:Team)"
    assert str(result) == expected

    g.delete()


def test_explain(client):
    g = client
    # graph creation / population
    create_query = """CREATE
                      (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
                      (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
                      (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    g.query(create_query)

    result = g.explain(
        """MATCH (r:Rider)-[:rides]->(t:Team)
           WHERE t.name = $name
           RETURN r.name, t.name
           UNION
           MATCH (r:Rider)-[:rides]->(t:Team)
           WHERE t.name = $name
           RETURN r.name, t.name""",
        {"name": "Yamaha"},
    )
    expected = """\
Results
Distinct
    Join
        Project
            Conditional Traverse | (t)->(r:Rider)
                Filter
                    Node By Label Scan | (t:Team)
        Project
            Conditional Traverse | (t)->(r:Rider)
                Filter
                    Node By Label Scan | (t:Team)"""
    assert str(result).replace(" ", "").replace("\n", "") == expected.replace(
        " ", ""
    ).replace("\n", "")

    expected = Operation("Results").append_child(
        Operation("Distinct").append_child(
            Operation("Join")
            .append_child(
                Operation("Project").append_child(
                    Operation("Conditional Traverse", "(t)->(r:Rider)").append_child(
                        Operation("Filter").append_child(
                            Operation("Node By Label Scan", "(t:Team)")
                        )
                    )
                )
            )
            .append_child(
                Operation("Project").append_child(
                    Operation("Conditional Traverse", "(t)->(r:Rider)").append_child(
                        Operation("Filter").append_child(
                            Operation("Node By Label Scan", "(t:Team)")
                        )
                    )
                )
            )
        )
    )

    assert result.structured_plan == expected

    result = g.explain("MATCH (r:Rider), (t:Team) RETURN r.name, t.name")
    expected = """\
Results
Project
    Cartesian Product
        Node By Label Scan | (r:Rider)
        Node By Label Scan | (t:Team)"""
    assert str(result).replace(" ", "").replace("\n", "") == expected.replace(
        " ", ""
    ).replace("\n", "")

    expected = Operation("Results").append_child(
        Operation("Project").append_child(
            Operation("Cartesian Product")
            .append_child(Operation("Node By Label Scan"))
            .append_child(Operation("Node By Label Scan"))
        )
    )

    assert result.structured_plan == expected

    g.delete()
