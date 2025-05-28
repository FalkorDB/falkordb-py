import pytest
from pytest import approx
from redis import ResponseError
from falkordb.asyncio import FalkorDB
from falkordb import Edge, Node, Path, Operation
from redis.asyncio import BlockingConnectionPool


@pytest.mark.asyncio
async def test_graph_creation():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    graph = db.select_graph("async_graph")

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
    result = await graph.query(query)

    person  = result.result_set[0][0]
    visit   = result.result_set[0][1]
    country = result.result_set[0][2]

    assert person == john
    assert visit.properties == edge.properties
    assert country == japan

    # Test vector float32 query result
    query = "CREATE (p:person {name:'Mike', ids: vecf32([1, -2, 3.14])}) RETURN p.ids"
    result = await graph.query(query)
    assert result.result_set[0][0] == approx([1, -2, 3.14])

    query = "RETURN [1, 2.3, '4', true, false, null]"
    result = await graph.query(query)
    assert [1, 2.3, "4", True, False, None] == result.result_set[0][0]

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_array_functions():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    graph = db.select_graph("async_graph")

    await graph.delete()

    query = """RETURN [0,1,2]"""
    result = await graph.query(query)
    assert [0, 1, 2] == result.result_set[0][0]

    a = Node(
        node_id=0,
        labels="person",
        properties={"name": "a", "age": 32, "array": [0, 1, 2]}
    )

    await graph.query(f"CREATE {a}")

    query = "MATCH(n) return collect(n)"
    result = await graph.query(query)

    assert [a] == result.result_set[0][0]

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_path():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    graph = db.select_graph("async_graph")

    await graph.delete()

    node0  = Node(alias="node0", node_id=0, labels="L1")
    node1  = Node(alias="node1", node_id=1, labels="L1")
    edge01 = Edge(node0, "R1", node1, edge_id=0, properties={"value": 1})

    await graph.query(f"CREATE {node0}, {node1}, {edge01}")

    path01 = Path([node0, node1], [edge01])
    expected_results = [[path01]]

    query = "MATCH p=(:L1)-[:R1]->(:L1) RETURN p"
    result = await graph.query(query)
    assert expected_results == result.result_set

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_param():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    graph = db.select_graph("async_graph")

    params = [1, 2.3, "str", True, False, None, [0, 1, 2], r"\" RETURN 1337 //"]
    query = "RETURN $param"
    for param in params:
        result = await graph.query(query, {"param": param})
        expected_results = [[param]]
        assert expected_results == result.result_set

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_map():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_graph")

    await g.delete()

    query = "RETURN {a:1, b:'str', c:NULL, d:[1,2,3], e:True, f:{x:1, y:2}}"
    actual = (await g.query(query)).result_set[0][0]
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
    await g.query(f"CREATE {src}, {dest}, {e}")

    query = "MATCH (src)-[e]->(dest) RETURN {src:src, e:e, dest:dest}"
    actual = (await g.query(query)).result_set[0][0]
    expected = { "src": src, "e": e, "dest": dest }
    assert actual == expected

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_point():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_graph")

    query = "RETURN point({latitude: 32.070794860, longitude: 34.820751118})"
    expected_lat = 32.070794860
    expected_lon = 34.820751118
    actual = (await g.query(query)).result_set[0][0]
    assert abs(actual["latitude"] - expected_lat) < 0.001
    assert abs(actual["longitude"] - expected_lon) < 0.001

    query = "RETURN point({latitude: 32, longitude: 34.0})"
    expected_lat = 32
    expected_lon = 34
    actual = (await g.query(query)).result_set[0][0]
    assert abs(actual["latitude"] - expected_lat) < 0.001
    assert abs(actual["longitude"] - expected_lon) < 0.001

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_index_response():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_graph")

    result_set = await g.query("CREATE INDEX ON :person(age)")
    assert 1 == result_set.indices_created

    with pytest.raises(ResponseError):
        await g.query("CREATE INDEX ON :person(age)")

    result_set = await g.query("DROP INDEX ON :person(age)")
    assert 1 == result_set.indices_deleted

    with pytest.raises(ResponseError):
        await g.query("DROP INDEX ON :person(age)")

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_stringify_query_result():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_graph")

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

    await g.query(f"CREATE {john}, {japan}, {e}")

    query = """MATCH (p:person)-[v:visited {purpose:"pleasure"}]->(c:country)
               RETURN p, v, c"""

    result  = await g.query(query)
    person  = result.result_set[0][0]
    visit   = result.result_set[0][1]
    country = result.result_set[0][2]

    assert (
        str(person)
        == """(:person{age:33,gender:"male",name:"John Doe",status:"single"})"""
    )
    assert str(visit) == """()-[:visited{purpose:"pleasure"}]->()"""
    assert str(country) == """(:country{name:"Japan"})"""

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_optional_match():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_graph")

    await g.delete()

    # build a graph of form (a)-[R]->(b)
    src = Node(alias="src", node_id=0, labels="L1", properties={"value": "a"})
    dest = Node(alias="dest", node_id=1, labels="L1", properties={"value": "b"})

    e = Edge(src, "R", dest, edge_id=0)

    await g.query(f"CREATE {src}, {dest}, {e}")

    # issue a query that collects all outgoing edges from both nodes
    # (the second has none)
    query = """MATCH (a)
               OPTIONAL MATCH (a)-[e]->(b)
               RETURN a, e, b
               ORDER BY a.value"""
    expected_results = [[src, e, dest], [dest, None, None]]

    result = await g.query(query)
    assert expected_results == result.result_set

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_cached_execution():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_graph")

    result = await g.query("RETURN $param", {"param": 0})
    assert result.cached_execution is False

    result = await g.query("RETURN $param", {"param": 0})
    assert result.cached_execution is True

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_slowlog():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_graph")

    await g.delete()

    create_query = """CREATE (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
                             (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
                             (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    await g.query(create_query)

    results = await g.slowlog()
    assert len(results[0]) == 4
    assert results[0][1] == "GRAPH.QUERY"
    assert results[0][2] == create_query

    # close the connection pool
    await pool.aclose()

@pytest.mark.xfail(strict=False)
@pytest.mark.asyncio
async def test_query_timeout():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_graph")

    # build a graph with 1000 nodes
    await g.query("UNWIND range(0, 1000) as val CREATE ({v: val})")
    # issue a long-running query with a 1-millisecond timeout
    with pytest.raises(ResponseError):
        await g.query("MATCH (a), (b), (c), (d) RETURN *", timeout=1)
        assert False is False

    with pytest.raises(Exception):
        await g.query("RETURN 1", timeout="str")
        assert False is False

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_read_only_query():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_graph")

    with pytest.raises(Exception):
        # issue a write query, specifying read-only true
        # call should fail
        await g.query("CREATE ()", read_only=True)
        assert False is False

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_multi_label():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_graph")

    await g.delete()

    node = Node(labels=["l", "ll"])
    await g.query(f"CREATE {node}")

    query = "MATCH (n) RETURN n"
    result = await g.query(query)
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

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_execution_plan():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_graph")

    create_query = """CREATE
                      (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
                      (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
                      (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    await g.query(create_query)

    result = await g.explain(
        """MATCH (r:Rider)-[:rides]->(t:Team)
           WHERE t.name = $name
           RETURN r.name, t.name, $params""", {"name": "Yehuda"}
    )

    expected = "Results\n    Project\n        Conditional Traverse | (t)->(r:Rider)\n            Filter\n                Node By Label Scan | (t:Team)"
    assert str(result) == expected

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_explain():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    g = db.select_graph("async_graph")

    # graph creation / population
    create_query = """CREATE
                      (:Rider {name:'Valentino Rossi'})-[:rides]->(:Team {name:'Yamaha'}),
                      (:Rider {name:'Dani Pedrosa'})-[:rides]->(:Team {name:'Honda'}),
                      (:Rider {name:'Andrea Dovizioso'})-[:rides]->(:Team {name:'Ducati'})"""
    await g.query(create_query)

    result = await g.explain(
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

    result = await g.explain("MATCH (r:Rider), (t:Team) RETURN r.name, t.name")
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

    # close the connection pool
    await pool.aclose()
