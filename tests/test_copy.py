import pytest
from falkordb import FalkorDB

def test_graph_copy():
    # create a simple graph and clone it
    # make sure graphs are the same

    db = FalkorDB(host='localhost', port=6379)
    src = db.select_graph("copy_src")

    # create entities
    src.query("CREATE (:A {v:1})-[:R {v:2}]->(:B {v:3})")

    # create index
    src.create_edge_range_index("A", "v")
    src.create_edge_range_index("R", "v")
    src.create_node_fulltext_index("B", "v")

    # create constrain
    src.create_node_unique_constraint("A", "v")
    src.create_edge_unique_constraint("R", "v")

    # clone graph
    dest = src.copy("copy_dest")

    # validate src and dest are the same
    # validate entities
    q = "MATCH (a) RETURN a ORDER BY ID(a)"
    src_res  = src.query(q).result_set
    dest_res = dest.query(q).result_set
    assert(src_res == dest_res)

    q = "MATCH ()-[e]->() RETURN e ORDER BY ID(e)"
    src_res  = src.query(q).result_set
    dest_res = dest.query(q).result_set
    assert(src_res == dest_res)

    # validate schema
    src_res  = src.call_procedure("DB.LABELS").result_set
    dest_res = dest.call_procedure("DB.LABELS").result_set
    assert(src_res == dest_res)

    src_res  = src.call_procedure("DB.PROPERTYKEYS").result_set
    dest_res = dest.call_procedure("DB.PROPERTYKEYS").result_set
    assert(src_res == dest_res)

    src_res  = src.call_procedure("DB.RELATIONSHIPTYPES").result_set
    dest_res = dest.call_procedure("DB.RELATIONSHIPTYPES").result_set
    assert(src_res == dest_res)

    # validate indices
    q = """CALL DB.INDEXES()
           YIELD label, properties, types, language, stopwords, entitytype, status
           RETURN *
           ORDER BY label, properties, types, language, stopwords, entitytype, status"""
    src_res = src.query(q).result_set
    dest_res = dest.query(q).result_set

    assert(src_res == dest_res)

    # validate constraints
    src_res = src.list_constraints()
    dest_res = dest.list_constraints()
    assert(src_res == dest_res)
