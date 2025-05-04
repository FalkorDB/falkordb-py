import pytest
from falkordb import FalkorDB


@pytest.fixture
def client(request):
    return FalkorDB(host='localhost', port=6379)


def test_config(client):
    db = client
    config_name = "RESULTSET_SIZE"

    # save olf configuration value
    prev_value = int(db.config_get(config_name))

    # set configuration
    response = db.config_set(config_name, 3)
    assert response == "OK"

    # make sure config been updated
    new_value = int(db.config_get(config_name))
    assert new_value == 3

    # restore original value
    response = db.config_set(config_name, prev_value)
    assert response == "OK"

    # trying to get / set invalid configuration
    with pytest.raises(Exception):
        db.config_get("none_existing_conf")

    with pytest.raises(Exception):
        db.config_set("none_existing_conf", 1)

    with pytest.raises(Exception):
        db.config_set(config_name, "invalid value")

def test_connect_via_url():
    # make sure we're able to connect via url

    # just host
    db = FalkorDB.from_url("falkor://localhost")
    g = db.select_graph("db")
    qr = g.query("RETURN 1")
    one = qr.result_set[0][0]
    qr_g = qr.graph
    assert one == 1
    assert g.name == qr_g.name

    # host & Port
    db = FalkorDB.from_url("falkor://localhost:6379")
    g = db.select_graph("db")
    qr = g.query("RETURN 1")
    one = qr.result_set[0][0]
    header = qr.header
    assert one == 1
    assert header[0][0] == 1
    assert header[0][1] == '1'
