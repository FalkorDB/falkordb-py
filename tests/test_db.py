import pytest
from falkordb import FalkorDB


@pytest.fixture
def client(request):
    f = FalkorDB(host='localhost', port=6379)
    return f


def test_config(client):
    db = client
    config_name = "RESULTSET_SIZE"
    config_value = 3

    # set configuration
    response = db.config_set(config_name, config_value)
    assert response == "OK"

    # make sure config been updated
    response = db.config_get(config_name)
    expected_response = [config_name, config_value]
    assert response == expected_response

    config_name = "QUERY_MEM_CAPACITY"
    config_value = 1 << 20  # 1MB

    # set configuration
    response = db.config_set(config_name, config_value)
    assert response == "OK"

    # make sure config been updated
    response = db.config_get(config_name)
    expected_response = [config_name, config_value]
    assert response == expected_response
