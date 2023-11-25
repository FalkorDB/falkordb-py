import pytest
from falkordb import DB


@pytest.fixture
def client(request):
    return DB(host='localhost', port=6379)


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
