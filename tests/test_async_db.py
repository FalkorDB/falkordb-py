import pytest
import asyncio
from falkordb.asyncio import FalkorDB


@pytest.fixture
def client(request):
    return FalkorDB(host='localhost', port=6379)


async def test_config(client):
    db = client
    config_name = "RESULTSET_SIZE"

    # save olf configuration value
    prev_value = int(await db.config_get(config_name))

    # set configuration
    response = await db.config_set(config_name, 3)
    assert response == "OK"

    # make sure config been updated
    new_value = int(await db.config_get(config_name))
    assert new_value == 3

    # restore original value
    response = await db.config_set(config_name, prev_value)
    assert response == "OK"

    # trying to get / set invalid configuration
    with pytest.raises(Exception):
        await db.config_get("none_existing_conf")

    with pytest.raises(Exception):
        await db.config_set("none_existing_conf", 1)

    with pytest.raises(Exception):
        await db.config_set(config_name, "invalid value")

async def test_connect_via_url():
    # make sure we're able to connect via url
    db = FalkorDB.from_url("falkor://localhost:6379")
    g = db.select_graph("g")
    one = (await g.query("RETURN 1")).result_set[0][0]
    assert one == 1
