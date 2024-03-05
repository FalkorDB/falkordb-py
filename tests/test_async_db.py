import pytest
import asyncio
from falkordb.asyncio import FalkorDB
from redis.asyncio import BlockingConnectionPool

@pytest.mark.asyncio
async def test_config():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
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

    # close the connection pool
    await pool.aclose()

@pytest.mark.asyncio
async def test_connect_via_url():
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)

    # make sure we're able to connect via url
    g = db.select_graph("async_db")
    one = (await g.query("RETURN 1")).result_set[0][0]
    assert one == 1

    # close the connection pool
    await pool.aclose()
