import pytest
import asyncio
from falkordb.asyncio import FalkorDB
from redis.asyncio import BlockingConnectionPool
import redis.exceptions

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


@pytest.mark.asyncio
async def test_from_url():
    """Test that from_url uses the correct host/port from URL"""
    # Test basic connection with just host
    db = FalkorDB.from_url("falkor://localhost")
    g = db.select_graph("async_db")
    one = (await g.query("RETURN 1")).result_set[0][0]
    assert one == 1
    await db.connection.aclose()
    
    # Test connection with host and port
    db = FalkorDB.from_url("falkor://localhost:6379")
    g = db.select_graph("async_db")
    qr = await g.query("RETURN 1")
    one = qr.result_set[0][0]
    header = qr.header
    assert one == 1
    assert header[0][0] == 1
    assert header[0][1] == '1'
    await db.connection.aclose()
    
    # Test SSL URL parsing (falkors:// scheme)
    # We can't test actual SSL connection without a proper SSL server,
    # but we can verify the URL is parsed and SSL flag is set
    with pytest.raises((redis.exceptions.ConnectionError, ConnectionRefusedError, OSError)) as exc_info:
        db_ssl = FalkorDB.from_url("falkors://nonexistent-ssl.example.com:6380")
    # Verify it tried to connect to the SSL host (not localhost)
    error_str = str(exc_info.value)
    assert "nonexistent-ssl.example.com" in error_str or "6380" in error_str, f"Error should mention SSL host: {error_str}"
    
    # Test that from_url fails with correct host when connecting to non-existent host
    # This verifies that the URL parsing works and connects to the right host (not localhost)
    with pytest.raises((redis.exceptions.ConnectionError, ConnectionRefusedError, OSError)) as exc_info:
        db_bad = FalkorDB.from_url("falkor://nonexistent.example.com:1234")
    # The error should mention the correct host, not localhost
    error_str = str(exc_info.value)
    assert "nonexistent.example.com" in error_str or "1234" in error_str, f"Error should mention correct host: {error_str}"
    assert "localhost" not in error_str, f"Error should not mention localhost: {error_str}"
