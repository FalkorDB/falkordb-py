import pytest
import asyncio
from falkordb.asyncio import FalkorDB
from redis.asyncio import BlockingConnectionPool
import redis.exceptions


@pytest.fixture
async def async_client():
    """Fixture to provide an async FalkorDB client with connection pool."""
    pool = BlockingConnectionPool(max_connections=16, timeout=None, decode_responses=True)
    db = FalkorDB(connection_pool=pool)
    yield db
    await pool.aclose()


@pytest.mark.asyncio
async def test_config(async_client):
    db = async_client
    config_name = "RESULTSET_SIZE"

    # save old configuration value
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

@pytest.mark.asyncio
async def test_connect_via_url(async_client):
    db = async_client

    # make sure we're able to connect via url
    g = db.select_graph("async_db")
    one = (await g.query("RETURN 1")).result_set[0][0]
    assert one == 1

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

@pytest.mark.asyncio
async def test_udf_load(async_client):
    """Test loading a UDF library asynchronously"""
    db = async_client
    
    # Ensure clean state
    await db.udf_flush()
    
    # Define a simple UDF script with proper registration
    udf_script = """
    function my_add(x, y) {
        return x + y;
    }
    
    falkor.register('my_add', my_add);
    """
    
    # Load the UDF
    result = await db.udf_load("testlib", udf_script)
    assert result == "OK"
    
    # Verify the UDF was loaded
    udfs = await db.udf_list()
    assert len(udfs) == 1
    assert udfs[0][1] == "testlib"
    
    # Verify the function is registered
    assert udfs[0][3] == ['my_add']
    
    # Call the loaded UDF in a query to verify it works
    graph = db.select_graph("test_udf_graph")
    query_result = await graph.query("RETURN testlib.my_add(5, 3) AS result")
    assert query_result.result_set[0][0] == 8
    
    # Test replacing a UDF
    new_script = """
    function my_multiply(x, y) {
        return x * y;
    }
    
    falkor.register('my_multiply', my_multiply);
    """
    result = await db.udf_load("testlib", new_script, replace=True)
    assert result == "OK"
    
    # Verify the replaced UDF works
    query_result = await graph.query("RETURN testlib.my_multiply(5, 3) AS result")
    assert query_result.result_set[0][0] == 15
    
    # Clean up
    await db.udf_flush()


@pytest.mark.asyncio
async def test_udf_list(async_client):
    """Test listing UDF libraries asynchronously"""
    db = async_client
    
    # Ensure clean state
    await db.udf_flush()
    
    # Initially, no UDFs should exist
    udfs = await db.udf_list()
    assert udfs == []
    
    # Load a UDF
    udf_script = """
    function test_func() {
        return 42;
    }
    """
    await db.udf_load("lib1", udf_script)
    
    # List all UDFs
    udfs = await db.udf_list()
    assert len(udfs) == 1
    assert udfs[0][1] == "lib1"
    
    # Load another UDF
    await db.udf_load("lib2", udf_script)
    udfs = await db.udf_list()
    assert len(udfs) == 2
    
    # List specific UDF
    specific_udf = await db.udf_list("lib1")
    assert len(specific_udf) == 1
    assert specific_udf[0][1] == "lib1"
    
    # List with code
    udf_with_code = await db.udf_list("lib1", with_code=True)
    assert len(udf_with_code) == 1
    assert udf_with_code[0][1] == "lib1"
    # The response should contain the code
    assert len(udf_with_code[0]) > 2
    
    # Clean up
    await db.udf_flush()


@pytest.mark.asyncio
async def test_udf_delete(async_client):
    """Test deleting a specific UDF library asynchronously"""
    db = async_client
    
    # Ensure clean state
    await db.udf_flush()
    
    # Load two UDFs
    udf_script = """
    function test_func() {
        return 1;
    }
    """
    await db.udf_load("lib1", udf_script)
    await db.udf_load("lib2", udf_script)
    
    # Verify both are loaded
    udfs = await db.udf_list()
    assert len(udfs) == 2
    
    # Delete one UDF
    result = await db.udf_delete("lib1")
    assert result == "OK"
    
    # Verify only one remains
    udfs = await db.udf_list()
    assert len(udfs) == 1
    assert udfs[0][1] == "lib2"
    
    # Clean up
    await db.udf_flush()


@pytest.mark.asyncio
async def test_udf_flush(async_client):
    """Test flushing all UDF libraries asynchronously"""
    db = async_client
    
    # Load multiple UDFs
    udf_script = """
    function test_func() {
        return 1;
    }
    """
    await db.udf_load("lib1", udf_script)
    await db.udf_load("lib2", udf_script)
    await db.udf_load("lib3", udf_script)
    
    # Verify they are loaded
    udfs = await db.udf_list()
    assert len(udfs) == 3
    
    # Flush all UDFs
    result = await db.udf_flush()
    assert result == "OK"
    
    # Verify all UDFs are removed
    udfs = await db.udf_list()
    assert udfs == []