import pytest
import redis.exceptions

from falkordb import FalkorDB


@pytest.fixture
def client(request):
    return FalkorDB(host="localhost", port=6379)


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
    assert one == 1

    # host & Port
    db = FalkorDB.from_url("falkor://localhost:6379")
    g = db.select_graph("db")
    qr = g.query("RETURN 1")
    one = qr.result_set[0][0]
    header = qr.header
    assert one == 1
    assert header[0][0] == 1
    assert header[0][1] == "1"


def test_from_url():
    """Test that from_url uses the correct host/port from URL"""
    # Test basic connection with just host
    db = FalkorDB.from_url("falkor://localhost")
    g = db.select_graph("db")
    one = g.query("RETURN 1").result_set[0][0]
    assert one == 1
    db.close()

    # Test connection with host and port
    db = FalkorDB.from_url("falkor://localhost:6379")
    g = db.select_graph("db")
    qr = g.query("RETURN 1")
    one = qr.result_set[0][0]
    header = qr.header
    assert one == 1
    assert header[0][0] == 1
    assert header[0][1] == "1"
    db.close()

    # Test SSL URL parsing (falkors:// scheme)
    with pytest.raises(
        (redis.exceptions.ConnectionError, ConnectionRefusedError, OSError)
    ) as exc_info:
        FalkorDB.from_url("falkors://nonexistent-ssl.example.com:6380")
    error_str = str(exc_info.value)
    assert "nonexistent-ssl.example.com" in error_str or "6380" in error_str, (
        f"Error should mention SSL host: {error_str}"
    )

    # Test that from_url fails with correct host when connecting
    # to non-existent host (not localhost)
    with pytest.raises(
        (redis.exceptions.ConnectionError, ConnectionRefusedError, OSError)
    ) as exc_info:
        FalkorDB.from_url("falkor://nonexistent.example.com:1234")
    error_str = str(exc_info.value)
    assert "nonexistent.example.com" in error_str or "1234" in error_str, (
        f"Error should mention correct host: {error_str}"
    )
    assert "localhost" not in error_str, (
        f"Error should not mention localhost: {error_str}"
    )


def test_udf_load(client):
    """Test loading a UDF library"""
    db = client

    # Ensure clean state
    db.udf_flush()

    # Define a simple UDF script with proper registration
    udf_script = """
    function my_add(x, y) {
        return x + y;
    }

    falkor.register('my_add', my_add);
    """

    # Load the UDF
    result = db.udf_load("testlib", udf_script)
    assert result == "OK"

    # Verify the UDF was loaded
    udfs = db.udf_list()
    assert len(udfs) == 1
    assert udfs[0][1] == "testlib"

    # Verify the function is registered
    assert udfs[0][3] == ["my_add"]

    # Call the loaded UDF in a query to verify it works
    graph = db.select_graph("test_udf_graph")
    query_result = graph.query("RETURN testlib.my_add(5, 3) AS result")
    assert query_result.result_set[0][0] == 8

    # Test replacing a UDF
    new_script = """
    function my_multiply(x, y) {
        return x * y;
    }

    falkor.register('my_multiply', my_multiply);
    """
    result = db.udf_load("testlib", new_script, replace=True)
    assert result == "OK"

    # Verify the replaced UDF works
    query_result = graph.query("RETURN testlib.my_multiply(5, 3) AS result")
    assert query_result.result_set[0][0] == 15

    # Clean up
    db.udf_flush()


def test_udf_list(client):
    """Test listing UDF libraries"""
    db = client

    # Ensure clean state
    db.udf_flush()

    # Initially, no UDFs should exist
    udfs = db.udf_list()
    assert udfs == []

    # Load a UDF
    udf_script = """
    function test_func() {
        return 42;
    }
    """
    db.udf_load("lib1", udf_script)

    # List all UDFs
    udfs = db.udf_list()
    assert len(udfs) == 1
    assert udfs[0][1] == "lib1"

    # Load another UDF
    db.udf_load("lib2", udf_script)
    udfs = db.udf_list()
    assert len(udfs) == 2

    # List specific UDF
    specific_udf = db.udf_list("lib1")
    assert len(specific_udf) == 1
    assert specific_udf[0][1] == "lib1"

    # List with code
    udf_with_code = db.udf_list("lib1", with_code=True)
    assert len(udf_with_code) == 1
    assert udf_with_code[0][1] == "lib1"
    # The response should contain the code
    assert len(udf_with_code[0]) > 2

    # Clean up
    db.udf_flush()


def test_udf_delete(client):
    """Test deleting a specific UDF library"""
    db = client

    # Ensure clean state
    db.udf_flush()

    # Load two UDFs
    udf_script = """
    function test_func() {
        return 1;
    }
    """
    db.udf_load("lib1", udf_script)
    db.udf_load("lib2", udf_script)

    # Verify both are loaded
    udfs = db.udf_list()
    assert len(udfs) == 2

    # Delete one UDF
    result = db.udf_delete("lib1")
    assert result == "OK"

    # Verify only one remains
    udfs = db.udf_list()
    assert len(udfs) == 1
    assert udfs[0][1] == "lib2"

    # Clean up
    db.udf_flush()


def test_udf_flush(client):
    """Test flushing all UDF libraries"""
    db = client

    # Load multiple UDFs
    udf_script = """
    function test_func() {
        return 1;
    }
    """
    db.udf_load("lib1", udf_script)
    db.udf_load("lib2", udf_script)
    db.udf_load("lib3", udf_script)

    # Verify they are loaded
    udfs = db.udf_list()
    assert len(udfs) == 3

    # Flush all UDFs
    result = db.udf_flush()
    assert result == "OK"

    # Verify all UDFs are removed
    udfs = db.udf_list()
    assert udfs == []
