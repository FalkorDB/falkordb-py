from falkordb.exceptions import SchemaVersionMismatchException


def test_schema_version_mismatch_exception():
    """Test SchemaVersionMismatchException initialization and attributes."""
    version = 5
    exception = SchemaVersionMismatchException(version)
    
    assert exception.version == version
    assert isinstance(exception, Exception)


def test_schema_version_mismatch_exception_different_versions():
    """Test SchemaVersionMismatchException with different version values."""
    exception1 = SchemaVersionMismatchException(1)
    exception2 = SchemaVersionMismatchException(100)
    exception3 = SchemaVersionMismatchException(0)
    
    assert exception1.version == 1
    assert exception2.version == 100
    assert exception3.version == 0
