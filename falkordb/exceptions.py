class SchemaVersionMismatchException(Exception):
    """
    Exception raised when the schema version of the database does not match the
    version of the schema that the application expects.
    """
    def __init__(self, version: int):
        """
        Create a new SchemaVersionMismatchException.

        Args:
            version: The version of the schema that the application expects.

        """

        self.version = version
