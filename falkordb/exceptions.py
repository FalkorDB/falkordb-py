class SchemaVersionMismatchException(Exception):
    """
    """
    def __init__(self, version):
        self.version = version
