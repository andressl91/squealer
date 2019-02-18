from enum import Enum


class SqlDataType(Enum):
    """Declare all valid Sqlite data types"""
    null = "NULL"
    integer = "INTEGER"
    real = "REAL"
    text = "TEXT"
    blob = "BLOB"

    @classmethod
    def data_types(cls):
        """Return list of all sqlite data types"""
        return [data_type.value for data_type in cls]