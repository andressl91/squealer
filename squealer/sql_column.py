import sqlite3
import pytest
from typing import Dict, List
from enum import Enum
from io import StringIO

from squealer.sqlite_session import SqlSession, SqliteSession
from squealer.sql_datatypes import SqlDataType


class Column:

    def __init__(self, sql_session, column_name: str, data_type:str ):

        """Base class for a sql column.

       Parameters:
           sql_session: Current sqlsession
           column_name: Name of column
           data_type: SqlDataType

        """
        self._sql_session = sql_session
        self._column_name = column_name
        self._data_type = data_type
        self.check_valid_data_type(data_type)


    def check_valid_data_type(self, data_type):
        if data_type not in SqlDataType.data_types():
            raise RuntimeError("Invalid data type")
