import pytest
import tempfile
from squealer.sql_table_tools import DataTableTools
from squealer.sqlite_session import SqliteSession




@pytest.fixture()
def get_lots_of_data():

    def _get_lots_of_data(keys, n_rows):

        sql_data = []
        for i in range(n_rows):
            data = [j for j in range(len(keys))]
            sql_data.append(dict(zip(keys, data)))

        return sql_data

    return _get_lots_of_data
