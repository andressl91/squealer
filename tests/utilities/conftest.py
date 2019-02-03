import pytest
from pathlib import Path
import tempfile
from squealer.sql_table_tools import DataTableTools
from squealer.sqlite_session import SqliteSession


@pytest.fixture()
def get_db_tools(db_name: str="test.db"):
    td = tempfile.mkdtemp()
    tf = Path(td) / "test.db"
    tf = str(tf)

    db_tools = DataTableTools(db_path=tf)
    tf = tempfile.TemporaryFile(suffix=".db", prefix=db_name)
    tf = str(tf)
    db_tools = DataTableTools(db_path=tf)
    return db_tools


@pytest.fixture()
def get_lots_of_data():

    def _get_lots_of_data(keys, n_rows):

        sql_data = []
        for i in range(n_rows):
            data = [j for j in range(len(keys))]
            sql_data.append(dict(zip(keys, data)))

        return sql_data

    return _get_lots_of_data
