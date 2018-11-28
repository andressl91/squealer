import os
import tempfile
import pytest
from squealer.sql_table_tools import DataTable, DataTableTools     
from squealer.sqlite_session import SqliteSession


def test_recreate_sqlite_db():
    # TODO: FIND BUG MAKING THIS TEST FAIL EVERY SECOND RUN
    

    tf = tempfile.mktemp(suffix=".db", prefix="test.db")
    sql_session = SqliteSession(db_path=tf)
    db_tools = DataTableTools(sql_session=sql_session)


    assert list(db_tools.tables.keys()) == []


    categories = {"money": "RREAL", "time": "REAL"}
    # Checks for unvalid SQL data type 
    with pytest.raises(TypeError):
       db_tools.create_table(table_name="data",
                             categories=categories)

    categories = {"money": "REAL", "time": "REAL"}
    db_tools.create_table(table_name="data",
                          categories=categories)

    assert list(db_tools.tables.keys()) == ["data"]

    data_table = db_tools.tables["data"]
    sql_data = {"money": "2000", "time": "10"}
    data_table.write_to_table(sql_data)
    sql_data = {"time": "300", "money": "600"}
    data_table.write_to_table(sql_data)

    res = data_table.select("*")
    print(res[0])

    db_tools.delete_table("data")
    assert list(db_tools.tables.keys()) == []


if __name__ == "__main__":
    test_recreate_sqlite_db()

