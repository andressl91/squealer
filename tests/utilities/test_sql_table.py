import tempfile
import pytest
from squealer.sql_table_tools import DataTableTools
from squealer.sqlite_session import SqliteSession


# TODO: Replace with pytest fixture
def get_db_tools(db_name:str="test.db"):
    tf = tempfile.mktemp(suffix=".db", prefix=db_name)
    sql_session = SqliteSession(db_path=tf)
    db_tools = DataTableTools(sql_session=sql_session)
    return db_tools


def test_recreate_sqlite_db():
    db_tools = get_db_tools()
    # Check inital registry is empty
    assert list(db_tools.tables.keys()) == []
    categories = {"money": "REAL", "time": "REAL"}
    db_tools.create_table(table_name="data",
                          categories=categories)
    assert list(db_tools.tables.keys()) == ["data"]
    db_tools.delete_table("data")
    assert list(db_tools.tables.keys()) == []


def test_create_table_uniqe_key():
    db_tools = get_db_tools()


    categories = {"money": "REAL", "time": "REAL"}
    db_tools.create_table(table_name="data",
                         categories=categories,
                         primary_key="money")
    data_table = db_tools.tables["data"]
    data_table.write_to_table({"money": 2000, "time": 10})

    print(data_table.select("*"))


def test_read_and_write_table():
    db_tools = get_db_tools()
    not_valid_categories = {"money": "RREAL", "time": "REAL"}
    # Checks for unvalid SQL data type
    with pytest.raises(TypeError):
        db_tools.create_table(table_name="data",
                              categories=not_valid_categories)

    categories = {"money": "INTEGER", "time": "INTEGER"}
    db_tools.create_table(table_name="data",
                          categories=categories)

    # Test write, and order of write features is arbritary
    data_table = db_tools.tables["data"]
    sql_data = {"money": "2000", "time": "10"}
    data_table.write_to_table(sql_data)
    sql_data = {"time": "300", "money": "600"}
    data_table.write_to_table(sql_data)

    res = data_table.select("*")
    assert res[0] == (1, 2000, 10)
    assert res[1] == (2, 600, 300)

    time_res = data_table.select("time")
    assert time_res[0] == (10, )
    assert time_res[1] == (300, )


if __name__ == "__main__":
    test_create_table_uniqe_key()
