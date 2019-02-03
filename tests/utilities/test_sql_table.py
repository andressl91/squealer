import tempfile
import sqlite3
import pytest
from pathlib import Path
from squealer.sql_table_tools import DataTableTools

def get_data_table_tool(db_name="test"):
    tf = tempfile.TemporaryFile(suffix=".db", prefix=db_name)
    tf = str(tf)
    db_tools = DataTableTools(db_path=tf)
    return db_tools

def test_recreate_sqlite_db():

    tf = tempfile.mkdtemp()
    tf = Path(tf) / "test.db"
    db_tools = DataTableTools(db_path=tf)

    # Check inital registry is empty
    assert list(db_tools.tables.keys()) == []
    categories = {"money": "REAL", "time": "REAL"}
    db_tools.create_table(table_name="data",
                          categories=categories)
    assert list(db_tools.tables.keys()) == ["data"]
    db_tools.delete_table("data")
    assert list(db_tools.tables.keys()) == []


def test_create_table_uniqe_key(get_db_tools):

    tf = tempfile.mkdtemp()
    tf = Path(tf) / "test.db"
    db_tools = DataTableTools(db_path=tf)
    categories = {"money": "REAL", "time": "REAL PRIMARY KEY"}
    db_tools.create_table(table_name="data",
                          categories=categories,
                          primary_key_id=False)

    data_table = db_tools.tables["data"]
    data_table.write({"money": 2000, "time": 10})
    data_table.write({"time": 33, "money": 22})

    res = data_table.select(["*"])
    assert res[0] == (2000, 10)
    with pytest.raises(sqlite3.IntegrityError):
        data_table.write({"time": 33, "money": 22})


def test_read_and_write_table():

    tf = tempfile.mkdtemp()
    tf = Path(tf) / "test.db"
    db_tools = DataTableTools(db_path=tf)

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
    sql_data = [{"money": "2000", "time": "10"},
                {"time": "300", "money": "600"}]

    for data in sql_data:
        data_table.write(data)

    res = data_table.select(["*"])
    assert res[0] == (1, 2000, 10)
    assert res[1] == (2, 600, 300)

    time_res = data_table.select(["time"])
    assert time_res[0] == (10, )
    assert time_res[1] == (300, )

    sql_data = [{"money": "2000", "time": "10"},
                {"time": "300", "money": "600"}]
    data_table.multi_write(sql_data)
    res = data_table.select(["*"])
    # TODO: Multi write should support random order of dict.keys
    # only write suppoerts this for now
    # Keep separate due to loots of checks for many rows
    assert res[2] == (3, 2000, 10)
    assert res[3] == (4, 600, 300)

    res = data_table.select(["time, money"])
    print(res)


def test_memory_read_and_write():
    tf = tempfile.mkdtemp()
    tf = Path(tf) / "test.db"
    db_tools = DataTableTools(db_path=tf)

    assert not db_tools.in_memory() 

    db_tools.load_to_memory()
    

if __name__ == "__main__":
    test_read_and_write_table()
