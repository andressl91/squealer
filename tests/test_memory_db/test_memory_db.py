
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


def test_initiate_memory_db():
    tf = tempfile.mkdtemp()
    tf = Path(tf) / "mytest.db"
    db_tools = DataTableTools(db_path=tf)

    assert not db_tools.in_memory() 
    assert db_tools.context == "local"

    categories = {"money": "INTEGER", "time": "INTEGER"}
    db_tools.create_table(table_name="data",
                          categories=categories)
    # Create local db with values
    local_tables = db_tools._fetch_all_tables()
    assert "data" in local_tables

    # Change context to memory db and check that data table isn't initiated
    db_tools.set_active_session("memory")
    assert db_tools.context == "memory"
    memory_tables = db_tools._fetch_all_tables()
    assert "data" not in memory_tables

    # Change context to memory db and check that data table isn't initiated
    #db_tools.load_to_memory_stringio()
    db_tools.load_to_memory()
    memory_tables = db_tools._fetch_all_tables()
    assert "data" in memory_tables


def test_load_local_db_to_memory():
    tf = tempfile.mkdtemp()
    tf = Path(tf) / "mytest.db"
    db_tools = DataTableTools(db_path=tf)

    categories = {"money": "INTEGER", "time": "INTEGER"}
    db_tools.create_table(table_name="data",
                          categories=categories)

    sql_data = [{"money": "2000", "time": "10"},
                {"time": "300", "money": "600"}]
    local_table = db_tools.tables["data"]
    local_table.multi_write(sql_data)

    db_tools.set_active_session("memory")
    assert db_tools._fetch_all_tables() == []
    db_tools.load_to_memory()
    assert db_tools._fetch_all_tables() == ["data"]
    
    memory_data_table = db_tools.tables["data"]
    res = memory_data_table.select(["*"])
    assert res[0] == (1, 2000, 10)
    assert res[1] == (2, 600, 300)
    assert len(res) == 2

    memory_data_table = db_tools.tables["data"]
    memory_data_table.multi_write(sql_data)


def test_memory_read_and_write():
    tf = tempfile.mkdtemp()
    tf = Path(tf) / "mytest.db"
    db_tools = DataTableTools(db_path=tf)

    categories = {"money": "INTEGER", "time": "INTEGER"}
    db_tools.set_active_session("memory")

    db_tools.create_table(table_name="data",
                          categories=categories)

    sql_data = [{"money": "2000", "time": "10"},
                {"time": "300", "money": "600"}]
    memory_table = db_tools.tables["data"]
    memory_table.multi_write(sql_data)


    res = memory_table.select(["*"])
    assert res[0] == (1, 2000, 10)
    assert res[1] == (2, 600, 300)
    assert len(res) == 2

    sql_data = [{"money": "6000", "time": "60"},
                {"time": "900", "money": "900"}]
    # BUG, table is now connected to local db
    memory_data_table = db_tools.tables["data"]
    memory_data_table.multi_write(sql_data)

    assert db_tools.get_active_session().db_path == ":memory:"
    
    # Check that local file db is empty
    db_tools.set_active_session("local")
    local_tables = db_tools._fetch_all_tables()

    assert local_tables == []





if __name__ == "__main__":
    test_read_and_write_table()
