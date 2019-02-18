import tempfile
import sqlite3
import pytest
from pathlib import Path
from squealer.sql_database import DataBase
from squealer.sql_column import Column


def test_table_stuff():
    tf = tempfile.mkdtemp()
    tf = Path(tf) / "test.db"
    db_tools = DataBase(db_path=tf)

    categories = {"money": "INTEGER", "time": "INTEGER"}
    db_tools.create_table(table_name="data",
                          categories=categories)

    data_table = db_tools.tables["data"]
    sql_data = [{"money": "2000", "time": "10"},
                {"time": "300", "money": "600"}]

    for data in sql_data:
        data_table.write(data)

    assert len(data_table.columns) == 3
    assert data_table.columns == ["id", "money", "time"]

    money_column = data_table / "money"
    print(money_column)
    assert money_column == [(2000, ), (600, )]

    all_columns = data_table / "*"
    print(all_columns)
    assert all_columns[0] == (1, 2000, 10)

    # TODO: Implement arithmeic operators for columns

def test_columns():
    tf = tempfile.mkdtemp()
    tf = Path(tf) / "test.db"
    db_tools = DataBase(db_path=tf)

    categories = {"money": "INTEGER", "time": "INTEGER"}
    db_tools.create_table(table_name="data",
                          categories=categories)

    data_table = db_tools.tables["data"]
    sql_data = [{"money": "2000", "time": "10"},
                {"time": "300", "money": "600"}]

    for data in sql_data:
        data_table.write(data)

    connection = db_tools.active_db
    with pytest.raises(RuntimeError):
        col = Column(sql_session=connection,
                    column_name="data",
                    data_type="Intege")
   
    
    col = Column(sql_session=connection,
                column_name="data",
                data_type="INTEGER")


if __name__ == "__main__":
    test_table_stuff()
