import os
import pytest
from squealer.sql_table_tools import DataTable, DataTableTools     
from squealer.sqlite_session import SqliteSession


def test_data_table_initiation():

    table_name = "data.db"
    categories = {"money": "REAL"}
    data_table = DataTable(table_name=table_name,
                           categories=categories)

    assert data_table.table_name == table_name
    assert data_table.categories == categories

    with pytest.raises(RuntimeError):
        data_table.validate_category_type({"money": "REA"})


def test_data_table_tools():

    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "test.db")

    table_name = "data2"
    categories = {"money": "REAL", "time": "REAL"}
    data_table = DataTable(table_name=table_name, categories=categories)

    sql_session = SqliteSession(db_path=db_path)
    
    db_tools = DataTableTools(sql_session=sql_session)
    # Clean up table for test
    with sql_session as sql_s:
        if db_tools._does_table_exist(sql_s, table_name):
            db_tools.delete_table(data_table)

    db_tools.create_table(data_table=data_table)

    sql_data = {"time": "2000", "money": "10"}
    db_tools.write_to_table(data_table, sql_data)
    
    cats = db_tools.get_categories(data_table)
    if data_table.primary_key == False:
        cats = set(cats)
        cats.remove("id")
        assert cats == set(list(categories.keys()))


    sql_data = {"mon": "2000"}
    with pytest.raises(RuntimeError):
        db_tools.write_to_table(data_table, sql_data)

   #  db_tools.clean_table(data_table)


    
