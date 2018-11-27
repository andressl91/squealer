import os
import tempfile
import pytest
from squealer.sql_table_tools import DataTable, DataTableTools     
from squealer.sqlite_session import SqliteSession


def test_recreate_sqlite_db():
    # TODO: FIND BUG MAKING THIS TEST FAIL EVERY SECOND RUN
    

    sql_session = SqliteSession(db_path="test.db")
    db_tools = DataTableTools(sql_session=sql_session)


    assert list(db_tools.tables.keys()) == []


    # categories = {"money": "RREAL", "time": "REAL"}
    # with pytest.raises(TypeError):
    #    db_tools.create_table(table_name="data",
      #                       categories=categories)

    categories = {"money": "REAL", "time": "REAL"}
    db_tools.create_table(table_name="data",
                          categories=categories)

    # db_tools.build_db()
    print(db_tools.tables.keys())
    
    # TODO: WHY DO I NEED THIS BUILD_DB, SHOULD BE ENOUGH IN create_table
    # method
    # db_tools.build_db()
    assert list(db_tools.tables.keys()) == ["data"]

    sql_data = {"money": "2000", "time": "10"}
    data_table = db_tools.tables["data"]
    data_table.write_to_table(sql_data)

    db_tools.delete_table("data")
    assert list(db_tools.tables.keys()) == []
    # Check that data tables are added to instance attribute tables
    # db_tools.tables = ["data"]
    # sql_session.close_db(a)
    



@pytest.mark.skip()
def test_data_table_tools():
    #TODO: REWRITE TEST TO FIT REFACTORISATION

    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "test.db")

    table_name = "data2"
    categories = {"money": "REAL", "time": "REAL"}
    data_table = DataTable(table_name=table_name, categories=categories)

    # TODO: Consider refactoring, where and SqliteSession has a .get_table
    # method also, if allready exist. Which has editing tools.
    sql_session = SqliteSession(db_path=db_path)
    
    db_tools = DataTableTools(sql_session=sql_session)
    # Clean datatable
    db_tools.clean_table(data_table)

    db_tools.create_table(data_table=data_table)

    sql_data = {"time": "2000", "money": "10"}
    db_tools.write_to_table(data_table, sql_data)
    
    # Test write of incomplete data
    sql_data = {"time": "2000", "money": "NULL"}
    db_tools.write_to_table(data_table, sql_data)

    cats = db_tools.get_categories(data_table)
    if data_table.primary_key == False:
        cats = set(cats)
        cats.remove("id")
        assert cats == set(list(categories.keys()))


    sql_data = {"mon": "2000"}
    with pytest.raises(RuntimeError):
        db_tools.write_to_table(data_table, sql_data)

    a = db_tools.select(data_table, sql="*")
    print(a)
    db_tools.clean_table(data_table)


if __name__ == "__main__":
    test_recreate_sqlite_db()

