import os
import pytest
from sql_toolbox.utilities.sql_table import DataTable, DataTableTools     
from sql_toolbox.utilities.sql_refactor import SqliteSession


def test_data_table():

    table_name = "data.db"
    categories = {"money": "REAL"}
    data_table = DataTable(table_name=table_name
                           ,categories=categories)


    assert data_table.table_name == table_name
    assert data_table.categories == categories

    with pytest.raises(RuntimeError):
        data_table.validate_category_type({"money": "REA"})



def test_data_table_tools():
    
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "test.db")

    table_name = "data2"
    categories = {"money": "REAL", "time": "REAL"}
    data_table = DataTable(table_name=table_name, categories=categories)
    print(db_path)
    sql_session = SqliteSession(db_path=db_path)
    
    db_tools = DataTableTools(sql_session=sql_session)
    #db_tools.purge_table(data_table)
   
    # db_tools.create_table(data_table=data_table)

    sql_data = {"money": "2000", "time": "10"}
    db_tools.write_to_table(data_table, sql_data)
    sql_data = {"time": "2000", "money": "10"}
    db_tools.write_to_table(data_table, sql_data)
    #sql_data = {"mon": "2000"}
    #with pytest.raises(RuntimeError):
    #    db_tools.write_to_table(data_table, sql_data)

    #db_tools.purge_table(data_table)



if __name__ == "__main__":
    test_unit_tests()
    
